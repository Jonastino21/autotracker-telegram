#!/usr/bin/env python3
"""
maintenance_core.py — Logique de maintenance des pointages, sans I/O.

Partagé par :
  • maintenance.py        — console interactive (CLI)
  • dashboard.py          — page web /maintenance.html (API /api/maintenance/*)

Les pointages créés portent comme `source` le LIEU fourni (ex. « Siège
KAROKA »), comme les requêtes SQL manuelles. La session (matin/apm) et le code
horaire effectif (tours de garde inclus) viennent de l'application → cohérence
garantie. Ré-insérer un créneau existant (prno+date+type+session) le CORRIGE
(INSERT OR REPLACE) — pas besoin de DELETE préalable.
"""

from datetime import date as date_cls, timedelta

import database as db
from parser import get_plages_jour, _min_to_heure, get_session_depuis_horaire

LIEU_DEFAUT = "Siège KAROKA"


# ─── Normalisation ─────────────────────────────────────────────────────────────

def norm_date(s: str) -> str | None:
    """yyyy/mm/dd, yyyy-mm-dd, 'auj'/'today'/'.' → ISO yyyy-mm-dd, ou None."""
    s = (s or "").strip().lower()
    if s in ("", "q"):
        return None
    if s in ("auj", "aujourd'hui", "today", "."):
        return date_cls.today().isoformat()
    parts = s.replace("/", "-").split("-")
    if len(parts) != 3 or not all(p.isdigit() for p in parts):
        return None
    try:
        y, mo, da = (int(p) for p in parts)
        return date_cls(y, mo, da).isoformat()
    except ValueError:
        return None


def norm_heure(s: str) -> str | None:
    """HH:MM, HHhMM, HHMM, HH:MM:SS → HH:MM:SS, ou None."""
    s = (s or "").strip().lower().replace("h", ":")
    if not s:
        return None
    if ":" not in s and s.isdigit() and len(s) in (3, 4):
        s = s.zfill(4)
        s = f"{s[:2]}:{s[2:]}"
    parts = s.split(":")
    try:
        hh = int(parts[0]); mm = int(parts[1]) if len(parts) > 1 else 0
        ss = int(parts[2]) if len(parts) > 2 else 0
        if not (0 <= hh < 24 and 0 <= mm < 60 and 0 <= ss < 60):
            return None
        return f"{hh:02d}:{mm:02d}:{ss:02d}"
    except (ValueError, IndexError):
        return None


# ─── Session & écriture ────────────────────────────────────────────────────────

def session_auto(prno: str, date_iso: str, heure: str) -> str:
    """Session (matin/apm) déduite du code horaire effectif, comme l'appli."""
    date_obj = date_cls.fromisoformat(date_iso)
    code = db.get_code_effectif(prno, date_iso)
    if code:
        return get_session_depuis_horaire(code, date_obj, heure)
    return "matin" if int(heure[:2]) < 13 else "apm"


def inserer_pointage(prno, date_iso, heure, type_pointage, session,
                     source=None, remplacer=True) -> str:
    """INSERT (OR REPLACE) d'un pointage. message_id déterministe par créneau
    (prno+date+type+session). Renvoie 'inséré', 'corrigé' ou 'ignoré'."""
    source = (source or LIEU_DEFAUT).strip() or LIEU_DEFAUT
    msg_id = f"manuel:{prno}:{date_iso}:{type_pointage}:{session}"
    conn = db.get_connection()
    try:
        existant = conn.execute(
            """SELECT id FROM pointages
               WHERE prno=? AND date_local=? AND type_pointage=? AND session=?""",
            (prno, date_iso, type_pointage, session)).fetchone()
        if existant and not remplacer:
            return "ignoré"
        verbe = "corrigé" if existant else "inséré"
        op = "INSERT OR REPLACE" if remplacer else "INSERT OR IGNORE"
        conn.execute(f"""
            {op} INTO pointages
                (message_id, telegram_id, prno, date_local, heure_locale,
                 type_pointage, session, raw_text, source)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (msg_id, 0, prno, date_iso, heure, type_pointage, session,
              "saisie maintenance", source))
        conn.commit()
        return verbe
    finally:
        conn.close()


def pointages_jour_employe(prno: str, date_iso: str) -> list:
    conn = db.get_connection()
    try:
        rows = conn.execute("""
            SELECT id, heure_locale, type_pointage, session,
                   COALESCE(source,'telegram') AS source
            FROM pointages WHERE prno=? AND date_local=?
            ORDER BY heure_locale
        """, (prno, date_iso)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def supprimer_ids(ids: list) -> int:
    ids = [int(i) for i in (ids or [])]
    if not ids:
        return 0
    conn = db.get_connection()
    try:
        marqueurs = ",".join("?" * len(ids))
        cur = conn.execute(f"DELETE FROM pointages WHERE id IN ({marqueurs})", ids)
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


# ─── Remplissage d'après le planning ───────────────────────────────────────────

def evenements_planning(prno: str, date_iso: str):
    """Pointages théoriques (date, heure, type) déduits du planning du jour.
    Gère les plages de nuit (départ le lendemain). Renvoie (events, code)."""
    code = db.get_code_effectif(prno, date_iso)
    if not code:
        return [], None
    date_obj = date_cls.fromisoformat(date_iso)
    events = []
    for p in get_plages_jour(code, date_obj):
        events.append({"date": date_iso, "heure": _min_to_heure(p["debut"]) + ":00",
                       "type": "arrivee"})
        off = p.get("fin_offset", 0)
        overnight = off >= 1 or p["fin"] < p["debut"]
        fin_date = (date_obj + timedelta(days=max(off, 1))).isoformat() if overnight else date_iso
        events.append({"date": fin_date, "heure": _min_to_heure(p["fin"]) + ":00",
                       "type": "depart"})
    return events, code


def remplir_jour(prno: str, date_iso: str, lieu: str, remplacer=True) -> dict:
    """Insère les pointages du planning pour un jour.
    Renvoie {ok, code, details:[{date,heure,type,session,res}], touche:[dates]}."""
    events, code = evenements_planning(prno, date_iso)
    if not events:
        return {"ok": False, "raison": "aucun_service", "code": code,
                "details": [], "touche": []}
    details, touche = [], set()
    for ev in events:
        session = session_auto(prno, ev["date"], ev["heure"])
        res = inserer_pointage(prno, ev["date"], ev["heure"], ev["type"],
                               session, source=lieu, remplacer=remplacer)
        touche.add(ev["date"])
        details.append({**ev, "session": session, "res": res})
    return {"ok": True, "code": code, "details": details, "touche": sorted(touche)}


def remplir_plage(prno: str, date_debut: str, date_fin: str, lieu: str,
                  remplacer=True) -> dict:
    """Remplit chaque jour avec service prévu entre deux dates incluses."""
    cur, fin = date_cls.fromisoformat(date_debut), date_cls.fromisoformat(date_fin)
    jours, details, touche = [], [], set()
    while cur <= fin:
        di = cur.isoformat()
        r = remplir_jour(prno, di, lieu, remplacer=remplacer)
        if r["ok"]:
            jours.append(di)
            details += r["details"]
            touche.update(r["touche"])
        cur += timedelta(days=1)
    return {"ok": bool(jours), "jours": jours, "details": details,
            "touche": sorted(touche)}
