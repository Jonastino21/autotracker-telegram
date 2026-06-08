#!/usr/bin/env python3
"""Supprime les pointages des GARDIENS à partir d'une date (défaut : aujourd'hui),
en CONSERVANT l'historique antérieur (paie passée intacte).

À lancer sur CHAQUE serveur (chacun a sa propre base `presences.db`). Le code
(moteur, parser, affichage) se propage par git ; seules les données se nettoient
par serveur, d'où ce script.

Usage :
  python scripts/reset_pointages_gardiens.py                  # dry-run (montre, ne supprime pas)
  python scripts/reset_pointages_gardiens.py --apply          # supprime à partir d'aujourd'hui
  python scripts/reset_pointages_gardiens.py --from 2026-06-08 --apply
  python scripts/reset_pointages_gardiens.py --sessions --apply   # + relabellise les sessions matin/apm
"""
import argparse
import os
import sys
from datetime import date, datetime

# Permet d'importer database.py / parser.py depuis la racine du projet
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import database as db  # noqa: E402


def _aujourdhui() -> str:
    """Date du jour dans le fuseau configuré (même logique que l'appli)."""
    try:
        from zoneinfo import ZoneInfo
        tz = os.getenv("TIMEZONE", "Indian/Antananarivo")
        return datetime.now(ZoneInfo(tz)).date().isoformat()
    except Exception:
        return date.today().isoformat()


def _gardiens() -> list[str]:
    return [e["prno"] for e in db.get_all_employes(actifs_only=False)
            if db._est_gardien(e)]


def supprimer(cutoff: str, apply: bool) -> None:
    gard = _gardiens()
    if not gard:
        print("Aucun gardien trouvé — rien à faire.")
        return
    ph = ",".join("?" * len(gard))
    conn = db.get_connection()
    rows = conn.execute(
        f"SELECT date_local, prno, COUNT(*) c FROM pointages "
        f"WHERE prno IN ({ph}) AND date_local >= ? "
        f"GROUP BY date_local, prno ORDER BY date_local, prno",
        (*gard, cutoff),
    ).fetchall()
    total = sum(r["c"] for r in rows)

    print(f"Gardiens          : {gard}")
    print(f"Seuil (≥, inclus) : {cutoff}  — l'historique AVANT cette date est conservé.")
    print(f"Pointages gardiens à supprimer (≥ {cutoff}) : {total}")
    for r in rows:
        print(f"  {r['date_local']}  {r['prno']:6} : {r['c']}")

    if not apply:
        print("\n(dry-run) Aucune suppression effectuée. Relance avec --apply pour exécuter.")
        conn.close()
        return

    conn.execute(
        f"DELETE FROM pointages WHERE prno IN ({ph}) AND date_local >= ?",
        (*gard, cutoff),
    )
    conn.commit()
    conn.close()
    print(f"\n✅ {total} pointages gardiens supprimés (≥ {cutoff}). Historique antérieur intact.")


def relabelliser_sessions(cutoff: str, apply: bool) -> None:
    """Recalcule la session matin/apm des pointages restants (≥ cutoff) avec la
    logique corrigée (un pointage du matin sur une nuit n'est plus « après-midi »)."""
    import sqlite3
    from parser import get_session_depuis_horaire

    conn = db.get_connection()
    rows = conn.execute(
        "SELECT id, prno, date_local, heure_locale, session FROM pointages "
        "WHERE date_local >= ?", (cutoff,),
    ).fetchall()

    a_corriger = []
    for r in rows:
        code = db.get_code_effectif(r["prno"], r["date_local"])
        if not code:
            continue
        new = get_session_depuis_horaire(code, date.fromisoformat(r["date_local"]),
                                         r["heure_locale"])
        if new != r["session"]:
            a_corriger.append((r["id"], r["prno"], r["date_local"],
                               r["heure_locale"], r["session"], new))

    print(f"\nSessions à relabelliser (≥ {cutoff}) : {len(a_corriger)}")
    for c in a_corriger:
        print(f"  id={c[0]} {c[1]:6} {c[2]} {c[3][:5]} : {c[4]} -> {c[5]}")

    if not apply:
        print("(dry-run) Aucun changement de session.")
        conn.close()
        return

    ok, skip = 0, 0
    for cid, _prno, _d, _h, _old, new in a_corriger:
        try:
            conn.execute("UPDATE pointages SET session=? WHERE id=?", (new, cid))
            ok += 1
        except sqlite3.IntegrityError:
            skip += 1
    conn.commit()
    conn.close()
    print(f"✅ Sessions corrigées : {ok} | ignorées (conflit unicité) : {skip}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--from", dest="cutoff", default=None,
                    help="date ISO de début incluse (défaut : aujourd'hui)")
    ap.add_argument("--apply", action="store_true",
                    help="exécute réellement (sinon dry-run)")
    ap.add_argument("--sessions", action="store_true",
                    help="relabellise aussi les sessions matin/apm des pointages restants")
    args = ap.parse_args()

    cutoff = args.cutoff or _aujourdhui()
    supprimer(cutoff, args.apply)
    if args.sessions:
        relabelliser_sessions(cutoff, args.apply)


if __name__ == "__main__":
    main()
