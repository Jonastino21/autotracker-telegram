#!/usr/bin/env python3
"""
maintenance.py — Console de maintenance des pointages (CLI, usage admin/RH).

Variante ligne de commande de la page web /maintenance.html. Toute la logique
vit dans maintenance_core.py (partagée avec le dashboard) → comportement
identique des deux côtés.

  • Inspecter une journée (tous les employés) ou un employé sur une période.
  • Remplir une journée (ou une plage de jours) d'après le PLANNING — pour ceux
    qui travaillent ailleurs sans badge/téléphone, ou pour réparer une absence
    due à un raté de l'appli.
  • Ajouter / corriger un pointage à la main (arrivée/départ, matin/apm).
  • Supprimer un pointage incohérent, ou vider la journée d'un employé.

Le `source` enregistré est le LIEU saisi (défaut « Siège KAROKA »).

Usage :
    python maintenance.py
"""

import sys
from datetime import date as date_cls, timedelta

import database as db
import maintenance_core as mc


# ─── Saisie ────────────────────────────────────────────────────────────────────

def _ask(prompt: str, default: str = None) -> str:
    suff = f" [{default}]" if default else ""
    val = input(f"{prompt}{suff} : ").strip()
    return val or (default or "")


def _confirmer(prompt: str) -> bool:
    return input(f"{prompt} (o/N) : ").strip().lower() in ("o", "oui", "y", "yes")


def _ask_lieu() -> str:
    """Demande le lieu (colonne `source`) et mémorise le choix pour la suite."""
    lieu = _ask("Lieu / source", mc.LIEU_DEFAUT).strip()
    if lieu:
        mc.LIEU_DEFAUT = lieu
    return lieu or mc.LIEU_DEFAUT


def _ask_date(prompt="Date") -> str | None:
    d = mc.norm_date(_ask(f"{prompt} (yyyy/mm/dd, 'auj', q=annuler)"))
    if d is None:
        print("  ✗ date invalide ou annulée.")
    return d


def _choisir_employe() -> dict | None:
    terme = _ask("Employé (PRNO ou nom, q=annuler)").strip().lower()
    if not terme or terme == "q":
        return None
    employes = db.get_all_employes(actifs_only=False)
    exact = [e for e in employes if e["prno"] == terme]
    if exact:
        return exact[0]
    matches = [e for e in employes
               if terme in e["prno"] or terme in (e["nom_complet"] or "").lower()]
    if not matches:
        print("  ✗ aucun employé trouvé.")
        return None
    if len(matches) == 1:
        return matches[0]
    print("  Plusieurs correspondances :")
    for i, e in enumerate(matches, 1):
        print(f"   {i:>2}. {e['prno']:<14} {e['nom_complet']}  ({e.get('categorie') or 'standard'})")
    sel = _ask("Numéro").strip()
    if sel.isdigit() and 1 <= int(sel) <= len(matches):
        return matches[int(sel) - 1]
    print("  ✗ choix invalide.")
    return None


def _regen_excel(dates):
    try:
        import exporter
        for d in sorted(set(dates)):
            exporter.generate_excel(d)
        print(f"  ✓ Excel régénéré ({len(set(dates))} date(s)).")
    except Exception as e:
        print(f"  ⚠ Excel non régénéré : {e}")


# ─── Affichage ─────────────────────────────────────────────────────────────────

_HEURE = lambda h: (h or "")[:5] or "—"


def _ligne_resume(r: dict) -> str:
    return (f"  {r['statut']:<10} {r['prno']:<14} {r['nom_prenom'][:24]:<24} "
            f"M {_HEURE(r['arr_mat'])}-{_HEURE(r['dep_mat'])}  "
            f"A {_HEURE(r['arr_apm'])}-{_HEURE(r['dep_apm'])}  "
            f"théo {r.get('theorique_label','—'):<7} réel {r.get('reel_label','—')}")


def inspecter_jour():
    d = _ask_date("Jour à inspecter")
    if not d:
        return
    rows = db.get_resume_jour_avec_horaires(d)
    ferie = db.is_jour_ferie(d)
    print(f"\n── {d}{'  [FÉRIÉ]' if ferie else ''} ──  ({len(rows)} employés)")
    filtre = _ask("Filtrer statut (Absent/Incomplet/…, vide=tous)").strip().lower()
    for r in sorted(rows, key=lambda x: (x["statut"], x["nom_prenom"])):
        if filtre and filtre not in r["statut"].lower():
            continue
        print(_ligne_resume(r))
    print()


def inspecter_employe():
    emp = _choisir_employe()
    if not emp:
        return
    d1 = _ask_date("Du")
    if not d1:
        return
    d2 = _ask_date("Au") or d1
    cur = date_cls.fromisoformat(d1)
    fin = date_cls.fromisoformat(d2)
    print(f"\n── {emp['prno']} — {emp['nom_complet']}  ({d1} → {d2}) ──")
    while cur <= fin:
        di = cur.isoformat()
        rows = db.get_resume_jour_avec_horaires(di)
        r = next((x for x in rows if x["prno"] == emp["prno"]), None)
        if r:
            j = ["lu", "ma", "me", "je", "ve", "sa", "di"][cur.weekday()]
            print(f"  {di} {j}  {_ligne_resume(r).strip()}")
        cur += timedelta(days=1)
    print()


def remplir_planning_jour():
    print("\n[Remplir une journée d'après le planning de l'employé]")
    emp = _choisir_employe()
    if not emp:
        return
    d = _ask_date("Jour")
    if not d:
        return
    events, code = mc.evenements_planning(emp["prno"], d)
    if not events:
        print("  ✗ aucun service prévu ce jour (repos/exclu/hors planning).")
        return
    print(f"  Code effectif du jour : {code}")
    print("  Pointages qui seront créés :")
    for ev in events:
        print(f"    {ev['date']} {ev['heure'][:5]}  {ev['type']}")
    lieu = _ask_lieu()
    if not _confirmer(f"Confirmer la création (lieu='{lieu}', corrige l'existant)"):
        print("  — annulé.")
        return
    r = mc.remplir_jour(emp["prno"], d, lieu, remplacer=True)
    for det in r["details"]:
        print(f"    {det['date']} {det['heure'][:5]}  {det['type']:<8} "
              f"{det['session']:<5} → {det['res']}")
    if r["touche"] and _confirmer("Régénérer l'Excel"):
        _regen_excel(r["touche"])


def remplir_planning_plage():
    print("\n[Remplir une plage de jours d'après le planning]")
    emp = _choisir_employe()
    if not emp:
        return
    d1 = _ask_date("Du")
    if not d1:
        return
    d2 = _ask_date("Au")
    if not d2:
        return
    if d2 < d1:
        print("  ✗ 'Au' antérieur à 'Du'.")
        return
    lieu = _ask_lieu()
    if not _confirmer(f"Remplir tous les jours avec service entre {d1} et {d2} (lieu='{lieu}')"):
        print("  — annulé.")
        return
    r = mc.remplir_plage(emp["prno"], d1, d2, lieu, remplacer=True)
    if not r["ok"]:
        print("  ✗ aucun jour avec service prévu sur la plage.")
        return
    print(f"  Jours remplis : {', '.join(r['jours'])}")
    for det in r["details"]:
        print(f"    {det['date']} {det['heure'][:5]}  {det['type']:<8} "
              f"{det['session']:<5} → {det['res']}")
    if r["touche"] and _confirmer("Régénérer l'Excel"):
        _regen_excel(r["touche"])


def ajouter_pointage():
    print("\n[Ajouter / corriger un pointage]")
    emp = _choisir_employe()
    if not emp:
        return
    d = _ask_date("Date")
    if not d:
        return
    h = mc.norm_heure(_ask("Heure (HH:MM)"))
    if not h:
        print("  ✗ heure invalide.")
        return
    t = _ask("Type (a=arrivée, d=départ)", "a").lower()
    type_pointage = "depart" if t.startswith("d") else "arrivee"
    sess_in = _ask("Session (m=matin, a=apm, auto)", "auto").lower()
    if sess_in.startswith("m"):
        session = "matin"
    elif sess_in.startswith("a") and sess_in != "auto":
        session = "apm"
    else:
        session = mc.session_auto(emp["prno"], d, h)
    lieu = _ask_lieu()
    print(f"  → {emp['prno']} {d} {h[:5]} {type_pointage} [{session}] lieu='{lieu}'")
    if not _confirmer("Confirmer"):
        print("  — annulé.")
        return
    res = mc.inserer_pointage(emp["prno"], d, h, type_pointage, session,
                              source=lieu, remplacer=True)
    print(f"  ✓ {res}.")
    if _confirmer("Régénérer l'Excel"):
        _regen_excel([d])


def supprimer_pointage():
    print("\n[Supprimer un / des pointage(s)]")
    emp = _choisir_employe()
    if not emp:
        return
    d = _ask_date("Date")
    if not d:
        return
    pts = mc.pointages_jour_employe(emp["prno"], d)
    if not pts:
        print("  (aucun pointage ce jour)")
        return
    print(f"  Pointages de {emp['prno']} le {d} :")
    for p in pts:
        print(f"   id {p['id']:<6} {p['heure_locale'][:5]}  {p['type_pointage']:<8} "
              f"{p['session']:<5} [{p['source']}]")
    sel = _ask("ID(s) à supprimer (séparés par espace, 'tout', q)").strip().lower()
    if sel in ("", "q"):
        return
    if sel == "tout":
        ids = [p["id"] for p in pts]
    else:
        ids = [int(x) for x in sel.split() if x.isdigit()]
    if not ids or not _confirmer(f"Supprimer {len(ids)} pointage(s)"):
        print("  — annulé.")
        return
    n = mc.supprimer_ids(ids)
    print(f"  ✓ {n} supprimé(s).")
    if _confirmer("Régénérer l'Excel"):
        _regen_excel([d])


# ─── Menu ──────────────────────────────────────────────────────────────────────

MENU = """
╔════════════════════════════════════════════════════╗
║   KAROKA Présences — Console de maintenance          ║
╠════════════════════════════════════════════════════╣
║  1. Inspecter une journée (tous les employés)        ║
║  2. Inspecter un employé sur une période             ║
║  3. Remplir une JOURNÉE d'après le planning          ║
║  4. Remplir une PLAGE de jours d'après le planning   ║
║  5. Ajouter / corriger un pointage à la main         ║
║  6. Supprimer un / des pointage(s)                   ║
║  0. Quitter                                          ║
╚════════════════════════════════════════════════════╝"""

ACTIONS = {
    "1": inspecter_jour,
    "2": inspecter_employe,
    "3": remplir_planning_jour,
    "4": remplir_planning_plage,
    "5": ajouter_pointage,
    "6": supprimer_pointage,
}


def main():
    print(f"Base : {db.DB_PATH}")
    while True:
        print(MENU)
        choix = input("Choix : ").strip()
        if choix in ("0", "q", "quit", "exit"):
            print("Au revoir.")
            return
        action = ACTIONS.get(choix)
        if not action:
            print("  ✗ choix invalide.")
            continue
        try:
            action()
        except (KeyboardInterrupt, EOFError):
            print("\n  — interrompu.")
        except Exception as e:
            print(f"  ⚠ erreur : {e}")


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, EOFError):
        print("\nAu revoir.")
        sys.exit(0)
