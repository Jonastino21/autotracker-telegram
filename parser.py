"""
parser.py — Parser du code horaire KAROKA + calcul temps réel plafonné.
"""

import re
import logging
from datetime import date, timedelta

logger = logging.getLogger(__name__)

JOURS = {
    "lu":[0],"ma":[1],"me":[2],"je":[3],
    "ve":[4],"sa":[5],"di":[6],
    "tj":[0,1,2,3,4,5,6],
}
JOURS_NOMS = ["Lundi","Mardi","Mercredi","Jeudi","Vendredi","Samedi","Dimanche"]


def _parse_heure(h: str) -> int:
    h = h.zfill(4)
    return int(h[:2])*60 + int(h[2:])


def _duree_plage(debut: int, fin: int) -> int:
    """Durée en minutes, gère chevauchement minuit."""
    if fin > debut:
        return fin - debut
    return (24*60 - debut) + fin


def _parse_segment(seg: str):
    seg = seg.strip().lower()
    m = re.match(r'^(\d{4})(lu|ma|me|je|ve|sa|di|tj)(\d{4})$', seg)
    if not m:
        return None
    return {
        "jours": JOURS.get(m.group(2), []),
        "debut": _parse_heure(m.group(1)),
        "fin":   _parse_heure(m.group(3)),
    }


def parse_code_horaire(code: str) -> dict:
    """
    Parse le code horaire complet.
    Retourne pour chaque jour (0-6) :
      - plages_travail : [(debut, fin), ...]  — après exclusions
      - minutes_theoriques : int
      - travaille : bool
    """
    if not code or not code.strip():
        return _empty_result()

    jours_raw = {i: {"travail": [], "pauses": []} for i in range(7)}

    for seg in [s.strip() for s in code.split(";") if s.strip()]:
        is_pause = seg.startswith("(") and seg.endswith(")")
        raw    = seg[1:-1] if is_pause else seg
        parsed = _parse_segment(raw)
        if not parsed:
            continue
        for j in parsed["jours"]:
            plage = (parsed["debut"], parsed["fin"])
            if is_pause:
                jours_raw[j]["pauses"].append(plage)
            else:
                jours_raw[j]["travail"].append(plage)

    result = {}
    for jour_idx in range(7):
        travail = jours_raw[jour_idx]["travail"]
        pauses  = jours_raw[jour_idx]["pauses"]

        # Appliquer les exclusions/pauses
        plages_nettes = []
        for td, tf in travail:
            brut = _duree_plage(td, tf)
            pause_total = 0
            exclure = False
            for pd, pf in pauses:
                dur_pause = _duree_plage(pd, pf)
                # Exclusion totale de la plage
                if pd <= td and pf >= tf:
                    exclure = True
                    break
                # Pause dans la plage
                if pd >= td and pf <= tf:
                    pause_total += dur_pause
                elif pd >= td and pd < tf:
                    pause_total += min(pf, tf) - pd
            if not exclure:
                net = max(0, brut - pause_total)
                if net > 0:
                    plages_nettes.append({"debut": td, "fin": tf, "minutes_net": net})

        total_net = sum(p["minutes_net"] for p in plages_nettes)

        result[jour_idx] = {
            "nom":                JOURS_NOMS[jour_idx],
            "plages_travail":     plages_nettes,
            "minutes_theoriques": total_net,
            "travaille":          total_net > 0,
        }

    return result


def _empty_result():
    return {i: {
        "nom": JOURS_NOMS[i],
        "plages_travail": [],
        "minutes_theoriques": 0,
        "travaille": False,
    } for i in range(7)}


def get_minutes_theoriques_jour(code: str, date_obj: date) -> int:
    if not code:
        return 0
    return parse_code_horaire(code)[date_obj.weekday()]["minutes_theoriques"]


def get_plages_jour(code: str, date_obj: date) -> list:
    """Retourne les plages de travail nettes pour un jour donné."""
    if not code:
        return []
    return parse_code_horaire(code)[date_obj.weekday()]["plages_travail"]


def est_dans_plage_horaire(code: str, date_obj: date, heure_str: str,
                            tolerance: int = 30) -> bool:
    """
    Vérifie si une heure de pointage appartient à une plage de travail
    de l'employé pour ce jour, avec une tolérance en minutes.

    Retourne False si hors de toute plage → pointage ignoré.
    Retourne True si dans une fenêtre [debut - tolerance, fin + tolerance].
    """
    plages = get_plages_jour(code, date_obj)
    if not plages:
        return False
    h_min = _heure_to_min(heure_str)
    for plage in plages:
        if (plage["debut"] - tolerance) <= h_min <= (plage["fin"] + tolerance):
            return True
    return False


def get_session_depuis_horaire(code: str, date_obj: date, heure_str: str) -> str:
    """
    Détermine la session (matin/apm) d'un pointage selon les plages horaires
    réelles de l'employé, sans seuil fixe à 13h.

    Logique :
    - Si une seule plage dans la journée → toujours 'matin' (session unique)
    - Si deux plages → la plage qui se termine la plus tôt = matin,
      l'autre = apm
    - On affecte le pointage à la plage dont le milieu est le plus proche
    """
    plages = get_plages_jour(code, date_obj)
    if not plages:
        # Fallback seuil fixe si pas d'horaire défini
        h = _heure_to_min(heure_str)
        return "matin" if h < 780 else "apm"

    if len(plages) == 1:
        return "matin"

    # Plusieurs plages : trier par heure de début
    plages_triees = sorted(plages, key=lambda p: p["debut"])
    h_min = _heure_to_min(heure_str)

    # Calculer le milieu de chaque plage et trouver la plus proche
    distances = []
    for i, plage in enumerate(plages_triees):
        milieu = (plage["debut"] + plage["fin"]) // 2
        distances.append((abs(h_min - milieu), i))

    distances.sort()
    idx_plus_proche = distances[0][1]

    # La première plage (index 0 après tri) = matin, les suivantes = apm
    return "matin" if idx_plus_proche == 0 else "apm"


def get_minutes_theoriques_semaine(code: str) -> int:
    if not code:
        return 0
    return sum(j["minutes_theoriques"] for j in parse_code_horaire(code).values())


def get_minutes_theoriques_periode(code: str, date_debut: date, date_fin: date,
                                    jours_feries: set = None) -> int:
    """Minutes théoriques sur une période, jours fériés comptés comme payés."""
    if not code:
        return 0
    parsed = parse_code_horaire(code)
    total  = 0
    d      = date_debut
    while d <= date_fin:
        # Jour férié = heures théoriques quand même (payé)
        total += parsed[d.weekday()]["minutes_theoriques"]
        d += timedelta(days=1)
    return total


def calculer_temps_reel_plafonne(code: str, date_obj: date,
                                  pointages: list) -> dict:
    """
    Calcule le temps réel plafonné par plage horaire.

    Règle anti-doublons :
    Pour chaque plage théorique, on prend :
    - La PREMIÈRE arrivée dans la fenêtre de la plage (ou la plus proche avant)
    - Le DERNIER départ dans la fenêtre de la plage (ou le plus proche après)
    Peu importe combien de fois l'employé a écrit Bonjour/Au revoir.

    Plafonnement :
    - Arrivée en avance → on compte depuis le début de la plage
    - Départ en retard  → on compte jusqu'à la fin de la plage
    - Arrivée en retard → pénalité (on compte depuis l'arrivée réelle)
    - Départ anticipé   → pénalité (on compte jusqu'au départ réel)
    """
    plages = get_plages_jour(code, date_obj)
    if not plages:
        return {
            "minutes_theoriques": 0, "minutes_reels": 0,
            "ecart": 0, "plages_detail": [], "complet": False,
        }

    # Convertir tous les pointages en minutes, triés
    arrivees = sorted([_heure_to_min(p["heure"]) for p in pointages if p["type"] == "arrivee"])
    departs  = sorted([_heure_to_min(p["heure"]) for p in pointages if p["type"] == "depart"])

    total_theorique = sum(p["minutes_net"] for p in plages)
    total_reel      = 0
    plages_detail   = []
    plages_couverts = 0

    for plage in plages:
        pd, pf          = plage["debut"], plage["fin"]
        theorique_plage = plage["minutes_net"]

        # Fenêtre élargie : 2h avant le début et 2h après la fin
        # pour attraper les pointages proches de la plage
        fenetre_debut = pd - 120
        fenetre_fin   = pf + 120

        # Arrivées candidates : toutes dans la fenêtre
        arrivees_candidates = [a for a in arrivees if fenetre_debut <= a <= fenetre_fin]
        departs_candidates  = [d for d in departs  if fenetre_debut <= d <= fenetre_fin]

        if not arrivees_candidates and not departs_candidates:
            plages_detail.append({
                "plage_debut": _min_to_heure(pd),
                "plage_fin":   _min_to_heure(pf),
                "arrivee": None, "depart": None,
                "minutes": 0, "theorique": theorique_plage,
            })
            continue

        # Règle anti-doublons :
        # 1ère arrivée candidate = heure d'arrivée retenue
        # Dernier départ candidate = heure de départ retenue
        premiere_arrivee = arrivees_candidates[0]  if arrivees_candidates else None
        dernier_depart   = departs_candidates[-1]  if departs_candidates  else None

        if premiere_arrivee is None or dernier_depart is None:
            # Seulement arrivée ou seulement départ → incomplet
            plages_detail.append({
                "plage_debut": _min_to_heure(pd),
                "plage_fin":   _min_to_heure(pf),
                "arrivee": _min_to_heure(premiere_arrivee) if premiere_arrivee else None,
                "depart":  _min_to_heure(dernier_depart)   if dernier_depart   else None,
                "minutes": 0, "theorique": theorique_plage,
            })
            continue

        # Plafonnement
        debut_compte = max(premiere_arrivee, pd)  # pas de bonus si arrivée en avance
        fin_compte   = min(dernier_depart,   pf)  # pas de bonus si départ en retard

        if fin_compte <= debut_compte:
            minutes_brut = 0
        else:
            minutes_brut = fin_compte - debut_compte

        # Ratio pauses
        ratio = theorique_plage / _duree_plage(pd, pf) if _duree_plage(pd, pf) > 0 else 1
        minutes_net = int(minutes_brut * ratio)

        total_reel      += minutes_net
        plages_couverts += 1

        plages_detail.append({
            "plage_debut": _min_to_heure(pd),
            "plage_fin":   _min_to_heure(pf),
            "arrivee":     _min_to_heure(premiere_arrivee),
            "depart":      _min_to_heure(dernier_depart),
            "minutes":     minutes_net,
            "theorique":   theorique_plage,
        })

    complet = (plages_couverts == len(plages) and
               total_reel >= total_theorique * 0.95)

    return {
        "minutes_theoriques": total_theorique,
        "minutes_reels":      total_reel,
        "ecart":              total_reel - total_theorique,
        "plages_detail":      plages_detail,
        "complet":            complet,
    }


def _trouver_plus_proche(heures: list, reference: int):
    """Trouve l'heure la plus proche d'une référence dans une liste."""
    if not heures:
        return None
    return min(heures, key=lambda h: abs(h - reference))


def _heure_to_min(heure_str: str) -> int:
    """Convertit 'HH:MM:SS' ou 'HH:MM' en minutes."""
    parts = heure_str.split(":")
    return int(parts[0])*60 + int(parts[1])


def _min_to_heure(minutes: int) -> str:
    """Convertit des minutes en 'HH:MM'."""
    return f"{minutes//60:02d}:{minutes%60:02d}"


def format_duree(minutes) -> str:
    if minutes is None:
        return "—"
    h = abs(minutes) // 60
    m = abs(minutes) % 60
    return f"{h}h{m:02d}"


def format_ecart(minutes_theoriques: int, minutes_reelles: int) -> dict:
    if minutes_theoriques == 0:
        return {"ecart_min": 0, "label": "—", "type": "non_travaille"}
    ecart = minutes_reelles - minutes_theoriques
    if ecart >= 0:
        return {"ecart_min": ecart, "label": "✓", "type": "ok"}
    return {"ecart_min": ecart, "label": f"-{format_duree(abs(ecart))}", "type": "manquant"}


def valider_code_horaire(code: str) -> dict:
    if not code or not code.strip():
        return {"ok": False, "erreurs": ["Code vide"], "heures_semaine": 0}
    erreurs = []
    for seg in [s.strip() for s in code.split(";") if s.strip()]:
        is_pause = seg.startswith("(") and seg.endswith(")")
        raw = seg[1:-1] if is_pause else seg
        if not _parse_segment(raw):
            erreurs.append(f"Segment invalide : '{seg}'")
    if erreurs:
        return {"ok": False, "erreurs": erreurs, "heures_semaine": 0}
    try:
        total = get_minutes_theoriques_semaine(code)
        parsed = parse_code_horaire(code)
        detail = [{"jour": j["nom"], "theorique": format_duree(j["minutes_theoriques"]),
                   "travaille": j["travaille"]} for j in parsed.values()]
        return {"ok": True, "erreurs": [], "heures_semaine": total,
                "label_semaine": format_duree(total), "detail": detail}
    except Exception as e:
        return {"ok": False, "erreurs": [str(e)], "heures_semaine": 0}


# ─── Test ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    from datetime import date as date_cls

    print("=== Test standard KAROKA ===")
    code = "0800tj1200;1400tj1700;(1000tj1010);(1600tj1610);(0800di1200);(1400di1700)"
    val = valider_code_horaire(code)
    print(f"Semaine : {val['label_semaine']}")

    print("\n=== Test employée matin uniquement ===")
    code2 = "0600tj1300;(1200sa1300);(1000tj1010);(0600di1300)"
    val2  = valider_code_horaire(code2)
    for d in val2['detail']:
        print(f"  {d['jour']:<10} → {d['theorique'] if d['travaille'] else 'Repos'}")
    print(f"  Total : {val2['label_semaine']}")

    print("\n=== Test calcul plafonné ===")
    # Employée pointe en retard arrivée, départ anticipé
    code3   = "0600tj1300;(1000tj1010);(0600di1300)"
    today   = date_cls(2026, 4, 7)  # Lundi
    pointages = [
        {"type": "arrivee", "heure": "06:15:00"},  # 15 min retard
        {"type": "depart",  "heure": "12:45:00"},  # 15 min anticipé
    ]
    result = calculer_temps_reel_plafonne(code3, today, pointages)
    print(f"  Théorique : {format_duree(result['minutes_theoriques'])}")
    print(f"  Réel      : {format_duree(result['minutes_reels'])}")
    print(f"  Écart     : {format_duree(result['ecart'])}")
    print(f"  Complet   : {result['complet']}")

    print("\n=== Test arrivée en avance ===")
    pointages2 = [
        {"type": "arrivee", "heure": "05:45:00"},  # en avance → compte depuis 06h00
        {"type": "depart",  "heure": "13:15:00"},  # en retard  → compte jusqu'à 13h00
    ]
    result2 = calculer_temps_reel_plafonne(code3, today, pointages2)
    print(f"  Théorique : {format_duree(result2['minutes_theoriques'])}")
    print(f"  Réel      : {format_duree(result2['minutes_reels'])}")
    print(f"  Complet   : {result2['complet']}")