"""
detector.py — Détection des mots-clés de présence avec tolérance aux fautes.
Utilise rapidfuzz pour une correspondance floue à 85%.
"""

from rapidfuzz import fuzz

# Mots-clés de référence
KEYWORDS_ARRIVEE = [
    "bonjour", "bonjours", "bonsoir", "bjr", "bon jour",
    "hello", "salut", "présent", "present", "arrivée", "arrivee",
]

KEYWORDS_DEPART = [
    "au revoir", "aurevoir", "au rev", "au-revoir", "bonne journée",
    "bonne soirée", "bonsoir", "bye", "à bientôt", "a bientot",
    "départ", "depart", "je pars", "je m'en vais", "bonne soiree",
    "bonne journee",
]

THRESHOLD = 85  # Seuil de similarité en %


def _score(text: str, keyword: str) -> float:
    """Calcule le meilleur score de similarité entre le texte et un mot-clé."""
    text = text.lower().strip()
    keyword = keyword.lower().strip()

    scores = [
        fuzz.ratio(text, keyword),
        fuzz.partial_ratio(text, keyword),
        fuzz.token_sort_ratio(text, keyword),
        fuzz.token_set_ratio(text, keyword),
    ]
    return max(scores)


def is_arrivee(text: str) -> bool:
    """Retourne True si le texte correspond à une arrivée."""
    text = text.lower().strip()
    if not text:
        return False
    for kw in KEYWORDS_ARRIVEE:
        if _score(text, kw) >= THRESHOLD:
            return True
    return False


def is_depart(text: str) -> bool:
    """Retourne True si le texte correspond à un départ."""
    text = text.lower().strip()
    if not text:
        return False
    for kw in KEYWORDS_DEPART:
        if _score(text, kw) >= THRESHOLD:
            return True
    return False


def detect_type(text: str):
    """
    Détecte le type de pointage.
    Retourne : 'arrivee', 'depart', ou None
    Priorité : départ > arrivée (évite confusion "bonsoir")
    """
    if not text or not text.strip():
        return None

    # Tester départ en premier (priorité)
    if is_depart(text):
        return "depart"
    if is_arrivee(text):
        return "arrivee"
    return None


def get_match_score(text: str, keyword_type: str) -> float:
    """Retourne le meilleur score pour un type donné (debug)."""
    keywords = KEYWORDS_ARRIVEE if keyword_type == "arrivee" else KEYWORDS_DEPART
    return max((_score(text, kw) for kw in keywords), default=0)


# Tests rapides en mode standalone
if __name__ == "__main__":
    tests = [
        ("Bonjour tout le monde", "arrivee"),
        ("bjr", "arrivee"),
        ("bonjours", "arrivee"),
        ("Au revoir", "depart"),
        ("aurevoir", "depart"),
        ("au rev", "depart"),
        ("bonne journée", "depart"),
        ("je mange", None),
        ("présent", "arrivee"),
        ("Bonsoir", "depart"),  # ambigu → départ prioritaire
    ]

    print(f"{'Texte':<25} {'Attendu':<10} {'Détecté':<10} {'OK'}")
    print("-" * 55)
    for text, expected in tests:
        detected = detect_type(text)
        ok = "✓" if detected == expected else "✗"
        print(f"{text:<25} {str(expected):<10} {str(detected):<10} {ok}")
