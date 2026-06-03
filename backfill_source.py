#!/usr/bin/env python3
"""
backfill_source.py — Récupère la provenance (source) des pointages enregistrés
AVANT l'ajout de la colonne `source`.

Les pointages ESP32 (empreinte / badge / pin) stockaient déjà leur payload JSON
dans `raw_text` (ex: {"id":"6","type":"fingerprint","timestamp":...}). La migration
les a remplis par défaut avec source='telegram'. Ce script relit `raw_text` :
si c'est un JSON ESP32 avec un champ "type", il corrige `source` en conséquence.

Les vrais pointages Telegram (raw_text = "Bonjour", "Au revoir"…) ne sont pas
du JSON valide → ils restent en 'telegram'. Sans risque.

Usage :
    python backfill_source.py            # aperçu (dry-run, ne modifie rien)
    python backfill_source.py --apply    # applique les corrections
"""

import os
import sys
import json
import sqlite3

DB_PATH = os.getenv("DB_PATH", "presences.db")
TYPES_VALIDES = {"fingerprint", "empreinte", "badge", "pin"}


def main():
    apply = "--apply" in sys.argv
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    rows = conn.execute("""
        SELECT id, raw_text, source
        FROM pointages
        WHERE COALESCE(source, 'telegram') = 'telegram'
    """).fetchall()

    corrections = []
    for r in rows:
        raw = (r["raw_text"] or "").strip()
        if not raw.startswith("{"):
            continue  # pas du JSON → vrai pointage Telegram
        try:
            data = json.loads(raw)
        except Exception:
            continue
        t = str(data.get("type", "")).lower().strip()
        if t in TYPES_VALIDES:
            corrections.append((r["id"], t))

    print(f"Base            : {DB_PATH}")
    print(f"Lignes 'telegram' examinées : {len(rows)}")
    print(f"Corrections détectées       : {len(corrections)}")
    from collections import Counter
    for src, n in Counter(t for _, t in corrections).items():
        print(f"   {src:<12} → {n}")

    if not corrections:
        print("Rien à corriger.")
        conn.close()
        return

    if not apply:
        print("\n(dry-run) Relancez avec --apply pour écrire les corrections.")
        conn.close()
        return

    conn.executemany("UPDATE pointages SET source=? WHERE id=?",
                     [(t, pid) for pid, t in corrections])
    conn.commit()
    conn.close()
    print(f"\n✓ {len(corrections)} pointages corrigés.")


if __name__ == "__main__":
    main()
