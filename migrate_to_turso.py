"""
migrate_to_turso.py — Migre presences.db (SQLite local) vers Turso (libsql cloud).
Usage :
    python3 migrate_to_turso.py
"""

import sqlite3
import libsql_experimental as libsql
import os
import sys

# ─── CONFIG ───────────────────────────────────────────────────────────────────
SQLITE_PATH    = "presences.db"   # chemin vers ta base locale
TURSO_URL      = "libsql://karoka-presences-jonastino.aws-eu-west-1.turso.io"
TURSO_TOKEN    = "eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3Nzk5NDk4NTIsImlkIjoiMDE5ZTZkNDYtOTgwMS03NjAwLWEzN2MtMzdmYTZlMjVmZDBlIiwicmlkIjoiOWZjNDg1ZDAtNDFmNi00OGNjLTg5ZTctMWJlOGVkZTkwN2EyIn0.stOwBNBY4xwoK0jNI6If7WPL1ZPEwIjDgSQvNH2lWg6jfICvrnGDyZQBMLwrwo0uQoX4Odn0uod9TsRwnhjnBQ"
# ──────────────────────────────────────────────────────────────────────────────

TABLES = [
    "employes",
    "liaisons",
    "pointages",
    "jours_feries",
    "meta",
    "horaires",
    "historique_horaires",
    "releves",
    "exceptions_horaires",
    "liaisons_empreintes",
]

def get_local():
    if not os.path.exists(SQLITE_PATH):
        print(f"[ERREUR] Fichier introuvable : {SQLITE_PATH}")
        sys.exit(1)
    conn = sqlite3.connect(SQLITE_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def get_turso():
    conn = libsql.connect(TURSO_URL, auth_token=TURSO_TOKEN)
    return conn

def get_schema(local, table):
    row = local.execute(
        f"SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    return row[0] if row else None

def get_indexes(local, table):
    rows = local.execute(
        f"SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name=? AND sql IS NOT NULL",
        (table,)
    ).fetchall()
    return [r[0] for r in rows]

def migrate_table(local, turso, table):
    schema = get_schema(local, table)
    if not schema:
        print(f"  [SKIP] Table '{table}' absente en local")
        return 0

    # Créer la table sur Turso (IF NOT EXISTS pour être idempotent)
    create_sql = schema.replace(
        f"CREATE TABLE {table}",
        f"CREATE TABLE IF NOT EXISTS {table}"
    ).replace(
        f"CREATE TABLE \"{table}\"",
        f"CREATE TABLE IF NOT EXISTS \"{table}\""
    )
    turso.execute(create_sql)
    turso.commit()

    # Créer les index
    for idx_sql in get_indexes(local, table):
        idx_sql_safe = idx_sql.replace("CREATE INDEX", "CREATE INDEX IF NOT EXISTS") \
                               .replace("CREATE UNIQUE INDEX", "CREATE UNIQUE INDEX IF NOT EXISTS")
        try:
            turso.execute(idx_sql_safe)
            turso.commit()
        except Exception as e:
            print(f"  [WARN] Index ignoré ({e})")

    # Lire les données locales
    rows = local.execute(f"SELECT * FROM {table}").fetchall()
    if not rows:
        print(f"  [OK] '{table}' — 0 ligne")
        return 0

    cols = rows[0].keys()
    placeholders = ", ".join(["?" for _ in cols])
    col_names    = ", ".join(cols)
    insert_sql   = f"INSERT OR IGNORE INTO {table} ({col_names}) VALUES ({placeholders})"

    count = 0
    for row in rows:
        try:
            turso.execute(insert_sql, list(row))
            count += 1
        except Exception as e:
            print(f"  [WARN] Ligne ignorée dans '{table}' : {e}")

    turso.commit()
    print(f"  [OK] '{table}' — {count}/{len(rows)} lignes migrées")
    return count

def main():
    print("=" * 55)
    print("  Migration SQLite → Turso")
    print("=" * 55)

    local = get_local()
    print(f"\n[1] Connexion locale : {SQLITE_PATH} ✓")

    print(f"[2] Connexion Turso  : {TURSO_URL}")
    try:
        turso = get_turso()
        turso.execute("SELECT 1")
        print("    Connexion Turso ✓\n")
    except Exception as e:
        print(f"\n[ERREUR] Impossible de se connecter à Turso : {e}")
        sys.exit(1)

    total = 0
    for table in TABLES:
        total += migrate_table(local, turso, table)

    local.close()

    print(f"\n{'=' * 55}")
    print(f"  Migration terminée — {total} lignes transférées au total")
    print(f"{'=' * 55}")
    print("\nProchaine étape : adapter database.py pour utiliser Turso.")

if __name__ == "__main__":
    main()