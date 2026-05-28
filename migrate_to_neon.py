"""
migrate_to_neon.py — Migre presences.db (SQLite) vers Neon PostgreSQL.
Usage : python3 migrate_to_neon.py
"""
import sqlite3
import psycopg2
import os, sys

SQLITE_PATH  = "presences.db"
DATABASE_URL = "postgresql://neondb_owner:npg_jUzh5En1dSXK@ep-snowy-boat-aqdro28q.c-8.us-east-1.aws.neon.tech/neondb?sslmode=require"

TABLES = [
    ("employes",             ["prno","nom_complet","email","actif"]),
    ("liaisons",             ["telegram_id","prno","username","dans_groupe"]),
    ("pointages",            ["message_id","telegram_id","prno","date_local","heure_locale","type_pointage","session","raw_text"]),
    ("jours_feries",         ["date_str","libelle","recurrent","type_ferie"]),
    ("meta",                 ["key","value"]),
    ("horaires",             ["prno","code_horaire","date_effet","commentaire"]),
    ("historique_horaires",  ["prno","code_horaire","date_effet","date_fin"]),
    ("releves",              ["date_debut","date_fin","libelle","clos"]),
    ("exceptions_horaires",  ["prno","date_str","code_horaire","motif"]),
    ("liaisons_empreintes",  ["fingerprint_id","pin_id","prno"]),
]

def table_exists_sqlite(cur, table):
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
    return cur.fetchone() is not None

CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS employes (
    prno TEXT PRIMARY KEY, nom_complet TEXT NOT NULL,
    email TEXT, actif INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT NOW(), updated_at TIMESTAMP DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS liaisons (
    telegram_id BIGINT PRIMARY KEY, prno TEXT NOT NULL,
    username TEXT, dans_groupe INTEGER DEFAULT 1,
    lie_le TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY(prno) REFERENCES employes(prno)
);
CREATE TABLE IF NOT EXISTS pointages (
    id SERIAL PRIMARY KEY, message_id TEXT NOT NULL UNIQUE,
    telegram_id BIGINT NOT NULL, prno TEXT NOT NULL,
    date_local TEXT NOT NULL, heure_locale TEXT NOT NULL,
    type_pointage TEXT NOT NULL, session TEXT NOT NULL,
    raw_text TEXT, created_at TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY(prno) REFERENCES employes(prno)
);
CREATE TABLE IF NOT EXISTS jours_feries (
    id SERIAL PRIMARY KEY, date_str TEXT NOT NULL UNIQUE,
    libelle TEXT NOT NULL, recurrent INTEGER DEFAULT 0,
    type_ferie TEXT DEFAULT 'fixe', created_at TIMESTAMP DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS horaires (
    prno TEXT PRIMARY KEY, code_horaire TEXT NOT NULL,
    date_effet TEXT NOT NULL, commentaire TEXT,
    created_at TIMESTAMP DEFAULT NOW(), updated_at TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY(prno) REFERENCES employes(prno)
);
CREATE TABLE IF NOT EXISTS historique_horaires (
    id SERIAL PRIMARY KEY, prno TEXT NOT NULL,
    code_horaire TEXT NOT NULL, date_effet TEXT NOT NULL,
    date_fin TEXT, created_at TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY(prno) REFERENCES employes(prno)
);
CREATE TABLE IF NOT EXISTS releves (
    id SERIAL PRIMARY KEY, date_debut TEXT NOT NULL,
    date_fin TEXT NOT NULL, libelle TEXT,
    clos INTEGER DEFAULT 0, created_at TIMESTAMP DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS exceptions_horaires (
    id SERIAL PRIMARY KEY, prno TEXT NOT NULL,
    date_str TEXT NOT NULL, code_horaire TEXT NOT NULL,
    motif TEXT, created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(prno, date_str)
);
CREATE TABLE IF NOT EXISTS liaisons_empreintes (
    fingerprint_id INTEGER, pin_id INTEGER UNIQUE,
    prno TEXT NOT NULL, enregistre_le TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY(prno) REFERENCES employes(prno)
);
"""

def migrate():
    print("="*55)
    print("  Migration SQLite → Neon PostgreSQL")
    print("="*55)

    if not os.path.exists(SQLITE_PATH):
        print(f"[ERREUR] {SQLITE_PATH} introuvable")
        sys.exit(1)

    local = sqlite3.connect(SQLITE_PATH)
    local.row_factory = sqlite3.Row
    lc = local.cursor()

    print(f"\n[1] SQLite local : {SQLITE_PATH} ✓")
    print(f"[2] Connexion Neon...")
    try:
        pg = psycopg2.connect(DATABASE_URL)
        pgc = pg.cursor()
        pgc.execute("SELECT 1")
        print("    Connexion Neon ✓")
    except Exception as e:
        print(f"[ERREUR] Neon : {e}")
        sys.exit(1)

    print("[3] Création des tables...")
    for stmt in CREATE_TABLES_SQL.split(";"):
        stmt = stmt.strip()
        if stmt:
            pgc.execute(stmt)
    pg.commit()
    print("    Tables créées ✓\n")

    total = 0
    for table, cols in TABLES:
        if not table_exists_sqlite(lc, table):
            print(f"  [SKIP] '{table}' absente en local")
            continue

        rows = local.execute(f"SELECT {','.join(cols)} FROM {table}").fetchall()
        if not rows:
            print(f"  [OK] '{table}' — 0 ligne")
            continue

        placeholders = ",".join(["%s"] * len(cols))
        col_str      = ",".join(cols)

        # Build ON CONFLICT clause
        pk_map = {
            "employes": "prno", "liaisons": "telegram_id",
            "pointages": "message_id", "jours_feries": "date_str",
            "meta": "key", "horaires": "prno",
            "exceptions_horaires": "(prno,date_str)",
        }
        pk = pk_map.get(table)
        if pk:
            sql = f"INSERT INTO {table} ({col_str}) VALUES ({placeholders}) ON CONFLICT({pk}) DO NOTHING"
        else:
            sql = f"INSERT INTO {table} ({col_str}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"

        count = 0
        for row in rows:
            try:
                pgc.execute(sql, [row[c] for c in cols])
                count += 1
            except Exception as e:
                pg.rollback()
                print(f"  [WARN] Ligne ignorée dans '{table}' : {e}")
                pgc = pg.cursor()
        pg.commit()
        print(f"  [OK] '{table}' — {count}/{len(rows)} lignes migrées")
        total += count

    local.close()
    pg.close()
    print(f"\n{'='*55}")
    print(f"  Migration terminée — {total} lignes au total")
    print(f"{'='*55}")

if __name__ == "__main__":
    migrate()