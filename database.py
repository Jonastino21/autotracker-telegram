"""
database.py — Toutes les opérations SQLite pour le tracker KAROKA.
Tables : employes, liaisons, pointages, jours_feries, meta
"""

import csv
import sqlite3
import logging
from collections import defaultdict
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
import os

logger = logging.getLogger(__name__)

DB_PATH  = os.getenv("DB_PATH",  "presences.db")
TIMEZONE = os.getenv("TIMEZONE", "Indian/Antananarivo")

JOURS_FERIES_FIXES_MG = [
    ("01-01", "Jour de l'An"),
    ("03-29", "Commémoration 1947"),
    ("05-01", "Fête du Travail"),
    ("06-26", "Fête de l'Indépendance"),
    ("08-15", "Assomption"),
    ("11-01", "Toussaint"),
    ("11-02", "Fête des Morts"),
    ("12-25", "Noël"),
]
# Jours fériés variables (dates qui changent chaque année, ex: Aïd)
# Ces jours sont saisis manuellement via l'interface admin avec type='variable'


def get_tz():
    return ZoneInfo(TIMEZONE)


def get_connection():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")  # 30 secondes en ms
    return conn


def init_db():
    conn = get_connection()
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS employes (
                prno        TEXT PRIMARY KEY,
                nom_complet TEXT NOT NULL,
                email       TEXT,
                actif       INTEGER DEFAULT 1,
                created_at  TEXT DEFAULT (datetime('now')),
                updated_at  TEXT DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS liaisons (
                telegram_id  INTEGER PRIMARY KEY,
                prno         TEXT NOT NULL,
                username     TEXT,
                dans_groupe  INTEGER DEFAULT 1,
                lie_le       TEXT DEFAULT (datetime('now')),
                FOREIGN KEY(prno) REFERENCES employes(prno)
            );
            CREATE TABLE IF NOT EXISTS pointages (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id      INTEGER NOT NULL,
                telegram_id     INTEGER NOT NULL,
                prno            TEXT NOT NULL,
                date_local      TEXT NOT NULL,
                heure_locale    TEXT NOT NULL,
                type_pointage   TEXT NOT NULL,
                session         TEXT NOT NULL,
                raw_text        TEXT,
                created_at      TEXT DEFAULT (datetime('now')),
                UNIQUE(message_id),
                FOREIGN KEY(prno) REFERENCES employes(prno)
            );
            CREATE TABLE IF NOT EXISTS jours_feries (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                date_str    TEXT NOT NULL UNIQUE,
                libelle     TEXT NOT NULL,
                recurrent   INTEGER DEFAULT 0,
                type_ferie  TEXT DEFAULT 'fixe',
                created_at  TEXT DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS meta (
                key   TEXT PRIMARY KEY,
                value TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_pointages_date ON pointages(date_local);
            CREATE INDEX IF NOT EXISTS idx_pointages_prno ON pointages(prno);
            CREATE INDEX IF NOT EXISTS idx_pointages_tgid ON pointages(telegram_id);
            CREATE INDEX IF NOT EXISTS idx_liaisons_prno  ON liaisons(prno);

            CREATE TABLE IF NOT EXISTS horaires (
                prno         TEXT PRIMARY KEY,
                code_horaire TEXT NOT NULL,
                date_effet   TEXT NOT NULL,
                commentaire  TEXT,
                created_at   TEXT DEFAULT (datetime('now')),
                updated_at   TEXT DEFAULT (datetime('now')),
                FOREIGN KEY(prno) REFERENCES employes(prno)
            );

            CREATE TABLE IF NOT EXISTS historique_horaires (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                prno         TEXT NOT NULL,
                code_horaire TEXT NOT NULL,
                date_effet   TEXT NOT NULL,
                date_fin     TEXT,
                created_at   TEXT DEFAULT (datetime('now')),
                FOREIGN KEY(prno) REFERENCES employes(prno)
            );

            CREATE TABLE IF NOT EXISTS releves (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                date_debut  TEXT NOT NULL,
                date_fin    TEXT NOT NULL,
                libelle     TEXT,
                clos        INTEGER DEFAULT 0,
                created_at  TEXT DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS exceptions_horaires (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                prno         TEXT NOT NULL,
                date_str     TEXT NOT NULL,
                code_horaire TEXT NOT NULL,
                motif        TEXT,
                created_at   TEXT DEFAULT (datetime('now')),
                UNIQUE(prno, date_str)
            );
        """)
        conn.commit()
        _migrate_db(conn)
        _seed_jours_feries(conn)
        _seed_recurring_custom(conn)
        logger.info("Base de données initialisée : %s", DB_PATH)
    finally:
        conn.close()


def _migrate_db(conn):
    """Migrations pour ajouter les nouvelles colonnes sur une base existante."""
    try:
        conn.execute("ALTER TABLE jours_feries ADD COLUMN type_ferie TEXT DEFAULT 'fixe'")
        conn.commit()
    except Exception:
        pass  # Colonne déjà existante
    try:
        conn.execute("ALTER TABLE horaires ADD COLUMN commentaire TEXT")
        conn.commit()
    except Exception:
        pass
    # Provenance du pointage : telegram / empreinte / badge / pin
    try:
        conn.execute("ALTER TABLE pointages ADD COLUMN source TEXT DEFAULT 'telegram'")
        conn.commit()
    except Exception:
        pass
    for col in ("rotation_cycle", "rotation_ref_date", "dimanche_tour_ref",
                "dimanche_tour_cycle", "categorie", "ferie_tour_rang"):
        try:
            conn.execute(f"ALTER TABLE employes ADD COLUMN {col} TEXT")
            conn.commit()
        except Exception:
            pass
    # Heure d'effet (régularisation du jour d'intégration)
    for tbl in ("horaires", "historique_horaires"):
        try:
            conn.execute(f"ALTER TABLE {tbl} ADD COLUMN heure_effet TEXT")
            conn.commit()
        except Exception:
            pass
    # Table releves
    conn.execute("""
        CREATE TABLE IF NOT EXISTS releves (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            date_debut  TEXT NOT NULL,
            date_fin    TEXT NOT NULL,
            libelle     TEXT,
            clos        INTEGER DEFAULT 0,
            created_at  TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()

    # ── Déduplication des pointages — exécutée une seule fois ────────────
    dedup_done = conn.execute(
        "SELECT value FROM meta WHERE key='dedup_done'"
    ).fetchone()
    if not dedup_done:
        try:
            conn.execute("""
                DELETE FROM pointages
                WHERE id NOT IN (
                    SELECT MIN(id)
                    FROM pointages
                    GROUP BY prno, date_local, type_pointage, session
                )
            """)
            conn.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('dedup_done','1')")
            conn.commit()
        except Exception as e:
            logger.warning("Deduplication pointages : %s", e)

    # ── Index unique pour prévenir les futurs doublons ─────────────────────
    try:
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_pointages_unique_type_session
            ON pointages(prno, date_local, type_pointage, session)
        """)
        conn.commit()
    except Exception as e:
        logger.warning("Index unique pointages : %s", e)


def _seed_jours_feries(conn):
    """Insère les jours fériés fixes malgaches pour l'année courante et suivante."""
    annee_courante = date.today().year
    for annee in [annee_courante, annee_courante + 1]:
        for mois_jour, libelle in JOURS_FERIES_FIXES_MG:
            date_str = f"{annee}-{mois_jour}"
            conn.execute("""
                INSERT OR IGNORE INTO jours_feries(date_str, libelle, recurrent, type_ferie)
                VALUES(?, ?, 1, 'fixe')
            """, (date_str, libelle))
    conn.commit()


def _seed_recurring_custom(conn):
    """Auto-crée pour l'année courante et suivante les fériés fixes récurrents ajoutés manuellement."""
    annee_courante = date.today().year
    rows = conn.execute("""
        SELECT DISTINCT substr(date_str, 6) AS mois_jour, libelle
        FROM jours_feries
        WHERE recurrent=1 AND type_ferie='fixe'
    """).fetchall()
    for row in rows:
        for annee in [annee_courante, annee_courante + 1]:
            new_date = f"{annee}-{row['mois_jour']}"
            conn.execute("""
                INSERT OR IGNORE INTO jours_feries(date_str, libelle, recurrent, type_ferie)
                VALUES(?, ?, 1, 'fixe')
            """, (new_date, row['libelle']))
    conn.commit()


# ─── META ────────────────────────────────────────────────────────────────────

def get_meta(key, default=None):
    conn = get_connection()
    try:
        row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default
    finally:
        conn.close()


def set_meta(key, value):
    conn = get_connection()
    try:
        conn.execute("INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)", (key, str(value)))
        conn.commit()
    finally:
        conn.close()


# ─── JOURS FÉRIÉS ────────────────────────────────────────────────────────────

def get_jours_feries(annee=None):
    conn = get_connection()
    try:
        if annee:
            rows = conn.execute(
                "SELECT * FROM jours_feries WHERE date_str LIKE ? ORDER BY date_str",
                (f"{annee}%",)
            ).fetchall()
        else:
            rows = conn.execute("SELECT * FROM jours_feries ORDER BY date_str").fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_jours_feries_set(annee=None):
    """Retourne un set de date_str pour vérification rapide."""
    return {j["date_str"] for j in get_jours_feries(annee)}


def is_jour_ferie(date_str):
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT id FROM jours_feries WHERE date_str=?", (date_str,)
        ).fetchone()
        return row is not None
    finally:
        conn.close()


def ajouter_jour_ferie(date_str, libelle, recurrent=False, type_ferie='fixe'):
    conn = get_connection()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO jours_feries(date_str, libelle, recurrent, type_ferie) VALUES(?,?,?,?)",
            (date_str, libelle, int(recurrent), type_ferie)
        )
        conn.commit()
        return {"ok": True, "date_str": date_str, "libelle": libelle, "type_ferie": type_ferie}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


def supprimer_jour_ferie(date_str):
    conn = get_connection()
    try:
        conn.execute("DELETE FROM jours_feries WHERE date_str=?", (date_str,))
        conn.commit()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


# ─── EMPLOYÉS ────────────────────────────────────────────────────────────────

def import_csv_employes(csv_path):
    result = {"imported": 0, "errors": []}
    conn = get_connection()
    try:
        with open(csv_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader, 1):
                prno = (row.get("prno") or "").strip().lower()
                nom  = (row.get("nom_complet") or row.get("nom_prenom") or "").strip()
                if not prno or not nom:
                    result["errors"].append(f"Ligne {i} : prno ou nom manquant")
                    continue
                conn.execute("""
                    INSERT INTO employes(prno, nom_complet, email)
                    VALUES(?,?,?)
                    ON CONFLICT(prno) DO UPDATE SET
                        nom_complet=excluded.nom_complet,
                        email=excluded.email,
                        updated_at=datetime('now')
                """, (prno, nom, f"{prno}.karoka@gmail.com"))
                result["imported"] += 1
        conn.commit()
    except FileNotFoundError:
        result["errors"].append(f"Fichier introuvable : {csv_path}")
    except Exception as e:
        result["errors"].append(str(e))
    finally:
        conn.close()
    return result


def ajouter_employe(prno, nom_complet, categorie=None):
    prno = prno.strip().lower()
    nom_complet = nom_complet.strip()
    categorie = (categorie or "standard").strip().lower() or "standard"
    if not prno or not nom_complet:
        return {"ok": False, "error": "PRNO et nom obligatoires"}
    conn = get_connection()
    try:
        conn.execute("""
            INSERT INTO employes(prno, nom_complet, email, categorie)
            VALUES(?,?,?,?)
            ON CONFLICT(prno) DO UPDATE SET
                nom_complet=excluded.nom_complet,
                categorie=excluded.categorie,
                updated_at=datetime('now')
        """, (prno, nom_complet, f"{prno}.karoka@gmail.com", categorie))
        conn.commit()
        return {"ok": True, "prno": prno, "nom_complet": nom_complet}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


def modifier_employe(prno, nouveau_prno=None, nouveau_nom=None, categorie=None):
    """Modifie le nom, la catégorie et/ou renomme le PRNO d'un employé.
    Le renommage du PRNO se répercute sur toutes les tables liées."""
    prno = (prno or "").strip().lower()
    if not prno:
        return {"ok": False, "error": "PRNO manquant"}
    conn = get_connection()
    try:
        emp = conn.execute("SELECT * FROM employes WHERE prno=?", (prno,)).fetchone()
        if not emp:
            return {"ok": False, "error": f"Employé '{prno}' introuvable"}

        # 1. Nom
        if nouveau_nom is not None:
            nom = nouveau_nom.strip()
            if not nom:
                return {"ok": False, "error": "Le nom ne peut pas être vide"}
            conn.execute(
                "UPDATE employes SET nom_complet=?, updated_at=datetime('now') WHERE prno=?",
                (nom, prno))

        # 1b. Catégorie
        if categorie is not None:
            cat = (categorie or "").strip().lower() or "standard"
            conn.execute(
                "UPDATE employes SET categorie=?, updated_at=datetime('now') WHERE prno=?",
                (cat, prno))

        # 2. Renommage du PRNO (répercuté sur toutes les tables liées)
        new = (nouveau_prno or "").strip().lower()
        if new and new != prno:
            if conn.execute("SELECT 1 FROM employes WHERE prno=?", (new,)).fetchone():
                return {"ok": False, "error": f"Le PRNO '{new}' existe déjà"}
            conn.commit()                              # clore toute transaction
            conn.execute("PRAGMA foreign_keys=OFF")    # (hors transaction)
            existantes = {r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
            for t in ("employes", "liaisons", "pointages", "horaires",
                      "historique_horaires", "exceptions_horaires",
                      "liaisons_empreintes"):
                if t in existantes:
                    conn.execute(f"UPDATE {t} SET prno=? WHERE prno=?", (new, prno))
            # Aligner l'email auto-généré s'il portait l'ancien PRNO
            conn.execute(
                "UPDATE employes SET email=? WHERE prno=? AND email=?",
                (f"{new}.karoka@gmail.com", new, f"{prno}.karoka@gmail.com"))
            conn.execute(
                "UPDATE employes SET updated_at=datetime('now') WHERE prno=?", (new,))
            conn.commit()
            conn.execute("PRAGMA foreign_keys=ON")
            logger.info("PRNO renommé : %s → %s", prno, new)
            prno = new

        conn.commit()
        row = conn.execute(
            "SELECT prno, nom_complet FROM employes WHERE prno=?", (prno,)).fetchone()
        return {"ok": True, "prno": row["prno"], "nom_complet": row["nom_complet"]}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


def desactiver_employe(prno):
    """
    Désactive un employé :
    - marque actif=0
    - supprime la liaison Telegram (permet re-onboarding si réactivé)
    - retourne le telegram_id pour retirer du groupe
    """
    prno = prno.strip().lower()
    conn = get_connection()
    try:
        conn.execute(
            "UPDATE employes SET actif=0, updated_at=datetime('now') WHERE prno=?",
            (prno,)
        )
        row = conn.execute(
            "SELECT telegram_id FROM liaisons WHERE prno=?", (prno,)
        ).fetchone()
        telegram_id = row["telegram_id"] if row else None
        # Supprimer la liaison → permet re-onboarding si l'employé est réactivé
        conn.execute("DELETE FROM liaisons WHERE prno=?", (prno,))
        conn.commit()
        return {"ok": True, "telegram_id": telegram_id}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


def reactiver_employe(prno: str) -> dict:
    """Réactive un employé désactivé (sans recréer la liaison — l'employé doit re-onboarder)."""
    prno = prno.strip().lower()
    conn = get_connection()
    try:
        conn.execute(
            "UPDATE employes SET actif=1, updated_at=datetime('now') WHERE prno=?",
            (prno,)
        )
        conn.commit()
        return {"ok": True, "prno": prno}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


def get_all_employes(actifs_only=True):
    conn = get_connection()
    try:
        q = "SELECT * FROM employes"
        if actifs_only:
            q += " WHERE actif=1"
        q += " ORDER BY nom_complet"
        return [dict(r) for r in conn.execute(q).fetchall()]
    finally:
        conn.close()


def get_employe_by_prno(prno):
    conn = get_connection()
    try:
        row = conn.execute("SELECT * FROM employes WHERE prno=?", (prno.lower(),)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


# ─── LIAISONS ────────────────────────────────────────────────────────────────

def get_liaison(telegram_id):
    conn = get_connection()
    try:
        row = conn.execute("""
            SELECT l.*, e.nom_complet, e.email, e.actif
            FROM liaisons l JOIN employes e ON l.prno=e.prno
            WHERE l.telegram_id=?
        """, (telegram_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_liaison_by_prno(prno):
    conn = get_connection()
    try:
        row = conn.execute("SELECT * FROM liaisons WHERE prno=?", (prno.lower(),)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def creer_liaison(telegram_id, prno, username=""):
    prno = prno.strip().lower()
    employe = get_employe_by_prno(prno)
    if not employe:
        return {"ok": False, "code": "PRNO_INCONNU",
                "message": "❌ PRNO non reconnu. Contactez le service RH."}

    # Si l'employé existe mais est inactif → le réactiver automatiquement
    if not employe.get("actif", 1):
        reactiver_employe(prno)
        employe = get_employe_by_prno(prno)

    liaison_existante = get_liaison(telegram_id)
    if liaison_existante:
        return {"ok": True, "code": "DEJA_LIE",
                "message": f"✅ Déjà enregistré(e) : *{liaison_existante['nom_complet']}*",
                "employe": liaison_existante}

    liaison_prno = get_liaison_by_prno(prno)
    if liaison_prno and liaison_prno["telegram_id"] != telegram_id:
        logger.warning("ALERTE RH : PRNO %s conflit telegram_id %s vs %s",
                       prno, liaison_prno["telegram_id"], telegram_id)
        return {"ok": False, "code": "PRNO_DEJA_UTILISE",
                "message": "❌ PRNO déjà associé à un autre compte. Contactez le RH."}

    conn = get_connection()
    try:
        conn.execute("""
            INSERT INTO liaisons(telegram_id, prno, username, dans_groupe)
            VALUES(?,?,?,0)
        """, (telegram_id, prno, username or ""))
        conn.commit()
        logger.info("Liaison créée : %s ↔ %s (%s)", telegram_id, prno, employe["nom_complet"])
        return {
            "ok": True, "code": "LIAISON_CREEE",
            "message": (f"✅ Bonjour *{employe['nom_complet']}* !\n"
                        f"Votre compte est activé. Vous allez recevoir le lien du groupe."),
            "employe": employe,
        }
    except Exception as e:
        return {"ok": False, "code": "ERREUR", "message": str(e)}
    finally:
        conn.close()


def marquer_dans_groupe(telegram_id, dans_groupe):
    conn = get_connection()
    try:
        conn.execute("UPDATE liaisons SET dans_groupe=? WHERE telegram_id=?",
                     (int(dans_groupe), telegram_id))
        conn.commit()
    finally:
        conn.close()


def retirer_liaison(telegram_id):
    conn = get_connection()
    try:
        row = conn.execute("SELECT prno FROM liaisons WHERE telegram_id=?", (telegram_id,)).fetchone()
        if row:
            conn.execute("UPDATE liaisons SET dans_groupe=0 WHERE telegram_id=?", (telegram_id,))
            conn.execute("UPDATE employes SET actif=0, updated_at=datetime('now') WHERE prno=?",
                         (row["prno"],))
            conn.commit()
            logger.info("Retrait liaison : telegram_id=%s prno=%s", telegram_id, row["prno"])
            return row["prno"]
    except Exception as e:
        logger.error("Erreur retrait liaison : %s", e)
    finally:
        conn.close()
    return None


def get_all_liaisons():
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT l.telegram_id, l.prno, l.username, l.lie_le, l.dans_groupe, e.nom_complet, e.actif
            FROM liaisons l JOIN employes e ON l.prno=e.prno ORDER BY e.nom_complet
        """).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def supprimer_employe_permanent(prno: str) -> dict:
    """Supprime définitivement un employé et toutes ses données liées."""
    prno = prno.strip().lower()
    conn = get_connection()
    try:
        conn.execute("DELETE FROM pointages              WHERE prno=?", (prno,))
        conn.execute("DELETE FROM liaisons               WHERE prno=?", (prno,))
        conn.execute("DELETE FROM horaires               WHERE prno=?", (prno,))
        conn.execute("DELETE FROM historique_horaires    WHERE prno=?", (prno,))
        conn.execute("DELETE FROM exceptions_horaires    WHERE prno=?", (prno,))
        conn.execute("DELETE FROM liaisons_empreintes    WHERE prno=?", (prno,))
        conn.execute("DELETE FROM employes               WHERE prno=?", (prno,))
        conn.commit()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


def supprimer_liaison(prno: str) -> dict:
    """Supprime la liaison Telegram sans désactiver l'employé (permet le re-onboarding)."""
    prno = prno.strip().lower()
    conn = get_connection()
    try:
        conn.execute("DELETE FROM liaisons WHERE prno=?", (prno,))
        conn.commit()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


def get_employes_non_lies():
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT e.* FROM employes e
            LEFT JOIN liaisons l ON e.prno=l.prno
            WHERE l.telegram_id IS NULL AND e.actif=1 ORDER BY e.nom_complet
        """).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ─── POINTAGES ───────────────────────────────────────────────────────────────

def get_dernier_pointage_type(prno: str, date_local: str) -> str | None:
    """Retourne le type du dernier pointage de l'employé pour ce jour, ou None."""
    conn = get_connection()
    try:
        row = conn.execute("""
            SELECT type_pointage FROM pointages
            WHERE prno=? AND date_local=?
            ORDER BY heure_locale DESC LIMIT 1
        """, (prno, date_local)).fetchone()
        return row["type_pointage"] if row else None
    finally:
        conn.close()


def get_dernier_pointage(prno: str, date_local: str) -> dict | None:
    """Retourne le dernier pointage (type + heure) de l'employé ce jour, ou None."""
    conn = get_connection()
    try:
        row = conn.execute("""
            SELECT type_pointage, heure_locale FROM pointages
            WHERE prno=? AND date_local=?
            ORDER BY heure_locale DESC LIMIT 1
        """, (prno, date_local)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def insert_pointage(message_id, telegram_id, prno, date_local, heure_locale,
                    type_pointage, session, raw_text, source="telegram"):
    conn = get_connection()
    try:
        # Vérifier doublon de type dans la même transaction
        row = conn.execute("""
            SELECT type_pointage FROM pointages
            WHERE prno=? AND date_local=?
            ORDER BY heure_locale DESC LIMIT 1
        """, (prno, date_local)).fetchone()

        if row and row["type_pointage"] == type_pointage:
            return False  # doublon de type, on ignore

        cur = conn.execute("""
            INSERT OR IGNORE INTO pointages
                (message_id, telegram_id, prno, date_local, heure_locale,
                 type_pointage, session, raw_text, source)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (message_id, telegram_id, prno, date_local, heure_locale,
              type_pointage, session, raw_text, source))
        conn.commit()
        inserted = cur.rowcount > 0
        if inserted:
            logger.info("Pointage : %s %s %s %s %s", prno, date_local, heure_locale, type_pointage, session)
        return inserted
    finally:
        conn.close()


def get_pointages_du_jour(date_str):
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT p.*, e.nom_complet FROM pointages p
            LEFT JOIN employes e ON p.prno=e.prno
            WHERE p.date_local=? ORDER BY p.heure_locale
        """, (date_str,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_historique(limit=200, date_str=None, prno=None, source=None, recherche=None):
    """
    Journal des pointages le plus récent en premier, avec nom employé et
    provenance (source). Filtres optionnels : date, employé (prno exact),
    source, et recherche libre (nom ou PRNO, insensible à la casse).
    """
    conn = get_connection()
    try:
        clauses, params = [], []
        if date_str:
            clauses.append("p.date_local=?"); params.append(date_str)
        if prno:
            clauses.append("p.prno=?"); params.append(prno)
        if source:
            clauses.append("COALESCE(p.source,'telegram')=?"); params.append(source)
        if recherche:
            like = f"%{recherche.strip()}%"
            clauses.append("(p.prno LIKE ? OR e.nom_complet LIKE ?)")
            params.extend([like, like])
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(int(limit))
        rows = conn.execute(f"""
            SELECT p.id, p.prno, p.date_local, p.heure_locale, p.type_pointage,
                   p.session, COALESCE(p.source,'telegram') AS source,
                   e.nom_complet
            FROM pointages p
            LEFT JOIN employes e ON p.prno=e.prno
            {where}
            ORDER BY p.date_local DESC, p.heure_locale DESC
            LIMIT ?
        """, params).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_resume_jour(date_str):
    employes  = get_all_employes()
    pointages = get_pointages_du_jour(date_str)
    ferie     = is_jour_ferie(date_str)

    index = defaultdict(list)
    for p in pointages:
        index[(p["prno"], p["session"], p["type_pointage"])].append(p["heure_locale"])

    result = []
    for emp in employes:
        prno    = emp["prno"]
        arr_mat = min(index[(prno,"matin","arrivee")], default=None)
        dep_mat = max(index[(prno,"matin","depart")],  default=None)
        arr_apm = min(index[(prno,"apm","arrivee")],   default=None)
        dep_apm = max(index[(prno,"apm","depart")],    default=None)
        dur_mat = _duree_min(arr_mat, dep_mat)
        dur_apm = _duree_min(arr_apm, dep_apm)
        dur_tot = (dur_mat or 0) + (dur_apm or 0)
        has_any = any([arr_mat, dep_mat, arr_apm, dep_apm])
        complet = all([arr_mat, dep_mat, arr_apm, dep_apm])
        if ferie:
            statut = "Férié"
        elif not has_any:
            statut = "Absent"
        elif complet:
            statut = "Complet"
        else:
            statut = "Incomplet"
        result.append({
            "prno": prno, "nom_prenom": emp["nom_complet"], "date": date_str,
            "arr_mat": arr_mat, "dep_mat": dep_mat,
            "arr_apm": arr_apm, "dep_apm": dep_apm,
            "dur_mat": dur_mat, "dur_apm": dur_apm,
            "dur_tot": dur_tot if dur_tot > 0 else None,
            "statut": statut, "ferie": ferie,
        })
    return sorted(result, key=lambda x: x["nom_prenom"])


def _duree_min(heure_debut, heure_fin):
    if not heure_debut or not heure_fin:
        return None
    try:
        t1 = datetime.strptime(heure_debut, "%H:%M:%S")
        t2 = datetime.strptime(heure_fin,   "%H:%M:%S")
        return max(0, int((t2-t1).total_seconds()/60))
    except Exception:
        return None


def get_resume_periode(date_debut, date_fin):
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT p.prno, p.date_local, p.session, p.type_pointage, p.heure_locale
            FROM pointages p WHERE p.date_local BETWEEN ? AND ?
            ORDER BY p.date_local, p.heure_locale
        """, (date_debut, date_fin)).fetchall()
    finally:
        conn.close()

    feries = get_jours_feries_set()
    index  = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    for r in rows:
        index[r["prno"]][r["date_local"]][(r["session"], r["type_pointage"])].append(r["heure_locale"])

    start, end, all_dates, d = date.fromisoformat(date_debut), date.fromisoformat(date_fin), [], date.fromisoformat(date_debut)
    while d <= end:
        if d.weekday() < 5:
            all_dates.append(d.isoformat())
        d += timedelta(days=1)

    result = []
    for emp in get_all_employes():
        prno = emp["prno"]
        dates_statuts = {}
        for ds in all_dates:
            if ds in feries:
                dates_statuts[ds] = "Férié"
                continue
            pts     = index[prno][ds]
            has_any = bool(pts)
            complet = all([pts[("matin","arrivee")], pts[("matin","depart")],
                           pts[("apm","arrivee")],   pts[("apm","depart")]])
            dates_statuts[ds] = "Absent" if not has_any else ("Complet" if complet else "Incomplet")
        result.append({"prno": prno, "nom_prenom": emp["nom_complet"], "dates": dates_statuts})
    return sorted(result, key=lambda x: x["nom_prenom"])


def get_stats_jour(date_str):
    resume = get_resume_jour(date_str)
    ferie  = is_jour_ferie(date_str)
    return {
        "total":      len(resume),
        "complets":   sum(1 for r in resume if r["statut"] == "Complet"),
        "incomplets": sum(1 for r in resume if r["statut"] == "Incomplet"),
        "absents":    sum(1 for r in resume if r["statut"] == "Absent"),
        "ferie":      ferie,
    }


# ─── ROTATION ────────────────────────────────────────────────────────────────

def _est_jour_repos_rotation_from_emp(emp: dict, date_str: str) -> bool:
    """Retourne True si la date est un jour de repos dans la rotation cyclique (sans requête DB)."""
    rotation_cycle    = emp.get("rotation_cycle")
    rotation_ref_date = emp.get("rotation_ref_date")
    if not rotation_cycle or not rotation_ref_date:
        return False
    try:
        parts = rotation_cycle.split("/")
        jours_travail = int(parts[0])
        cycle_total   = int(parts[1])
        ref   = date.fromisoformat(rotation_ref_date)
        check = date.fromisoformat(date_str)
        delta = (check - ref).days % cycle_total
        return delta >= jours_travail
    except Exception:
        return False


def _est_dimanche_tour_from_emp(emp: dict, date_str: str) -> bool:
    """Retourne True si ce dimanche est le tour de garde de cet employé (sans requête DB).

    Le cycle est paramétrable via dimanche_tour_cycle = nombre de gardiens en
    rotation dominicale (défaut 3 → un dimanche sur 3, soit un cycle de 21 jours).
    """
    dimanche_tour_ref = emp.get("dimanche_tour_ref")
    if not dimanche_tour_ref:
        return False
    try:
        check = date.fromisoformat(date_str)
        if check.weekday() != 6:
            return False
        cycle_jours = _nb_gardiens_tour(emp) * 7
        ref   = date.fromisoformat(dimanche_tour_ref)
        return (check - ref).days % cycle_jours == 0
    except Exception:
        return False


# ── Génération dynamique du planning des gardiens ──────────────────────────────
# Les gardiens ne suivent pas un code_horaire statique : leur planning se déduit
# de leur cycle (2 nuits / 1 repos) + leur tour de garde dominical.
#   - nuit normale : 16:45 → 08:15 (déduit du code de base)
#   - tour du dimanche : garde finissant TOUJOURS lundi 08:15, début variable :
#       • samedi travaillé  → bloc continu Sam 16:45 → Lun 08:15 (~39h30)
#       • samedi repos      → Dim 08:00 → Lun 08:15 (~24h15)
#   - le repos se décale de +1 jour à chaque tour (le dimanche ajoute un jour).

GARDE_DEBUT_MIN = 8 * 60          # (déprécié) — ancienne prise de garde dominicale
JOUR_TOUR_DEBUT = 8 * 60          # tour de JOUR (dimanche / férié) : 08:00
JOUR_TOUR_FIN   = 17 * 60         #                                 → 17:00
_JOURS_CODE = ["lu", "ma", "me", "je", "ve", "sa", "di"]


def _min_to_hhmm(m: int) -> str:
    m %= 1440
    return f"{m // 60:02d}{m % 60:02d}"


def _est_gardien(emp: dict) -> bool:
    """Vrai si l'employé est un gardien (catégorie). Rétro-compat : si la catégorie
    n'est pas renseignée, on déduit de la présence d'une config de rotation."""
    if not emp:
        return False
    cat = (emp.get("categorie") or "").strip().lower()
    if cat == "gardien":
        return True
    if cat:                       # catégorie explicite non-gardien
        return False
    return bool(emp.get("rotation_cycle") and emp.get("rotation_ref_date")
                and emp.get("dimanche_tour_ref"))


def _est_jardinier(emp: dict) -> bool:
    return bool(emp and (emp.get("categorie") or "").strip().lower() == "jardinier")


def _mercredi_ref(d):
    """Mercredi de la semaine de d (le mercredi <= d le plus proche)."""
    return d - timedelta(days=(d.weekday() - 2) % 7)


def _recups_semaine(emp: dict, W, feries) -> list:
    """Jours de récup posés pour le mercredi-cible W : un par tour (dimanche/férié)
    dont le 1er mercredi suivant tombe sur W, empilés à partir de W sur les jours
    ouvrés (on saute dimanches et fériés)."""
    # Tours dont le 1er mercredi suivant == W : ils tombent dans [W-7, W)
    count = 0
    dim = W - timedelta(days=3)                      # le dimanche de cette fenêtre
    if _est_dimanche_tour_from_emp(emp, dim.isoformat()):
        count += 1
    d = W - timedelta(days=7)
    while d < W:                                     # fériés en semaine dans la fenêtre
        if d.weekday() != 6 and d.isoformat() in (feries or ()) \
                and _est_ferie_tour(emp, d, feries):
            count += 1
        d += timedelta(days=1)
    if count == 0:
        return []
    # Empiler count récup sur les jours ouvrés à partir de W (saute dim + fériés)
    allocated, day, garde = [], W, 0
    while len(allocated) < count and garde < 30:
        if day.weekday() != 6 and day.isoformat() not in (feries or ()):
            allocated.append(day)
        day += timedelta(days=1)
        garde += 1
    return allocated


def _est_recup_jardinier(emp: dict, date_obj, feries) -> bool:
    """True si date_obj est un jour de récupération du jardinier."""
    if date_obj.weekday() == 6 or date_obj.isoformat() in (feries or ()):
        return False                                # ni dimanche ni férié
    W = _mercredi_ref(date_obj)
    for Wc in (W, W - timedelta(days=7)):           # semaine courante + débordement
        if date_obj in _recups_semaine(emp, Wc, feries):
            return True
    return False


def _planning_jardinier(emp: dict, date_obj, code: str, feries):
    """Jardinier : horaire standard + tour chaque dimanche ET chaque férié (par
    rang), chacun donnant 1 récup (mercredi suivant, empilable, décalable)."""
    wd = date_obj.weekday()
    ds = date_obj.isoformat()

    # 1. Jour de récupération (chômé, échange)
    if _est_recup_jardinier(emp, date_obj, feries):
        return None, "Récup"

    # 2. Dimanche : tour → travaille (lève (di)) ; sinon exclu
    if wd == 6:
        if _est_dimanche_tour_from_emp(emp, ds):
            code_eff = (";".join(
                s for s in code.split(";")
                if s.strip().lower() not in ("(di)", "(di:am)", "(di:pm)")
            ) or code)
            return code_eff, None
        return None, "Exclu"

    # 3. Férié en semaine : tour → travaille ses heures standard ; sinon chômé payé
    if ds in (feries or ()):
        if _est_ferie_tour(emp, date_obj, feries):
            return code, None
        return None, "Férié"

    # 4. Jour ouvré normal
    return code, None


def _nb_gardiens_tour(emp: dict) -> int:
    try:
        n = int(emp.get("dimanche_tour_cycle") or 3)
    except (TypeError, ValueError):
        n = 3
    return n if n >= 1 else 3


def _est_ferie_tour(emp: dict, date_obj, feries) -> bool:
    """Vrai si ce férié en semaine est le tour de garde de ce gardien.
    Rotation par rang : les fériés en semaine pris dans l'ordre chronologique,
    le k-ième revient au gardien de rang (k mod N)."""
    ds = date_obj.isoformat()
    semaine = sorted(f for f in (feries or ())
                     if date.fromisoformat(f).weekday() != 6)
    if ds not in semaine:
        return False
    try:
        rang = int(emp.get("ferie_tour_rang") or 1)
    except (TypeError, ValueError):
        rang = 1
    N = _nb_gardiens_tour(emp)
    return (semaine.index(ds) % N) == ((rang - 1) % N)


def _est_tour(emp: dict, date_obj, feries) -> bool:
    """Tour de garde d'un jour « à part » : dimanche (rotation hebdo) ou férié (rang)."""
    if date_obj.weekday() == 6:
        return _est_dimanche_tour_from_emp(emp, date_obj.isoformat())
    if date_obj.isoformat() in (feries or ()):
        return _est_ferie_tour(emp, date_obj, feries)
    return False


def _position_cycle(emp: dict, date_obj, feries=None):
    """(position, jours_travail, cycle_total) ou None.

    Le cycle 2 nuits / 1 repos avance TOUS les jours, dimanche et férié compris :
    la nuit du dimanche/férié est une nuit ordinaire de la rotation (le tour de
    JOUR 08h-17h est géré à part, en plus de la nuit, dans `_planning_gardien`).
    """
    rc = emp.get("rotation_cycle")
    rr = emp.get("rotation_ref_date")
    if not rc or not rr:
        return None
    try:
        jt, ct = (int(x) for x in rc.split("/"))
        ref    = date.fromisoformat(rr)
    except Exception:
        return None
    if ct <= 0:
        return None
    return (date_obj - ref).days % ct, jt, ct


def _est_repos_gardien(emp: dict, date_obj, feries=None) -> bool:
    pc = _position_cycle(emp, date_obj, feries)
    if pc is None:
        return False
    pos, jt, _ = pc
    return pos >= jt


def _extraire_nuit(code: str):
    """Heures (debut, fin) de la nuit, lues sur le lundi du code de base."""
    from parser import parse_code_horaire
    plages = parse_code_horaire(code).get(0, {}).get("plages_travail", [])
    if not plages:
        return None
    return plages[0]["debut"], plages[0]["fin"]


def _duree_nuit_gardien(code: str) -> int:
    """Durée (minutes) d'une nuit de cycle — sert à créditer un férié chômé."""
    from parser import _duree_plage
    nuit = _extraire_nuit(code)
    return _duree_plage(nuit[0], nuit[1]) if nuit else 0


def _planning_gardien(emp: dict, date_obj, code: str, feries):
    """Génère (code_effectif|None, force_statut|None) pour un gardien.

    Modèle :
      • Rotation de nuit 2/1 en CONTINU (tous les jours, dimanche et férié inclus) :
        nuit 16h45 → lendemain 08h15 quand le cycle est en « travail ».
      • EN PLUS, tour de JOUR 08h → 17h le dimanche (rotation hebdo) et les fériés
        en semaine (par rang). Additif : un gardien peut cumuler jour + sa nuit.
      • Repos du cycle sans tour → 'Repos'. Plus de statut Exclu/Garde/Férié.

    force_statut ∈ {None, 'Repos'}.
    """
    nuit = _extraire_nuit(code)
    if not nuit:
        return code, None
    deb_n, fin_n = nuit
    wd = date_obj.weekday()
    jc = _JOURS_CODE[wd]
    is_ferie_semaine = wd != 6 and date_obj.isoformat() in (feries or ())

    segments = []
    # 1. Nuit selon le cycle (overnight auto-détecté : fin < début → J+1)
    if not _est_repos_gardien(emp, date_obj, feries):
        segments.append(f"{_min_to_hhmm(deb_n)}{jc}{_min_to_hhmm(fin_n)}")
    # 2. Tour de JOUR (dimanche ou férié en semaine) : 08h00 → 17h00, en plus
    if (wd == 6 or is_ferie_semaine) and _est_tour(emp, date_obj, feries):
        segments.append(f"{_min_to_hhmm(JOUR_TOUR_DEBUT)}{jc}{_min_to_hhmm(JOUR_TOUR_FIN)}")

    if segments:
        return ";".join(segments), None
    return None, "Repos"


def _planning_jour(emp: dict, code: str, date_obj, feries=None):
    """(code_effectif, force_statut) pour n'importe quel employé."""
    if not code or not emp:
        return code, None
    if _est_gardien(emp):
        if feries is None:
            feries = get_jours_feries_set()
        return _planning_gardien(emp, date_obj, code, feries)
    if _est_jardinier(emp):
        if feries is None:
            feries = get_jours_feries_set()
        return _planning_jardinier(emp, date_obj, code, feries)
    return code, None


def get_code_effectif(prno: str, date_str: str) -> str | None:
    """Code effectif d'un jour (utilisé par le tracker pour l'admission)."""
    emp     = get_employe_by_prno(prno)
    horaire = get_horaire(prno, date_str)
    code    = horaire["code_horaire"] if horaire else None
    code_eff, _ = _planning_jour(emp, code, date.fromisoformat(date_str))
    return code_eff


def est_jour_repos_rotation(prno: str, date_str: str) -> bool:
    emp = get_employe_by_prno(prno)
    return _est_jour_repos_rotation_from_emp(emp, date_str) if emp else False


def est_dimanche_tour(prno: str, date_str: str) -> bool:
    emp = get_employe_by_prno(prno)
    return _est_dimanche_tour_from_emp(emp, date_str) if emp else False


def set_rotation(prno: str, rotation_cycle: str = None,
                 rotation_ref_date: str = None, dimanche_tour_ref: str = None,
                 dimanche_tour_cycle: int = None, ferie_tour_rang: int = None) -> dict:
    prno = prno.strip().lower()
    conn = get_connection()
    try:
        conn.execute("""
            UPDATE employes SET
                rotation_cycle      = ?,
                rotation_ref_date   = ?,
                dimanche_tour_ref   = ?,
                dimanche_tour_cycle = ?,
                ferie_tour_rang     = ?,
                updated_at          = datetime('now')
            WHERE prno = ?
        """, (rotation_cycle or None, rotation_ref_date or None,
              dimanche_tour_ref or None, dimanche_tour_cycle or None,
              ferie_tour_rang or None, prno))
        conn.commit()
        return {"ok": True, "prno": prno}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


# ─── HORAIRES ────────────────────────────────────────────────────────────────

def init_horaires_table():
    """Ajoute les tables horaires si elles n'existent pas encore."""
    conn = get_connection()
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS horaires (
                prno         TEXT PRIMARY KEY,
                code_horaire TEXT NOT NULL,
                date_effet   TEXT NOT NULL,
                created_at   TEXT DEFAULT (datetime('now')),
                updated_at   TEXT DEFAULT (datetime('now')),
                FOREIGN KEY(prno) REFERENCES employes(prno)
            );

            CREATE TABLE IF NOT EXISTS historique_horaires (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                prno         TEXT NOT NULL,
                code_horaire TEXT NOT NULL,
                date_effet   TEXT NOT NULL,
                date_fin     TEXT,
                created_at   TEXT DEFAULT (datetime('now')),
                FOREIGN KEY(prno) REFERENCES employes(prno)
            );
        """)
        conn.commit()
    finally:
        conn.close()


def set_horaire(prno: str, code_horaire: str, date_effet: str, commentaire: str = None,
                heure_effet: str = None) -> dict:
    """
    Définit ou met à jour le code horaire d'un employé.
    Archive l'ancien dans historique_horaires avec date_fin = date_effet - 1 jour.
    heure_effet (HH:MM) : régularise le jour d'intégration (heures avant l'heure
    d'effet créditées présentes).
    """
    prno = prno.strip().lower()
    heure_effet = (heure_effet or "").strip() or None
    conn = get_connection()
    try:
        from datetime import date as _date, timedelta
        date_effet_obj = _date.fromisoformat(date_effet)
        date_fin_ancien = (date_effet_obj - timedelta(days=1)).isoformat()

        # Archiver l'ancien horaire si existant et différent
        ancien = conn.execute(
            "SELECT code_horaire, date_effet, heure_effet FROM horaires WHERE prno=?", (prno,)
        ).fetchone()
        if ancien:
            # Ne pas archiver si c'est le même code à la même date (pas de changement réel)
            if ancien["code_horaire"] == code_horaire and ancien["date_effet"] == date_effet:
                return {"ok": True, "prno": prno, "code_horaire": code_horaire}
            # Archiver avec date_fin = veille de la nouvelle date_effet
            # Mettre à jour date_fin si l'entrée existe déjà, sinon insérer
            existing = conn.execute("""
                SELECT id FROM historique_horaires WHERE prno=? AND date_effet=?
            """, (prno, ancien["date_effet"])).fetchone()
            if existing:
                conn.execute("""
                    UPDATE historique_horaires SET date_fin=? WHERE prno=? AND date_effet=?
                """, (date_fin_ancien, prno, ancien["date_effet"]))
            else:
                conn.execute("""
                    INSERT INTO historique_horaires(prno, code_horaire, date_effet, date_fin, heure_effet)
                    VALUES(?, ?, ?, ?, ?)
                """, (prno, ancien["code_horaire"], ancien["date_effet"], date_fin_ancien,
                      ancien["heure_effet"]))

        # Insérer ou mettre à jour l'horaire actuel
        conn.execute("""
            INSERT INTO horaires(prno, code_horaire, date_effet, commentaire, heure_effet)
            VALUES(?, ?, ?, ?, ?)
            ON CONFLICT(prno) DO UPDATE SET
                code_horaire = excluded.code_horaire,
                date_effet   = excluded.date_effet,
                commentaire  = excluded.commentaire,
                heure_effet  = excluded.heure_effet,
                updated_at   = datetime('now')
        """, (prno, code_horaire, date_effet, commentaire, heure_effet))
        conn.commit()
        logger.info("Horaire défini : %s → %s (dès %s)", prno, code_horaire, date_effet)
        return {"ok": True, "prno": prno, "code_horaire": code_horaire}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


def get_horaire(prno: str, date_str: str = None) -> dict | None:
    """
    Retourne le code horaire actif pour un employé à une date donnée.
    Priorité : exceptions_horaires > horaires (actuel) > historique_horaires.
    Si date_str=None, retourne le code actuel.
    """
    prno = prno.strip().lower()
    conn = get_connection()
    try:
        if date_str:
            # 1. Vérifier d'abord les exceptions ponctuelles
            exception = conn.execute("""
                SELECT code_horaire, date_str as date_effet
                FROM exceptions_horaires
                WHERE prno=? AND date_str=?
            """, (prno, date_str)).fetchone()
            if exception:
                return {**dict(exception), "est_exception": True}

            # 2. Chercher dans l'historique le code actif à cette date
            row = conn.execute("""
                SELECT code_horaire, date_effet, heure_effet FROM historique_horaires
                WHERE prno=? AND date_effet <= ?
                ORDER BY date_effet DESC LIMIT 1
            """, (prno, date_str)).fetchone()
            # 3. Comparer avec l'horaire actuel
            actuel = conn.execute(
                "SELECT code_horaire, date_effet, heure_effet FROM horaires WHERE prno=?", (prno,)
            ).fetchone()
            if actuel and actuel["date_effet"] <= date_str:
                if not row or actuel["date_effet"] >= row["date_effet"]:
                    return dict(actuel)
            return dict(row) if row else None
        else:
            row = conn.execute(
                "SELECT * FROM horaires WHERE prno=?", (prno,)
            ).fetchone()
            return dict(row) if row else None
    finally:
        conn.close()


# ── Exceptions horaires ───────────────────────────────────────────────────────

def get_exceptions_horaires(prno: str) -> list[dict]:
    """Retourne toutes les exceptions ponctuelles d'un employé, triées par date."""
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT * FROM exceptions_horaires
            WHERE prno=? ORDER BY date_str
        """, (prno,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def set_exception_horaire(prno: str, date_str: str, code_horaire: str, motif: str = None) -> dict:
    """Ajoute ou remplace une exception horaire pour une date précise."""
    prno = prno.strip().lower()
    conn = get_connection()
    try:
        conn.execute("""
            INSERT INTO exceptions_horaires(prno, date_str, code_horaire, motif)
            VALUES(?, ?, ?, ?)
            ON CONFLICT(prno, date_str) DO UPDATE SET
                code_horaire = excluded.code_horaire,
                motif        = excluded.motif
        """, (prno, date_str, code_horaire, motif))
        conn.commit()
        logger.info("Exception horaire ajoutée : %s le %s → %s", prno, date_str, code_horaire)
        return {"ok": True, "prno": prno, "date_str": date_str}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


def supprimer_exception_horaire(prno: str, date_str: str) -> dict:
    """Supprime une exception horaire pour une date précise."""
    prno = prno.strip().lower()
    conn = get_connection()
    try:
        conn.execute("""
            DELETE FROM exceptions_horaires WHERE prno=? AND date_str=?
        """, (prno, date_str))
        conn.commit()
        logger.info("Exception horaire supprimée : %s le %s", prno, date_str)
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


def get_historique_horaires(prno: str) -> list[dict]:
    """Retourne l'historique complet des horaires d'un employé."""
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT * FROM historique_horaires WHERE prno=?
            ORDER BY date_effet DESC
        """, (prno,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_all_horaires() -> dict:
    """Retourne tous les horaires actuels {prno: code_horaire}."""
    conn = get_connection()
    try:
        rows = conn.execute("SELECT prno, code_horaire, date_effet FROM horaires").fetchall()
        return {r["prno"]: dict(r) for r in rows}
    finally:
        conn.close()


def get_pointages_bruts_jour(date_str: str) -> dict:
    """Retourne les pointages bruts par prno pour un jour donné."""
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT prno, type_pointage, heure_locale
            FROM pointages WHERE date_local=?
            ORDER BY heure_locale
        """, (date_str,)).fetchall()
    finally:
        conn.close()
    result = {}
    for r in rows:
        prno = r["prno"]
        if prno not in result:
            result[prno] = []
        result[prno].append({"type": r["type_pointage"], "heure": r["heure_locale"]})
    return result


def _ajouter_departs_nuit(pts_jour: list, getter, code: str, date_obj) -> list:
    """Pour une plage qui franchit minuit (nuit, ou garde continue à J+2), rattache
    le départ du matin de fin (J+1 ou J+2) à la garde commencée ce jour-là.
    `getter(date_iso) -> list[pts]` fournit les pointages d'une date."""
    if not code:
        return pts_jour
    from parser import get_plages_jour, _heure_to_min
    plages = get_plages_jour(code, date_obj)
    nuit = [p for p in plages if p.get("fin_offset", 0) >= 1 or p["fin"] < p["debut"]]
    if not nuit:
        return pts_jour
    maxoff = max(p.get("fin_offset", 0) or 1 for p in nuit)   # jour de fin
    seuil  = max(p["fin"] for p in nuit) + 120                # fin + marge
    d_fin  = (date_obj + timedelta(days=maxoff)).isoformat()
    extra  = [p for p in getter(d_fin)
              if p["type"] == "depart" and _heure_to_min(p["heure"]) <= seuil]
    return pts_jour + extra if extra else pts_jour


def _retirer_departs_veille(pts_jour: list, emp: dict, date_obj, feries) -> list:
    """Symétrique de `_ajouter_departs_nuit` : retire des pointages d'AUJOURD'HUI
    les départs du petit matin qui closent la garde de nuit de la VEILLE (déjà
    rattachés à la veille). Sinon ils sont matchés à tort à la garde du jour
    (« Fin de garde » fantôme le lendemain de la garde)."""
    if not _est_gardien(emp):
        return pts_jour
    veille = date_obj - timedelta(days=1)
    h = get_horaire(emp["prno"], veille.isoformat())
    code_v = h["code_horaire"] if h else None
    if not code_v:
        return pts_jour
    code_v_eff, _ = _planning_jour(emp, code_v, veille, feries)
    if not code_v_eff:
        return pts_jour
    from parser import get_plages_jour, _heure_to_min
    nuit_v = [p for p in get_plages_jour(code_v_eff, veille)
              if p.get("fin_offset", 0) >= 1 or p["fin"] < p["debut"]]
    if not nuit_v:
        return pts_jour
    seuil = max(p["fin"] for p in nuit_v) + 120   # fin de la nuit de la veille + marge
    return [p for p in pts_jour
            if not (p["type"] == "depart" and _heure_to_min(p["heure"]) <= seuil)]


def _augmenter_jour_integration(code: str, date_obj, pts: list, heure_effet: str) -> list:
    """Jour d'intégration : crédite comme présentes les plages (de jour) commencées
    avant l'heure d'effet, via des pointages synthétiques (arrivée au début, départ
    à l'heure d'effet). La fin de plage reste validée par les pointages réels."""
    if not code or not heure_effet:
        return pts
    from parser import get_plages_jour, _heure_to_min, _min_to_heure
    try:
        he = _heure_to_min(heure_effet)
    except Exception:
        return pts
    extra = []
    for p in get_plages_jour(code, date_obj):
        deb, fin, off = p["debut"], p["fin"], p.get("fin_offset", 0)
        if off == 0 and fin > deb and deb < he:          # plage de jour, arrivée déjà passée
            extra.append({"type": "arrivee", "heure": _min_to_heure(deb)})
            extra.append({"type": "depart",  "heure": _min_to_heure(min(he, fin))})
    return pts + extra if extra else pts


def get_resume_jour_avec_horaires(date_str: str) -> list[dict]:
    """
    Résumé journalier enrichi avec calcul plafonné par horaire individuel.
    - Statut basé sur l'horaire individuel (pas de règle fixe 13h)
    - Temps réel plafonné (pas de bonus avance, pénalité retard)
    """
    from parser import (calculer_temps_reel_plafonne, format_duree,
                        format_ecart, get_minutes_theoriques_jour)
    from datetime import date as date_cls, timedelta

    employes  = get_all_employes()
    pointages = get_pointages_bruts_jour(date_str)
    feries    = get_jours_feries_set()
    ferie     = date_str in feries
    date_obj  = date_cls.fromisoformat(date_str)
    # Départs des matins suivants (J+1, J+2) pour rattraper les gardes de nuit / 24h+
    pts_j1 = get_pointages_bruts_jour((date_obj + timedelta(days=1)).isoformat())
    pts_j2 = get_pointages_bruts_jour((date_obj + timedelta(days=2)).isoformat())
    _suiv = {(date_obj + timedelta(days=1)).isoformat(): pts_j1,
             (date_obj + timedelta(days=2)).isoformat(): pts_j2}

    result = []
    for emp in employes:
        prno    = emp["prno"]
        # Utiliser get_horaire(prno, date_str) pour prendre en compte les exceptions
        horaire = get_horaire(prno, date_str)
        code    = horaire["code_horaire"] if horaire else None
        date_effet_emp = horaire.get("date_effet") if horaire else None
        pts     = pointages.get(prno, [])

        # Avant la date d'effet → employé pas encore en poste, jour exclu
        if date_effet_emp and date_str < date_effet_emp:
            result.append({
                "prno": prno, "nom_prenom": emp["nom_complet"],
                "categorie": emp.get("categorie") or "standard",
                "code_horaire": code, "statut": "Exclu",
                "theorique_label": "—", "reel_label": "—",
                "ecart_label": "—", "ecart_type": "neutre",
                "arr_mat": None, "dep_mat": None,
                "arr_apm": None, "dep_apm": None,
                "minutes_reels": 0, "ferie": False,
            })
            continue

        # Planning du jour (gardiens : généré dynamiquement ; sinon code de base)
        is_sunday = date_obj.weekday() == 6
        effective_code, force_statut = _planning_jour(emp, code, date_obj, feries)

        # Garde : retirer le départ du matin qui clôt la garde de la VEILLE
        # (sinon « Fin » fantôme), puis rattacher celui de fin de garde du jour.
        pts = _retirer_departs_veille(pts, emp, date_obj, feries)
        pts = _ajouter_departs_nuit(
            pts, lambda d: _suiv.get(d, {}).get(prno, []), effective_code, date_obj)

        # Jour d'intégration : créditer les heures avant l'heure d'effet
        heure_effet_emp = horaire.get("heure_effet") if horaire else None
        if heure_effet_emp and date_effet_emp == date_str:
            pts = _augmenter_jour_integration(effective_code, date_obj, pts, heure_effet_emp)

        if effective_code and pts:
            calc = calculer_temps_reel_plafonne(effective_code, date_obj, pts)
            theorique      = calc["minutes_theoriques"]
            reel           = calc["minutes_reels"]
            complet        = calc["complet"]
            plages_detail  = calc["plages_detail"]
        elif effective_code:
            theorique     = get_minutes_theoriques_jour(effective_code, date_obj)
            reel          = 0
            complet       = False
            plages_detail = []
        else:
            theorique     = 0
            reel          = 0
            complet       = False
            plages_detail = []

        has_any = bool(pts)
        # Détecter si le jour est exclu par le code horaire
        jour_exclu = False
        if code:
            from parser import parse_code_horaire
            parsed_h = parse_code_horaire(effective_code or code)
            jour_info_check = parsed_h.get(date_obj.weekday(), {})
            jour_exclu = not jour_info_check.get("travaille", True)

        # Repos rotation (non-gardiens : ancienne logique modulo, hors dimanche/férié)
        is_repos_rotation = (not _est_gardien(emp) and not is_sunday and not ferie and
                             _est_jour_repos_rotation_from_emp(emp, date_str))

        if ferie and not _est_gardien(emp) and not _est_jardinier(emp):
            statut = "Férié"                   # standards : férié géré ici
        elif force_statut == "Férié":          # gardien/jardinier : férié chômé payé
            statut = "Férié"
            theorique = reel = (_duree_nuit_gardien(code) if _est_gardien(emp)
                                else get_minutes_theoriques_jour(code, date_obj))
        elif force_statut == "Garde":          # gardien : jour de tour (heures gardées)
            statut = "Garde"
        elif force_statut == "Récup":          # jardinier : récup du dimanche de tour
            statut = "Récup"
        elif force_statut == "Exclu":          # gardien : dimanche hors planning
            statut = "Exclu"
        elif force_statut == "Repos":          # gardien : repos calculé
            statut = "Repos"
        elif jour_exclu and not has_any:
            statut = "Exclu"
        elif is_repos_rotation and not has_any:
            statut = "Repos"
        elif not has_any:
            if theorique == 0 and code:
                statut = "Exclu"
            else:
                statut = "Absent"
        elif complet:
            statut = "Complet"
        else:
            statut = "Incomplet"

        ecart = format_ecart(theorique, reel)

        # Déterminer quelles sessions sont actives selon les plages de l'horaire
        # Une plage est "matin" si elle commence avant 13h00, "apm" si elle finit après 13h00
        # Une plage peut couvrir les deux (ex: 0800-1700)
        has_session_mat = False
        has_session_apm = False
        mat_plage_fin   = None   # heure de fin de la dernière plage matin (en minutes)
        apm_plage_debut = None   # heure de début de la première plage apm (en minutes)

        if code and plages_detail:
            for plage in plages_detail:
                pd_h = int(plage["plage_debut"].split(":")[0]) * 60 + int(plage["plage_debut"].split(":")[1])
                pf_h = int(plage["plage_fin"].split(":")[0])   * 60 + int(plage["plage_fin"].split(":")[1])
                # Plage matin : commence avant 13h
                if pd_h < 780:
                    has_session_mat = True
                    if mat_plage_fin is None or pf_h > mat_plage_fin:
                        mat_plage_fin = pf_h
                # Plage apm : finit après 13h
                if pf_h > 780:
                    has_session_apm = True
                    if apm_plage_debut is None or pd_h < apm_plage_debut:
                        apm_plage_debut = pd_h

        # Répartir les pointages selon les plages de l'horaire, avec la même
        # tolérance ±1h que la fenêtre d'admission (tracker.py / parser.est_dans_plage_horaire).
        # Si pas d'horaire : on utilise le seuil fixe 13h.
        TOLERANCE = 60  # minutes

        def appartient_matin(heure_str):
            h = int(heure_str.split(":")[0]) * 60 + int(heure_str.split(":")[1])
            if mat_plage_fin is not None:
                # Appartient au matin si avant la fin de la plage matin + tolérance
                return h <= mat_plage_fin + TOLERANCE
            # Fallback seuil fixe
            return h < 780

        def appartient_apm(heure_str):
            h = int(heure_str.split(":")[0]) * 60 + int(heure_str.split(":")[1])
            if apm_plage_debut is not None:
                # Appartient à l'apm si après le début de la plage apm - tolérance
                return h >= apm_plage_debut - TOLERANCE
            # Fallback seuil fixe
            return h >= 780

        # Un pointage peut appartenir aux deux sessions si la plage couvre tout
        # Priorité : si has_session_mat ET has_session_apm → on sépare selon le milieu
        # Sinon → tout va dans la session active
        arrivees_mat, departs_mat, arrivees_apm, departs_apm = [], [], [], []
        for p in pts:
            h_str = p["heure"][:5]  # HH:MM
            h_min = int(h_str.split(":")[0]) * 60 + int(h_str.split(":")[1])
            if has_session_mat and has_session_apm:
                # Les deux sessions : on coupe au milieu des deux plages
                milieu = (mat_plage_fin + apm_plage_debut) // 2 if mat_plage_fin and apm_plage_debut else 780
                if h_min <= milieu:
                    (arrivees_mat if p["type"]=="arrivee" else departs_mat).append(p["heure"])
                else:
                    (arrivees_apm if p["type"]=="arrivee" else departs_apm).append(p["heure"])
            elif has_session_mat:
                (arrivees_mat if p["type"]=="arrivee" else departs_mat).append(p["heure"])
            elif has_session_apm:
                (arrivees_apm if p["type"]=="arrivee" else departs_apm).append(p["heure"])
            else:
                # Pas d'horaire : seuil fixe 13h
                if h_min < 780:
                    (arrivees_mat if p["type"]=="arrivee" else departs_mat).append(p["heure"])
                else:
                    (arrivees_apm if p["type"]=="arrivee" else departs_apm).append(p["heure"])
                has_session_mat = bool(arrivees_mat or departs_mat)
                has_session_apm = bool(arrivees_apm or departs_apm)

        arrivees_mat.sort(); departs_mat.sort()
        arrivees_apm.sort(); departs_apm.sort()

        # Calculer dur_mat et dur_apm depuis les plages_detail
        dur_mat_min = None
        dur_apm_min = None
        if plages_detail:
            for plage in plages_detail:
                pd_h = int(plage["plage_debut"].split(":")[0]) * 60 + int(plage["plage_debut"].split(":")[1])
                if plage["minutes"] and plage["minutes"] > 0:
                    if pd_h < 780:  # plage matin
                        dur_mat_min = (dur_mat_min or 0) + plage["minutes"]
                    else:            # plage apm
                        dur_apm_min = (dur_apm_min or 0) + plage["minutes"]

        # Gardien : services prévus du jour (jour 08-17h et/ou nuit 16h45→08h15),
        # avec la prise/fin RÉELLES matchées à chaque plage par le calcul des
        # heures (plages_detail) — source fiable, contrairement au bucketing
        # matin/apm qui range mal les nuits. Gère aussi le cas « jour + nuit ».
        gardes_shifts = []
        if _est_gardien(emp) and effective_code:
            from parser import parse_code_horaire as _pch, _min_to_heure as _mth
            _pls = _pch(effective_code).get(date_obj.weekday(), {}).get("plages_travail", [])
            for _p in sorted(_pls, key=lambda x: x["debut"]):
                _overnight = _p.get("fin_offset", 0) >= 1 or _p["fin"] < _p["debut"]
                _deb = _mth(_p["debut"])
                _det = next((d for d in (plages_detail or [])
                             if d.get("plage_debut") == _deb), None)
                gardes_shifts.append({
                    "kind":   "nuit" if _overnight else "jour",
                    "debut":  _deb,
                    "fin":    _mth(_p["fin"]),
                    "prise":  _det.get("arrivee") if _det else None,
                    "fin_reelle": _det.get("depart") if _det else None,
                })

        result.append({
            "prno":              prno,
            "nom_prenom":        emp["nom_complet"],
            "categorie":         emp.get("categorie") or "standard",
            "date":              date_str,
            "arr_mat":           arrivees_mat[0] if arrivees_mat else None,
            "dep_mat":           departs_mat[-1] if departs_mat  else None,
            "arr_apm":           arrivees_apm[0] if arrivees_apm else None,
            "dep_apm":           departs_apm[-1] if departs_apm  else None,
            "dur_mat":           dur_mat_min,
            "dur_apm":           dur_apm_min,
            "dur_tot":           reel if reel > 0 else None,
            "gardes_shifts":     gardes_shifts,
            "statut":            statut,
            "ferie":             ferie,
            "code_horaire":      code,
            "has_session_mat":   has_session_mat,
            "has_session_apm":   has_session_apm,
            "minutes_theoriques": theorique,
            "minutes_reels":     reel,
            "theorique_label":   format_duree(theorique) if theorique else "—",
            "reel_label":        format_duree(reel) if reel else "—",
            "ecart_label":       ecart["label"],
            "ecart_type":        ecart["type"],
            "plages_detail":     plages_detail,
        })

    return sorted(result, key=lambda x: x["nom_prenom"])


def get_resume_periode_avec_synthese(date_debut: str, date_fin: str) -> list[dict]:
    """
    Vue périodique enrichie :
    - Grille colorée par jour (Complet/Incomplet/Absent/Férié)
    - Colonnes synthèse : Théorique | Réel | Écart | ✓ | ◑ | ✕ | 🎌
    """
    from parser import (calculer_temps_reel_plafonne, format_duree,
                        format_ecart, get_minutes_theoriques_jour)
    from datetime import date as date_cls, timedelta

    start  = date_cls.fromisoformat(date_debut)
    end    = date_cls.fromisoformat(date_fin)
    feries = get_jours_feries_set()

    # Générer toutes les dates (lundi-dimanche selon horaire)
    all_dates = []
    d = start
    while d <= end:
        all_dates.append(d.isoformat())
        d += timedelta(days=1)

    employes = get_all_employes()

    # Récupérer tous les pointages de la période en une requête
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT prno, date_local, type_pointage, heure_locale
            FROM pointages
            WHERE date_local BETWEEN ? AND ?
            ORDER BY date_local, heure_locale
        """, (date_debut, date_fin)).fetchall()
    finally:
        conn.close()

    # Indexer : pointages[prno][date] = [{"type", "heure"}, ...]
    from collections import defaultdict
    pts_index = defaultdict(lambda: defaultdict(list))
    for r in rows:
        pts_index[r["prno"]][r["date_local"]].append({
            "type": r["type_pointage"], "heure": r["heure_locale"]
        })

    result = []
    for emp in employes:
        prno    = emp["prno"]
        # Récupérer l'horaire de base (sans date) pour la date_effet de début de contrat
        horaire_base = get_horaire(prno)
        date_effet_emp = horaire_base["date_effet"] if horaire_base else None

        dates_statuts   = {}
        total_theorique = 0
        total_reel      = 0
        nb_complet      = 0
        nb_incomplet    = 0
        nb_absent       = 0
        nb_ferie        = 0
        nb_repos        = 0

        for ds in all_dates:
            # Avant la date d'effet → employé pas encore en poste
            if date_effet_emp and ds < date_effet_emp:
                dates_statuts[ds] = "Exclu"
                continue
            # Récupérer l'horaire pour ce jour précis (prend en compte les exceptions)
            horaire = get_horaire(prno, ds)
            code    = horaire["code_horaire"] if horaire else None

            date_obj = date_cls.fromisoformat(ds)
            pts      = pts_index[prno][ds]
            is_ferie = ds in feries

            is_sunday_day     = date_obj.weekday() == 6
            is_repos_rot      = (not _est_gardien(emp) and not is_sunday_day
                                 and not is_ferie
                                 and _est_jour_repos_rotation_from_emp(emp, ds))

            code_eff, force_statut = _planning_jour(emp, code, date_obj, feries)
            if code_eff:
                theo_jour = get_minutes_theoriques_jour(code_eff, date_obj)
                from parser import parse_code_horaire
                parsed    = parse_code_horaire(code_eff)
                jour_info = parsed.get(date_obj.weekday(), {})
                jour_exclu = not jour_info.get("travaille", True)
            else:
                theo_jour  = 0
                jour_exclu = False

            # Garde : retirer le départ du matin qui clôt la garde de la VEILLE,
            # puis rattacher celui de fin de garde du jour.
            pts = _retirer_departs_veille(pts, emp, date_obj, feries)
            pts = _ajouter_departs_nuit(
                pts, lambda d: pts_index[prno][d], code_eff, date_obj)

            # Jour d'intégration : créditer les heures avant l'heure d'effet
            heure_effet_emp = horaire.get("heure_effet") if horaire else None
            if heure_effet_emp and horaire.get("date_effet") == ds:
                pts = _augmenter_jour_integration(code_eff, date_obj, pts, heure_effet_emp)

            if is_ferie and not _est_gardien(emp) and not _est_jardinier(emp):
                statut = "Férié"                     # standards : férié payé
                nb_ferie += 1
                total_theorique += theo_jour
                total_reel      += theo_jour
            elif force_statut == "Garde":
                # Dimanche de tour : heures comptées si garde fraîche (code présent),
                # sinon (garde continue) les heures sont portées par le samedi.
                statut = "Garde"
                if code_eff and pts:
                    calc = calculer_temps_reel_plafonne(code_eff, date_obj, pts)
                    total_theorique += calc["minutes_theoriques"]
                    total_reel      += calc["minutes_reels"]
                    if calc["complet"]:
                        nb_complet += 1
                    else:
                        nb_incomplet += 1
                elif code_eff:
                    total_theorique += theo_jour
                    nb_absent       += 1
            elif force_statut == "Férié":
                statut = "Férié"
                nb_ferie += 1
                credit = (_duree_nuit_gardien(code) if _est_gardien(emp)
                          else get_minutes_theoriques_jour(code, date_obj))
                total_theorique += credit
                total_reel      += credit
            elif force_statut == "Récup":
                statut = "Récup"
                nb_repos += 1
            elif force_statut == "Exclu":
                statut = "Exclu"
            elif force_statut == "Repos":
                statut = "Repos"
                nb_repos += 1
            elif jour_exclu and not pts:
                statut = "Exclu"
            elif is_repos_rot and not pts:
                statut = "Repos"
                nb_repos += 1
            elif not pts:
                if theo_jour == 0:
                    statut = "Exclu"
                else:
                    statut = "Absent"
                    nb_absent += 1
                    total_theorique += theo_jour
            elif code_eff and pts:
                calc    = calculer_temps_reel_plafonne(code_eff, date_obj, pts)
                reel_j  = calc["minutes_reels"]
                complet = calc["complet"]
                total_theorique += calc["minutes_theoriques"]
                total_reel      += reel_j
                if complet:
                    statut = "Complet"
                    nb_complet += 1
                else:
                    statut = "Incomplet"
                    nb_incomplet += 1
            else:
                statut = "Absent"
                nb_absent += 1

            dates_statuts[ds] = statut

        ecart = format_ecart(total_theorique, total_reel)

        result.append({
            "prno":              prno,
            "nom_prenom":        emp["nom_complet"],
            "categorie":         emp.get("categorie") or "standard",
            "dates":             dates_statuts,
            "theorique_label":   format_duree(total_theorique),
            "reel_label":        format_duree(total_reel),
            "ecart_label":       ecart["label"],
            "ecart_type":        ecart["type"],
            "nb_complet":        nb_complet,
            "nb_incomplet":      nb_incomplet,
            "nb_absent":         nb_absent,
            "nb_ferie":          nb_ferie,
            "nb_repos":          nb_repos,
        })

    return sorted(result, key=lambda x: x["nom_prenom"])

# ─── RELEVÉS ──────────────────────────────────────────────────────────────────

def get_releves() -> list[dict]:
    """Retourne tous les relevés triés du plus récent au plus ancien."""
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT * FROM releves ORDER BY date_debut DESC"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_releve_actif() -> dict | None:
    """Retourne le relevé en cours (non clos dont la date_fin >= aujourd'hui)."""
    conn = get_connection()
    try:
        today = date.today().isoformat()
        row = conn.execute("""
            SELECT * FROM releves
            WHERE clos=0 AND date_debut <= ? AND date_fin >= ?
            ORDER BY date_debut DESC LIMIT 1
        """, (today, today)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_releve_by_id(releve_id: int) -> dict | None:
    conn = get_connection()
    try:
        row = conn.execute("SELECT * FROM releves WHERE id=?", (releve_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def creer_releve(date_debut: str, date_fin: str, libelle: str = None) -> dict:
    """Crée un nouveau relevé. Vérifie qu'il n'y a pas de chevauchement."""
    conn = get_connection()
    try:
        # Vérifier chevauchement
        overlap = conn.execute("""
            SELECT id FROM releves
            WHERE NOT (date_fin < ? OR date_debut > ?)
        """, (date_debut, date_fin)).fetchone()
        if overlap:
            return {"ok": False, "error": "Ce relevé chevauche un relevé existant."}
        cur = conn.execute(
            "INSERT INTO releves(date_debut, date_fin, libelle) VALUES(?,?,?)",
            (date_debut, date_fin, libelle)
        )
        conn.commit()
        new_id = cur.lastrowid
        return {"ok": True, "id": new_id, "date_debut": date_debut, "date_fin": date_fin}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


def clore_releve(releve_id: int, libelle_prochain: str = None) -> dict:
    """Clôture un relevé et crée automatiquement le suivant (date_debut = date_fin + 1 mois)."""
    import calendar
    conn = get_connection()
    try:
        row = conn.execute("SELECT * FROM releves WHERE id=?", (releve_id,)).fetchone()
        if not row:
            return {"ok": False, "error": "Relevé introuvable"}
        # Calculer le relevé suivant avant de commencer la transaction
        date_fin_actuel = date.fromisoformat(row["date_fin"])
        new_debut = date_fin_actuel + timedelta(days=1)
        duree = (date_fin_actuel - date.fromisoformat(row["date_debut"])).days
        new_fin = new_debut + timedelta(days=duree)
        # Clôture + création du suivant en une seule transaction atomique
        conn.execute("UPDATE releves SET clos=1 WHERE id=?", (releve_id,))
        cur = conn.execute(
            "INSERT INTO releves(date_debut, date_fin, libelle) VALUES(?,?,?)",
            (new_debut.isoformat(), new_fin.isoformat(), libelle_prochain or None)
        )
        conn.commit()
        return {"ok": True, "prochain_id": cur.lastrowid,
                "prochain_debut": new_debut.isoformat(),
                "prochain_fin": new_fin.isoformat()}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


def supprimer_releve(releve_id: int) -> dict:
    conn = get_connection()
    try:
        conn.execute("DELETE FROM releves WHERE id=?", (releve_id,))
        conn.commit()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


def close_db():
    """
    Force le checkpoint WAL : fusionne presences.db-wal dans presences.db
    avant l'arrêt du serveur, pour éviter toute perte de données.
    À appeler via atexit dans tracker.py.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.close()
        logger.info("WAL checkpoint effectué — base de données correctement fermée.")
    except Exception as e:
        logger.error("Erreur lors du checkpoint WAL à l'arrêt : %s", e)