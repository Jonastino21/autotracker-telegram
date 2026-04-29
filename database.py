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
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
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
        """)
        conn.commit()
        _migrate_db(conn)
        _seed_jours_feries(conn)
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

    # ── Déduplication des pointages (doublons de type/session le même jour) ──
    # Garde uniquement le premier pointage (id MIN) par combinaison unique.
    try:
        conn.execute("""
            DELETE FROM pointages
            WHERE id NOT IN (
                SELECT MIN(id)
                FROM pointages
                GROUP BY prno, date_local, type_pointage, session
            )
        """)
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


def ajouter_employe(prno, nom_complet):
    prno = prno.strip().lower()
    nom_complet = nom_complet.strip()
    if not prno or not nom_complet:
        return {"ok": False, "error": "PRNO et nom obligatoires"}
    conn = get_connection()
    try:
        conn.execute("""
            INSERT INTO employes(prno, nom_complet, email)
            VALUES(?,?,?)
            ON CONFLICT(prno) DO UPDATE SET
                nom_complet=excluded.nom_complet,
                updated_at=datetime('now')
        """, (prno, nom_complet, f"{prno}.karoka@gmail.com"))
        conn.commit()
        return {"ok": True, "prno": prno, "nom_complet": nom_complet}
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


def insert_pointage(message_id, telegram_id, prno, date_local, heure_locale,
                    type_pointage, session, raw_text):
    conn = get_connection()
    try:
        cur = conn.execute("""
            INSERT OR IGNORE INTO pointages
                (message_id, telegram_id, prno, date_local, heure_locale,
                 type_pointage, session, raw_text)
            VALUES (?,?,?,?,?,?,?,?)
        """, (message_id, telegram_id, prno, date_local, heure_locale,
              type_pointage, session, raw_text))
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


def set_horaire(prno: str, code_horaire: str, date_effet: str, commentaire: str = None) -> dict:
    """
    Définit ou met à jour le code horaire d'un employé.
    Archive l'ancien dans historique_horaires.
    """
    prno = prno.strip().lower()
    conn = get_connection()
    try:
        # Archiver l'ancien horaire si existant
        ancien = conn.execute(
            "SELECT code_horaire, date_effet FROM horaires WHERE prno=?", (prno,)
        ).fetchone()
        if ancien:
            conn.execute("""
                INSERT INTO historique_horaires(prno, code_horaire, date_effet, date_fin)
                VALUES(?, ?, ?, ?)
            """, (prno, ancien["code_horaire"], ancien["date_effet"], date_effet))

        # Insérer ou mettre à jour
        conn.execute("""
            INSERT INTO horaires(prno, code_horaire, date_effet, commentaire)
            VALUES(?, ?, ?, ?)
            ON CONFLICT(prno) DO UPDATE SET
                code_horaire = excluded.code_horaire,
                date_effet   = excluded.date_effet,
                commentaire  = excluded.commentaire,
                updated_at   = datetime('now')
        """, (prno, code_horaire, date_effet, commentaire))
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
    Si date_str=None, retourne le code actuel.
    """
    prno = prno.strip().lower()
    conn = get_connection()
    try:
        if date_str:
            # Chercher dans l'historique le code actif à cette date
            row = conn.execute("""
                SELECT code_horaire, date_effet FROM historique_horaires
                WHERE prno=? AND date_effet <= ?
                ORDER BY date_effet DESC LIMIT 1
            """, (prno, date_str)).fetchone()
            # Comparer avec l'horaire actuel
            actuel = conn.execute(
                "SELECT code_horaire, date_effet FROM horaires WHERE prno=?", (prno,)
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


def get_resume_jour_avec_horaires(date_str: str) -> list[dict]:
    """
    Résumé journalier enrichi avec calcul plafonné par horaire individuel.
    - Statut basé sur l'horaire individuel (pas de règle fixe 13h)
    - Temps réel plafonné (pas de bonus avance, pénalité retard)
    """
    from parser import (calculer_temps_reel_plafonne, format_duree,
                        format_ecart, get_minutes_theoriques_jour)
    from datetime import date as date_cls

    employes  = get_all_employes()
    horaires  = get_all_horaires()
    pointages = get_pointages_bruts_jour(date_str)
    ferie     = is_jour_ferie(date_str)
    date_obj  = date_cls.fromisoformat(date_str)

    result = []
    for emp in employes:
        prno    = emp["prno"]
        horaire = horaires.get(prno)
        code    = horaire["code_horaire"] if horaire else None
        date_effet_emp = horaire["date_effet"] if horaire else None
        pts     = pointages.get(prno, [])

        # Avant la date d'effet → employé pas encore en poste, jour exclu
        if date_effet_emp and date_str < date_effet_emp:
            result.append({
                "prno": prno, "nom_prenom": emp["nom_complet"],
                "code_horaire": code, "statut": "Exclu",
                "theorique_label": "—", "reel_label": "—",
                "ecart_label": "—", "ecart_type": "neutre",
                "arr_mat": None, "dep_mat": None,
                "arr_apm": None, "dep_apm": None,
                "minutes_reels": 0, "ferie": False,
            })
            continue

        if code and pts:
            calc = calculer_temps_reel_plafonne(code, date_obj, pts)
            theorique      = calc["minutes_theoriques"]
            reel           = calc["minutes_reels"]
            complet        = calc["complet"]
            plages_detail  = calc["plages_detail"]
        elif code:
            theorique     = get_minutes_theoriques_jour(code, date_obj)
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
            parsed_h = parse_code_horaire(code)
            date_obj_check = date_cls.fromisoformat(date_str)
            jour_info_check = parsed_h.get(date_obj_check.weekday(), {})
            jour_exclu = not jour_info_check.get("travaille", True)

        if ferie:
            statut = "Férié"
        elif jour_exclu and not has_any:
            statut = "Exclu"
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
        # Une plage est "matin" si elle se termine avant 13h30, "apm" si elle commence après 12h30
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

        # Répartir les pointages selon les plages de l'horaire avec tolérance ±30 min
        # Si pas d'horaire : on utilise le seuil fixe 13h
        TOLERANCE = 30  # minutes

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

        result.append({
            "prno":              prno,
            "nom_prenom":        emp["nom_complet"],
            "date":              date_str,
            "arr_mat":           arrivees_mat[0] if arrivees_mat else None,
            "dep_mat":           departs_mat[-1] if departs_mat  else None,
            "arr_apm":           arrivees_apm[0] if arrivees_apm else None,
            "dep_apm":           departs_apm[-1] if departs_apm  else None,
            "dur_mat":           dur_mat_min,
            "dur_apm":           dur_apm_min,
            "dur_tot":           reel if reel > 0 else None,
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
    horaires = get_all_horaires()

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
        horaire = horaires.get(prno)
        code    = horaire["code_horaire"] if horaire else None
        date_effet_emp = horaire["date_effet"] if horaire else None  # date de début de contrat

        dates_statuts   = {}
        total_theorique = 0
        total_reel      = 0
        nb_complet      = 0
        nb_incomplet    = 0
        nb_absent       = 0
        nb_ferie        = 0

        for ds in all_dates:
            # Avant la date d'effet de l'horaire → jour exclu (employé pas encore en poste)
            if date_effet_emp and ds < date_effet_emp:
                dates_statuts[ds] = "Exclu"
                continue

            date_obj = date_cls.fromisoformat(ds)
            pts      = pts_index[prno][ds]
            is_ferie = ds in feries

            if code:
                theo_jour = get_minutes_theoriques_jour(code, date_obj)
                # Vérifier si ce jour est exclu dans le code horaire de l'employé
                from parser import parse_code_horaire
                parsed = parse_code_horaire(code)
                jour_semaine = date_obj.weekday()  # 0=lundi, 6=dimanche
                jour_info = parsed.get(jour_semaine, {})
                jour_exclu = not jour_info.get("travaille", True)
            else:
                theo_jour  = 0
                jour_exclu = False

            if is_ferie:
                statut = "Férié"
                nb_ferie += 1
                total_theorique += theo_jour  # Jour férié = payé
                total_reel      += theo_jour  # Compté comme accompli
            elif jour_exclu and not pts:
                # Jour exclu par l'horaire de l'employé → vide, pas absent
                statut = "Exclu"
            elif not pts:
                if theo_jour == 0:
                    # Pas de travail prévu ce jour (ex: dimanche sans horaire défini)
                    statut = "Exclu"
                else:
                    statut = "Absent"
                    nb_absent += 1
                    total_theorique += theo_jour
            elif code and pts:
                calc    = calculer_temps_reel_plafonne(code, date_obj, pts)
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
            "dates":             dates_statuts,
            "theorique_label":   format_duree(total_theorique),
            "reel_label":        format_duree(total_reel),
            "ecart_label":       ecart["label"],
            "ecart_type":        ecart["type"],
            "nb_complet":        nb_complet,
            "nb_incomplet":      nb_incomplet,
            "nb_absent":         nb_absent,
            "nb_ferie":          nb_ferie,
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
        conn.execute("UPDATE releves SET clos=1 WHERE id=?", (releve_id,))
        conn.commit()
        # Générer automatiquement le relevé suivant
        date_fin_actuel = date.fromisoformat(row["date_fin"])
        new_debut = (date_fin_actuel + timedelta(days=1))
        # Même durée que l'actuel
        duree = (date_fin_actuel - date.fromisoformat(row["date_debut"])).days
        new_fin = new_debut + timedelta(days=duree)
        # Créer le suivant avec le libellé fourni si précisé
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