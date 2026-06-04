"""
dashboard.py — Flask + SocketIO + Auth pour KAROKA.
"""
from flask_cors import CORS
import jwt as pyjwt
import datetime as _dt
import os, logging, threading
from datetime import date, timedelta, datetime
from functools import wraps
from zoneinfo import ZoneInfo

from flask import Flask, render_template, jsonify, request, send_file, redirect, url_for, session
from flask_socketio import SocketIO
from dotenv import load_dotenv
import database as db

load_dotenv()

logger        = logging.getLogger(__name__)
TIMEZONE      = os.getenv("TIMEZONE", "Indian/Antananarivo")
DASHBOARD_HOST= os.getenv("DASHBOARD_HOST", "0.0.0.0")
DASHBOARD_PORT= int(os.getenv("DASHBOARD_PORT", 5000))
EXCEL_PATH    = os.getenv("EXCEL_PATH", "presences.xlsx")
SECRET_KEY    = os.getenv("SECRET_KEY", "karoka-secret-key-change-me")

# Credentials depuis .env : "rh:pass1,pdg:pass2"
def _parse_credentials():
    raw = os.getenv("RH_CREDENTIALS", "rh:karoka2024,pdg:karoka2024")
    creds = {}
    for pair in raw.split(","):
        parts = pair.strip().split(":", 1)
        if len(parts) == 2:
            creds[parts[0].strip()] = parts[1].strip()
    return creds

CREDENTIALS = _parse_credentials()

app           = Flask(__name__)
CORS(app, origins="*", supports_credentials=False)
JWT_SECRET = os.getenv("JWT_SECRET", SECRET_KEY)
app.secret_key= SECRET_KEY
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading", logger=False, engineio_logger=False)

_bot      = None
_group_id = None


def set_bot(bot_instance, group_id):
    global _bot, _group_id
    _bot      = bot_instance
    _group_id = group_id
    _init_liaisons_empreintes()  


def emit_pointage(payload):
    """Appelé depuis tracker.py pour broadcaster un nouveau pointage."""
    try:
        socketio.emit("nouveau_pointage", payload)
    except Exception as e:
        logger.error("Erreur emit WebSocket : %s", e)


def emit_admin_update(event, payload=None):
    """Émet un événement admin (ajout/modif employé, horaire, etc.)."""
    try:
        socketio.emit(event, payload or {})
    except Exception as e:
        logger.error("Erreur emit admin WebSocket : %s", e)


def get_today():
    return datetime.now(ZoneInfo(TIMEZONE)).date().isoformat()


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        # Vérification JWT (frontend Netlify)
        auth = request.headers.get('Authorization', '')
        if auth.startswith('Bearer '):
            token = auth[7:]
            try:
                pyjwt.decode(token, JWT_SECRET, algorithms=["HS256"])
                return f(*args, **kwargs)
            except pyjwt.ExpiredSignatureError:
                return jsonify({"error": "Session expirée"}), 401
            except pyjwt.InvalidTokenError:
                return jsonify({"error": "Token invalide"}), 401

        # Vérification session Flask (accès direct local)
        if session.get("logged_in"):
            return f(*args, **kwargs)

        # Ni l'un ni l'autre
        if request.path.startswith('/api/'):
            return jsonify({"error": "Non autorisé"}), 401
        return redirect(url_for("login", next=request.url))
    return decorated


def _periode_dates(periode, ref_date=None):
    today = date.fromisoformat(ref_date) if ref_date else date.fromisoformat(get_today())
    if periode == "jour":
        return today.isoformat(), today.isoformat()
    if periode == "semaine":
        lundi = today - timedelta(days=today.weekday())
        return lundi.isoformat(), (lundi + timedelta(days=6)).isoformat()
    if periode == "s_mois":
        # Vue S-Mois : tout le mois calendaire (les semaines seront groupées côté JS)
        import calendar
        premier = today.replace(day=1)
        dernier = today.replace(day=calendar.monthrange(today.year, today.month)[1])
        return premier.isoformat(), dernier.isoformat()
    if periode == "mois":
        import calendar
        premier = today.replace(day=1)
        dernier = today.replace(day=calendar.monthrange(today.year, today.month)[1])
        return premier.isoformat(), dernier.isoformat()
    if periode == "releve":
        # Utiliser le relevé actif
        releve = db.get_releve_actif()
        if releve:
            return releve["date_debut"], releve["date_fin"]
        # Fallback : mois civil
        import calendar
        premier = today.replace(day=1)
        dernier = today.replace(day=calendar.monthrange(today.year, today.month)[1])
        return premier.isoformat(), dernier.isoformat()
    if periode == "trimestre":
        import calendar
        q = (today.month-1)//3
        premier  = today.replace(month=q*3+1, day=1)
        mois_fin = q*3+3
        dernier  = today.replace(month=mois_fin, day=calendar.monthrange(today.year, mois_fin)[1])
        return premier.isoformat(), dernier.isoformat()
    if periode == "semestre":
        if today.month <= 6:
            return today.replace(month=1,day=1).isoformat(), today.replace(month=6,day=30).isoformat()
        return today.replace(month=7,day=1).isoformat(), today.replace(month=12,day=31).isoformat()
    if periode == "annee":
        return today.replace(month=1,day=1).isoformat(), today.replace(month=12,day=31).isoformat()
    return today.isoformat(), today.isoformat()


# ─── Auth ─────────────────────────────────────────────────────────────────────

@app.route("/login", methods=["GET","POST"])
def login():
    error = None
    if request.method == "POST":
        u = request.form.get("username","").strip()
        p = request.form.get("password","").strip()
        if CREDENTIALS.get(u) == p:
            session["logged_in"] = True
            session["username"]  = u
            next_url = request.args.get("next") or url_for("index")
            return redirect(next_url)
        error = "Identifiants incorrects."
    return render_template("login.html", error=error)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

@app.route("/api/login", methods=["POST"])
def api_login():
    data = request.get_json(force=True) or {}
    u = data.get("username", "").strip()
    p = data.get("password", "").strip()
    if CREDENTIALS.get(u) == p:
        payload = {
            "sub": u,
            "role": u,
            "iat": _dt.datetime.utcnow(),
            "exp": _dt.datetime.utcnow() + _dt.timedelta(hours=12),
        }
        token = pyjwt.encode(payload, JWT_SECRET, algorithm="HS256")
        return jsonify({"token": token, "role": u})
    return jsonify({"error": "Identifiants incorrects."}), 401

# ─── Pages HTML ───────────────────────────────────────────────────────────────

@app.route("/")
@login_required
def index():
    date_param = request.args.get("date", get_today())
    return render_template("index.html", date_param=date_param, today=get_today(),
                           username=session.get("username",""))


@app.route("/admin")
@login_required
def admin():
    return render_template("admin.html", username=session.get("username",""))


@app.route("/help")
@login_required
def help_page():
    return render_template("help.html", username=session.get("username",""))


# ─── API Présences ────────────────────────────────────────────────────────────

@app.route("/api/stats")
@login_required
def api_stats():
    return jsonify(db.get_stats_jour(request.args.get("date", get_today())))


@app.route("/api/jour")
@login_required
def api_jour():
    return jsonify(db.get_resume_jour(request.args.get("date", get_today())))


@app.route("/api/periode")
@login_required
def api_periode():
    periode   = request.args.get("periode", "semaine")
    ref       = request.args.get("date", get_today())
    releve_id = request.args.get("releve_id")

    if releve_id:
        # Priorité au relevé sélectionné
        releve = db.get_releve_by_id(int(releve_id))
        if releve:
            d1, d2 = releve["date_debut"], releve["date_fin"]
        else:
            d1, d2 = _periode_dates(periode, ref)
    else:
        d1, d2 = _periode_dates(periode, ref)

    return jsonify({
        "date_debut": d1,
        "date_fin":   d2,
        "releve_id":  releve_id,
        "employes":   db.get_resume_periode_avec_synthese(d1, d2),
    })


@app.route("/api/historique")
@login_required
def api_historique():
    return jsonify(db.get_historique(
        limit     = int(request.args.get("limit", 200)),
        date_str  = request.args.get("date") or None,
        prno      = request.args.get("prno") or None,
        source    = request.args.get("source") or None,
        recherche = request.args.get("q") or None,
    ))


@app.route("/api/detail_jour")
@login_required
def api_detail_jour():
    ds = request.args.get("date", get_today())
    return jsonify({"date":ds,"stats":db.get_stats_jour(ds),"employes":db.get_resume_jour_avec_horaires(ds)})


@app.route("/api/download_excel")
@login_required
def api_download_excel():
    if os.path.exists(EXCEL_PATH):
        return send_file(EXCEL_PATH, as_attachment=True, download_name="presences_karoka.xlsx")
    return jsonify({"error":"Fichier Excel non trouvé"}), 404


@app.route("/api/health")
@login_required
def api_health():
    return jsonify({
        "status":"ok",
        "heure_locale":datetime.now(ZoneInfo(TIMEZONE)).strftime("%Y-%m-%d %H:%M:%S"),
        "employes":len(db.get_all_employes()),
        "lies":len(db.get_all_liaisons()),
    })


# ─── API Jours fériés ─────────────────────────────────────────────────────────

@app.route("/api/jours_feries", methods=["GET"])
@login_required
def api_jours_feries():
    annee = request.args.get("annee")
    return jsonify(db.get_jours_feries(annee))


@app.route("/api/jours_feries", methods=["POST"])
@login_required
def api_ajouter_ferie():
    data      = request.get_json()
    date_str  = (data.get("date_str") or "").strip()
    libelle   = (data.get("libelle") or "").strip()
    recurrent = bool(data.get("recurrent", False))
    type_ferie = (data.get("type_ferie") or "fixe").strip()
    if not date_str or not libelle:
        return jsonify({"ok":False,"error":"Date et libellé obligatoires"}), 400
    return jsonify(db.ajouter_jour_ferie(date_str, libelle, recurrent, type_ferie))


@app.route("/api/jours_feries/<date_str>", methods=["DELETE"])
@login_required
def api_supprimer_ferie(date_str):
    return jsonify(db.supprimer_jour_ferie(date_str))


# ─── API Admin Employés ───────────────────────────────────────────────────────

@app.route("/api/admin/employes", methods=["GET"])
@login_required
def api_employes():
    employes = db.get_all_employes(actifs_only=False)
    liaisons = {l["prno"]:l for l in db.get_all_liaisons()}
    result = []
    for emp in employes:
        l = liaisons.get(emp["prno"])
        result.append({**emp,"lie":l is not None,
            "dans_groupe":l["dans_groupe"] if l else False,
            "telegram_id":l["telegram_id"] if l else None,
            "username":l["username"] if l else None,
            "lie_le":l["lie_le"] if l else None})
    return jsonify(result)


@app.route("/api/admin/employes", methods=["POST"])
@login_required
def api_ajouter_employe():
    data           = request.get_json()
    prno           = (data.get("prno")        or "").strip()
    nom            = (data.get("nom_complet") or "").strip()
    categorie      = (data.get("categorie")   or "standard").strip()
    fingerprint_id = data.get("fingerprint_id")
    pin_id         = data.get("pin_id")

    result = db.ajouter_employe(prno, nom, categorie)
    if result["ok"]:
        if fingerprint_id:
            lier_empreinte(int(fingerprint_id), prno)
        if pin_id:
            lier_pin(pin_id, prno)
        emit_admin_update("admin_employes_updated")
    return jsonify(result), (200 if result["ok"] else 400)


@app.route("/api/admin/employes/<prno>", methods=["PUT"])
@login_required
def api_modifier_employe(prno):
    data         = request.get_json() or {}
    nouveau_prno = data.get("prno")
    nouveau_nom  = data.get("nom_complet")
    categorie    = data.get("categorie")
    result = db.modifier_employe(prno, nouveau_prno, nouveau_nom, categorie)
    if result["ok"]:
        emit_admin_update("admin_employes_updated")
    return jsonify(result), (200 if result["ok"] else 400)


@app.route("/api/admin/employes/<prno>", methods=["DELETE"])
@login_required
def api_desactiver_employe(prno):
    result = db.desactiver_employe(prno)
    if not result["ok"]:
        return jsonify(result), 400
    telegram_id = result.get("telegram_id")
    if telegram_id and _bot and _group_id:
        def _retirer():
            try:
                _bot.ban_chat_member(_group_id, telegram_id)
                import time; time.sleep(0.5)
                _bot.unban_chat_member(_group_id, telegram_id)
            except Exception as e:
                logger.error("Erreur retrait groupe : %s", e)
        threading.Thread(target=_retirer, daemon=True).start()
    emit_admin_update("admin_employes_updated")
    return jsonify({"ok":True,"message":f"Employé {prno} désactivé."})


@app.route("/api/admin/import_csv", methods=["POST"])
@login_required
def api_import_csv():
    if "file" not in request.files:
        return jsonify({"ok":False,"error":"Aucun fichier"}), 400
    f = request.files["file"]
    tmp = "tmp_import.csv"
    f.save(tmp)
    result = db.import_csv_employes(tmp)
    try: os.remove(tmp)
    except: pass
    return jsonify({"ok":True,**result})


@app.route("/api/admin/non_lies")
@login_required
def api_non_lies():
    return jsonify(db.get_employes_non_lies())


# ─── WebSocket ────────────────────────────────────────────────────────────────

@socketio.on("connect")
def on_connect():
    logger.debug("Client WebSocket connecté")


def run_dashboard():
    socketio.run(app, host=DASHBOARD_HOST, port=DASHBOARD_PORT,
                 debug=False, use_reloader=False, allow_unsafe_werkzeug=True)


if __name__ == "__main__":
    load_dotenv()
    db.init_db()
    _init_liaisons_empreintes()   # table empreintes
    run_dashboard()


# ─── API Horaires ─────────────────────────────────────────────────────────────

@app.route("/api/admin/horaires", methods=["GET"])
@login_required
def api_get_horaires():
    """Liste tous les horaires avec infos employé."""
    employes = db.get_all_employes()
    horaires = db.get_all_horaires()
    result = []
    for emp in employes:
        h = horaires.get(emp["prno"])
        result.append({
            "prno":        emp["prno"],
            "nom_complet": emp["nom_complet"],
            "code_horaire": h["code_horaire"] if h else None,
            "date_effet":   h["date_effet"]   if h else None,
        })
    return jsonify(result)


@app.route("/api/admin/horaires/<prno>", methods=["POST"])
@login_required
def api_set_horaire(prno):
    """Définit ou met à jour le code horaire d'un employé."""
    data         = request.get_json()
    code_horaire = (data.get("code_horaire") or "").strip()
    date_effet   = (data.get("date_effet")   or get_today()).strip()
    commentaire  = (data.get("commentaire")  or "").strip() or None
    heure_effet  = (data.get("heure_effet")  or "").strip() or None

    if not code_horaire:
        return jsonify({"ok": False, "error": "Code horaire obligatoire"}), 400

    from parser import valider_code_horaire
    validation = valider_code_horaire(code_horaire)
    if not validation["ok"]:
        return jsonify({"ok": False, "error": " | ".join(validation["erreurs"])}), 400

    result = db.set_horaire(prno, code_horaire, date_effet, commentaire, heure_effet)
    if result["ok"]:
        result["heures_semaine"] = validation["label_semaine"]
        emit_admin_update("admin_employes_updated")
    return jsonify(result), (200 if result["ok"] else 400)


@app.route("/api/admin/horaires/<prno>/historique", methods=["GET"])
@login_required
def api_historique_horaire(prno):
    return jsonify(db.get_historique_horaires(prno))


@app.route("/api/admin/horaires/<prno>/exceptions", methods=["GET"])
@login_required
def api_get_exceptions(prno):
    return jsonify(db.get_exceptions_horaires(prno))


@app.route("/api/admin/horaires/<prno>/exceptions", methods=["POST"])
@login_required
def api_set_exception(prno):
    data         = request.get_json()
    date_str     = (data.get("date_str")     or "").strip()
    code_horaire = (data.get("code_horaire") or "").strip()
    motif        = (data.get("motif")        or "").strip() or None
    if not date_str or not code_horaire:
        return jsonify({"ok": False, "error": "date_str et code_horaire obligatoires"}), 400
    from parser import valider_code_horaire
    validation = valider_code_horaire(code_horaire)
    if not validation["ok"]:
        return jsonify({"ok": False, "error": " | ".join(validation["erreurs"])}), 400
    result = db.set_exception_horaire(prno, date_str, code_horaire, motif)
    return jsonify(result), (200 if result["ok"] else 400)


@app.route("/api/admin/horaires/<prno>/exceptions/<date_str>", methods=["DELETE"])
@login_required
def api_supprimer_exception(prno, date_str):
    result = db.supprimer_exception_horaire(prno, date_str)
    return jsonify(result), (200 if result["ok"] else 400)


@app.route("/api/admin/employes/<prno>/permanent", methods=["DELETE"])
@login_required
def api_supprimer_employe_permanent(prno):
    result = db.supprimer_employe_permanent(prno)
    if result["ok"]:
        emit_admin_update("admin_employes_updated")
    return jsonify(result), (200 if result["ok"] else 400)


@app.route("/api/admin/liaisons/<prno>", methods=["DELETE"])
@login_required
def api_supprimer_liaison(prno):
    result = db.supprimer_liaison(prno)
    if result["ok"]:
        emit_admin_update("admin_employes_updated")
    return jsonify(result), (200 if result["ok"] else 400)


@app.route("/api/admin/employes/<prno>/rotation", methods=["PUT"])
@login_required
def api_set_rotation(prno):
    data              = request.get_json()
    rotation_cycle    = (data.get("rotation_cycle")    or "").strip() or None
    rotation_ref_date = (data.get("rotation_ref_date") or "").strip() or None
    dimanche_tour_ref = (data.get("dimanche_tour_ref") or "").strip() or None
    try:
        dimanche_tour_cycle = int(data.get("dimanche_tour_cycle")) if data.get("dimanche_tour_cycle") else None
    except (TypeError, ValueError):
        dimanche_tour_cycle = None
    try:
        ferie_tour_rang = int(data.get("ferie_tour_rang")) if data.get("ferie_tour_rang") else None
    except (TypeError, ValueError):
        ferie_tour_rang = None
    result = db.set_rotation(prno, rotation_cycle, rotation_ref_date,
                             dimanche_tour_ref, dimanche_tour_cycle, ferie_tour_rang)
    if result["ok"]:
        emit_admin_update("admin_employes_updated")
    return jsonify(result), (200 if result["ok"] else 400)


@app.route("/api/admin/employes/<prno>/reactiver", methods=["POST"])
@login_required
def api_reactiver_employe(prno):
    result = db.reactiver_employe(prno)
    return jsonify(result), (200 if result["ok"] else 400)


@app.route("/api/admin/horaires/<prno>", methods=["PUT"])
@login_required
def api_modifier_horaire(prno):
    """Modifie le code horaire d'un employé existant."""
    data         = request.get_json()
    code_horaire = (data.get("code_horaire") or "").strip()
    date_effet   = (data.get("date_effet")   or get_today()).strip()
    commentaire  = (data.get("commentaire")  or "").strip() or None
    heure_effet  = (data.get("heure_effet")  or "").strip() or None
    if not code_horaire:
        return jsonify({"ok": False, "error": "Code horaire obligatoire"}), 400
    from parser import valider_code_horaire
    validation = valider_code_horaire(code_horaire)
    if not validation["ok"]:
        return jsonify({"ok": False, "error": " | ".join(validation["erreurs"])}), 400
    result = db.set_horaire(prno, code_horaire, date_effet, commentaire, heure_effet)
    if result["ok"]:
        result["heures_semaine"] = validation["label_semaine"]
    return jsonify(result), (200 if result["ok"] else 400)


# ─── API Relevés ──────────────────────────────────────────────────────────────

@app.route("/api/releves", methods=["GET"])
@login_required
def api_get_releves():
    return jsonify(db.get_releves())


@app.route("/api/releves/actif", methods=["GET"])
@login_required
def api_releve_actif():
    r = db.get_releve_actif()
    return jsonify(r or {})


@app.route("/api/releves", methods=["POST"])
@login_required
def api_creer_releve():
    data = request.get_json()
    date_debut = (data.get("date_debut") or "").strip()
    date_fin   = (data.get("date_fin")   or "").strip()
    libelle    = (data.get("libelle")    or "").strip() or None
    if not date_debut or not date_fin:
        return jsonify({"ok": False, "error": "date_debut et date_fin obligatoires"}), 400
    if date_debut >= date_fin:
        return jsonify({"ok": False, "error": "date_debut doit être avant date_fin"}), 400
    result = db.creer_releve(date_debut, date_fin, libelle)
    return jsonify(result), (200 if result["ok"] else 400)


@app.route("/api/releves/<int:releve_id>/clore", methods=["POST"])
@login_required
def api_clore_releve(releve_id):
    data = request.get_json(silent=True) or {}
    libelle_prochain = data.get("libelle_prochain") or None
    result = db.clore_releve(releve_id, libelle_prochain=libelle_prochain)
    return jsonify(result), (200 if result["ok"] else 400)


@app.route("/api/releves/<int:releve_id>", methods=["DELETE"])
@login_required
def api_supprimer_releve(releve_id):
    result = db.supprimer_releve(releve_id)
    return jsonify(result), (200 if result["ok"] else 400)


@app.route("/api/admin/horaires/valider", methods=["POST"])
@login_required
def api_valider_horaire():
    """Valide un code horaire et retourne les heures calculées."""
    data = request.get_json()
    code = (data.get("code_horaire") or "").strip()
    if not code:
        return jsonify({"ok": False, "error": "Code vide"}), 400
    from parser import valider_code_horaire, parse_code_horaire, format_duree
    validation = valider_code_horaire(code)
    if not validation["ok"]:
        return jsonify(validation), 400
    # Détail par jour
    parsed = parse_code_horaire(code)
    detail = [{
        "jour":       j["nom"],
        "theorique":  format_duree(j["minutes_theoriques"]),
        "travaille":  j["travaille"],
    } for j in parsed.values()]
    return jsonify({**validation, "detail": detail})


@app.route("/api/jour_horaires")
@login_required
def api_jour_horaires():
    """Vue journalière enrichie avec heures théoriques et écarts."""
    date_str = request.args.get("date", get_today())
    return jsonify(db.get_resume_jour_avec_horaires(date_str))


@app.route("/api/admin/renvoi_lien/<prno>", methods=["POST"])
@login_required
def api_renvoi_lien(prno):
    """Renvoie le lien d'invitation Telegram depuis le dashboard admin."""
    liaison = db.get_liaison_by_prno(prno)
    if not liaison:
        return jsonify({"ok": False, "error": "Employé non lié"}), 400
    telegram_id = liaison["telegram_id"]
    employe = db.get_employe_by_prno(prno)
    nom = employe["nom_complet"] if employe else prno
    # Import différé pour éviter la circularité
    import importlib
    tracker_mod = importlib.import_module("tracker")
    ok = tracker_mod.renvoi_lien_groupe(telegram_id, nom)
    return jsonify({"ok": ok, "message": "Lien renvoyé" if ok else "Fallback — RH doit ajouter manuellement"})

# ─── EMPREINTES : table de liaison ───────────────────────────────────────────

def _init_liaisons_empreintes():
    """Crée la table liaisons_empreintes si elle n'existe pas + migration pin_id."""
    conn = db.get_connection()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS liaisons_empreintes (
                fingerprint_id  INTEGER UNIQUE,
                pin_id          TEXT UNIQUE,
                prno            TEXT NOT NULL,
                enregistre_le   TEXT DEFAULT (datetime('now')),
                FOREIGN KEY(prno) REFERENCES employes(prno)
            )
        """)
        # Migration : ajouter pin_id si table existante sans cette colonne
        try:
            conn.execute("ALTER TABLE liaisons_empreintes ADD COLUMN pin_id TEXT UNIQUE")
        except Exception:
            pass  # colonne déjà présente
        # Migration : si pin_id est encore en INTEGER (ancien schéma), reconstruire en
        # TEXT pour préserver les zéros de tête (ex. "061019" ≠ 61019)
        cols = {r[1]: (r[2] or "").upper() for r in
                conn.execute("PRAGMA table_info(liaisons_empreintes)")}
        if cols.get("pin_id") == "INTEGER":
            conn.execute("""
                CREATE TABLE liaisons_empreintes_new (
                    fingerprint_id  INTEGER UNIQUE,
                    pin_id          TEXT UNIQUE,
                    prno            TEXT NOT NULL,
                    enregistre_le   TEXT DEFAULT (datetime('now')),
                    FOREIGN KEY(prno) REFERENCES employes(prno)
                )
            """)
            conn.execute("""
                INSERT INTO liaisons_empreintes_new(fingerprint_id, pin_id, prno, enregistre_le)
                SELECT fingerprint_id,
                       CASE WHEN pin_id IS NULL THEN NULL ELSE CAST(pin_id AS TEXT) END,
                       prno, enregistre_le
                FROM liaisons_empreintes
            """)
            conn.execute("DROP TABLE liaisons_empreintes")
            conn.execute("ALTER TABLE liaisons_empreintes_new RENAME TO liaisons_empreintes")
        # Migration : ajouter index UNIQUE sur fingerprint_id pour les tables existantes
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_lem_fingerprint
            ON liaisons_empreintes(fingerprint_id)
            WHERE fingerprint_id IS NOT NULL
        """)
        conn.commit()
    finally:
        conn.close()


def get_liaison_empreinte(fingerprint_id: int):
    conn = db.get_connection()
    try:
        row = conn.execute("""
            SELECT le.fingerprint_id, le.prno, e.nom_complet
            FROM liaisons_empreintes le
            JOIN employes e ON le.prno = e.prno
            WHERE le.fingerprint_id = ?
        """, (fingerprint_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def lier_empreinte(fingerprint_id: int, prno: str):
    conn = db.get_connection()
    try:
        conn.execute("""
            INSERT INTO liaisons_empreintes(fingerprint_id, prno)
            VALUES(?, ?)
            ON CONFLICT(fingerprint_id) DO UPDATE SET
                prno = excluded.prno,
                enregistre_le = datetime('now')
        """, (fingerprint_id, prno.strip().lower()))
        conn.commit()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


def get_liaison_par_pin(pin_id):
    pin_id = str(pin_id).strip()          # PIN = chaîne (préserve les zéros de tête)
    conn = db.get_connection()
    try:
        row = conn.execute("""
            SELECT le.pin_id, le.fingerprint_id, le.prno, e.nom_complet
            FROM liaisons_empreintes le
            JOIN employes e ON le.prno = e.prno
            WHERE le.pin_id = ?
        """, (pin_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def lier_pin(pin_id, prno: str):
    pin_id = str(pin_id).strip()          # PIN = chaîne (préserve les zéros de tête)
    conn = db.get_connection()
    try:
        # Si l'employé a déjà une ligne → UPDATE pin_id
        # Sinon → INSERT nouvelle ligne
        existing = conn.execute(
            "SELECT fingerprint_id FROM liaisons_empreintes WHERE prno=?",
            (prno.strip().lower(),)
        ).fetchone()
        if existing:
            conn.execute("""
                UPDATE liaisons_empreintes SET pin_id=?, enregistre_le=datetime('now')
                WHERE prno=?
            """, (pin_id, prno.strip().lower()))
        else:
            conn.execute("""
                INSERT INTO liaisons_empreintes(pin_id, prno) VALUES(?, ?)
            """, (pin_id, prno.strip().lower()))
        conn.commit()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


# ─── WEBHOOK TUYA ─────────────────────────────────────────────────────────────

@app.route("/api/webhook/tuya", methods=["POST"])
def tuya_webhook():
    """
    Reçoit les événements du lock ESP32.
    Payload : {"id": "6", "type": "badge|fingerprint|pin", "timestamp": 1779512566}
    """
    import json
    from tracker import traiter_scan

    payload = request.get_json(silent=True)
    if not payload:
        return jsonify({"ok": False, "error": "payload invalide"}), 400

    logger.info("Webhook ESP32 reçu : %s", json.dumps(payload))

    scan_id    = payload.get("id")
    type_acces = payload.get("type", "badge")
    ts         = payload.get("timestamp")

    if not scan_id:
        return jsonify({"ok": False, "error": "id manquant"}), 400

    # PIN = chaîne (préserve les zéros de tête) ; empreinte/badge = entier
    if type_acces == "pin":
        scan_id = str(scan_id).strip()
    else:
        scan_id = int(scan_id)
    tz       = ZoneInfo(TIMEZONE)
    dt_local = datetime.fromtimestamp(int(ts), tz=tz) if ts else datetime.now(tz)

    # ── Résoudre prno selon le type ──
    if type_acces == "pin":
        liaison = get_liaison_par_pin(scan_id)
    else:
        liaison = get_liaison_empreinte(scan_id)

    if not liaison:
        logger.warning("ID #%s (type=%s) non lié à aucun employé", scan_id, type_acces)
        return jsonify({
            "ok": True,
            "results": [{"id": scan_id, "type": type_acces,
                         "ok": False, "error": "non enregistré dans le système"}]
        })

    prno        = liaison["prno"]
    nom_complet = liaison["nom_complet"]

    date_local    = dt_local.strftime("%Y-%m-%d")
    dernier_type  = db.get_dernier_pointage_type(prno, date_local)
    type_pointage = "depart" if dernier_type == "arrivee" else "arrivee"

    msg_id = f"esp_{type_acces}_{scan_id}_{int(dt_local.timestamp())}"
    result = traiter_scan(
        prno          = prno,
        nom_complet   = nom_complet,
        type_pointage = type_pointage,
        dt_local      = dt_local,
        msg_id        = msg_id,
        raw_text      = json.dumps(payload),
        source        = type_acces,
    )

    return jsonify({"ok": True, "results": [result]})


@app.route("/api/admin/empreintes", methods=["GET"])
@login_required
def api_get_empreintes():
    """Liste toutes les liaisons empreinte+pin ↔ employé."""
    conn = db.get_connection()
    try:
        rows = conn.execute("""
            SELECT le.fingerprint_id, le.pin_id, le.prno, e.nom_complet, le.enregistre_le
            FROM liaisons_empreintes le
            JOIN employes e ON le.prno = e.prno
            ORDER BY le.prno
        """).fetchall()
        return jsonify([dict(r) for r in rows])
    finally:
        conn.close()


@app.route("/api/admin/empreintes", methods=["POST"])
@login_required
def api_lier_empreinte():
    """Lie un fingerprint_id à un prno employé."""
    data           = request.get_json()
    fingerprint_id = data.get("fingerprint_id")
    prno           = (data.get("prno") or "").strip()
    if not fingerprint_id or not prno:
        return jsonify({"ok": False, "error": "fingerprint_id et prno obligatoires"}), 400
    result = lier_empreinte(int(fingerprint_id), prno)
    return jsonify(result), (200 if result["ok"] else 400)


@app.route("/api/admin/empreintes/<int:fingerprint_id>", methods=["DELETE"])
@login_required
def api_supprimer_empreinte(fingerprint_id):
    """Supprime la liaison d'une empreinte."""
    conn = db.get_connection()
    try:
        conn.execute("DELETE FROM liaisons_empreintes WHERE fingerprint_id=?", (fingerprint_id,))
        conn.commit()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400
    finally:
        conn.close()

@app.route("/api/admin/employes/<prno>/empreinte", methods=["PUT"])
@login_required
def api_lier_empreinte_employe(prno):
    """Lie ou met à jour l'empreinte d'un employé existant."""
    data           = request.get_json()
    fingerprint_id = data.get("fingerprint_id")
    if not fingerprint_id:
        return jsonify({"ok": False, "error": "fingerprint_id obligatoire"}), 400
    # S'assurer que l'employé a une ligne dans liaisons_empreintes
    conn = db.get_connection()
    try:
        conn.execute("""
            INSERT INTO liaisons_empreintes(fingerprint_id, prno)
            VALUES(?, ?)
            ON CONFLICT(fingerprint_id) DO UPDATE SET
                prno = excluded.prno,
                enregistre_le = datetime('now')
        """, (int(fingerprint_id), prno.strip().lower()))
        conn.commit()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400
    finally:
        conn.close()


@app.route("/api/admin/employes/<prno>/pin", methods=["PUT"])
@login_required
def api_lier_pin_employe(prno):
    """Lie ou met à jour le PIN d'un employé existant."""
    data   = request.get_json()
    pin_id = data.get("pin_id")
    if not pin_id:
        return jsonify({"ok": False, "error": "pin_id obligatoire"}), 400
    result = lier_pin(pin_id, prno)
    if result["ok"]:
        emit_admin_update("admin_employes_updated")
    return jsonify(result), (200 if result["ok"] else 400)


@app.route("/api/admin/employes/<prno>/pin", methods=["DELETE"])
@login_required
def api_supprimer_pin_employe(prno):
    """Supprime le PIN d'un employé."""
    conn = db.get_connection()
    try:
        conn.execute("UPDATE liaisons_empreintes SET pin_id=NULL WHERE prno=?",
                     (prno.strip().lower(),))
        conn.commit()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400
    finally:
        conn.close()