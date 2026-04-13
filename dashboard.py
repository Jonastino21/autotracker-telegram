"""
dashboard.py — Flask + SocketIO + Auth pour KAROKA.
"""

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
app.secret_key= SECRET_KEY
socketio      = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

_bot      = None
_group_id = None


def set_bot(bot_instance, group_id):
    global _bot, _group_id
    _bot      = bot_instance
    _group_id = group_id


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
        if not session.get("logged_in"):
            return redirect(url_for("login", next=request.url))
        return f(*args, **kwargs)
    return decorated


def _periode_dates(periode, ref_date=None):
    today = date.fromisoformat(ref_date) if ref_date else date.fromisoformat(get_today())
    if periode == "jour":
        return today.isoformat(), today.isoformat()
    if periode == "semaine":
        lundi = today - timedelta(days=today.weekday())
        return lundi.isoformat(), (lundi + timedelta(days=6)).isoformat()
    if periode == "mois":
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
    periode = request.args.get("periode","semaine")
    ref     = request.args.get("date", get_today())
    d1, d2  = _periode_dates(periode, ref)
    return jsonify({
        "date_debut": d1,
        "date_fin":   d2,
        "employes":   db.get_resume_periode_avec_synthese(d1, d2),
    })


@app.route("/api/detail_jour")
@login_required
def api_detail_jour():
    ds = request.args.get("date", get_today())
    return jsonify({"date":ds,"stats":db.get_stats_jour(ds),"employes":db.get_resume_jour(ds)})


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
    if not date_str or not libelle:
        return jsonify({"ok":False,"error":"Date et libellé obligatoires"}), 400
    return jsonify(db.ajouter_jour_ferie(date_str, libelle, recurrent))


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
    data = request.get_json()
    result = db.ajouter_employe((data.get("prno") or "").strip(), (data.get("nom_complet") or "").strip())
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

    if not code_horaire:
        return jsonify({"ok": False, "error": "Code horaire obligatoire"}), 400

    # Valider le code avant de sauvegarder
    from parser import valider_code_horaire
    validation = valider_code_horaire(code_horaire)
    if not validation["ok"]:
        return jsonify({"ok": False, "error": " | ".join(validation["erreurs"])}), 400

    result = db.set_horaire(prno, code_horaire, date_effet)
    if result["ok"]:
        result["heures_semaine"] = validation["label_semaine"]
        emit_admin_update("admin_employes_updated")
    return jsonify(result), (200 if result["ok"] else 400)


@app.route("/api/admin/horaires/<prno>/historique", methods=["GET"])
@login_required
def api_historique_horaire(prno):
    return jsonify(db.get_historique_horaires(prno))


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