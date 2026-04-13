import logging
import os
import sys
import threading
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import telebot
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv

import database as db
import detector
import exporter
import dashboard

load_dotenv()

BOT_TOKEN     = os.getenv("TELEGRAM_BOT_TOKEN", "")
GROUP_ID      = int(os.getenv("TELEGRAM_GROUP_ID", "0"))
TIMEZONE      = os.getenv("TIMEZONE", "Indian/Antananarivo")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SEC", "120"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.FileHandler("tracker.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("tracker")

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="Markdown")


def get_tz():
    return ZoneInfo(TIMEZONE)


def get_session(heure_locale):
    return "matin" if int(heure_locale.split(":")[0]) < 13 else "apm"


def ajouter_au_groupe(telegram_id, nom):
    """
    Envoie un lien d'invitation personnalisé au nouvel employé.
    Stratégie : essaie create_chat_invite_link, fallback sur lien fixe du groupe.
    """
    import time as _time
    for tentative in range(3):
        try:
            invite = bot.create_chat_invite_link(
                GROUP_ID,
                member_limit=1,
                name=f"Onboarding {nom}",
                expire_date=int(_time.time()) + 86400  # 24h
            )
            bot.send_message(
                telegram_id,
                f"Bonjour *{nom}* !\n\n"
                f"Voici votre lien personnel pour rejoindre le groupe de présence KAROKA :\n"
                f"{invite.invite_link}\n\n"
                f"Ce lien est à usage unique et expire dans 24h."
            )
            db.marquer_dans_groupe(telegram_id, True)
            logger.info("Lien invitation envoyé : telegram_id=%s (%s)", telegram_id, nom)
            return True
        except Exception as e:
            logger.warning("Tentative %d/3 — Erreur invitation : %s", tentative+1, e)
            if tentative < 2:
                _time.sleep(2)

    # Fallback : message au RH + notification à l'employé
    logger.error("Impossible de créer le lien d'invitation pour %s", telegram_id)
    bot.send_message(
        telegram_id,
        f"Bonjour *{nom}* ! Votre compte est activé.\n\n"
        f"Votre responsable RH va vous ajouter dans le groupe *Présence KAROKA* manuellement.\n"
        f"Une fois ajouté(e), pointez avec *Bonjour* et *Au revoir* chaque jour."
    )
    return False


def renvoi_lien_groupe(telegram_id: int, nom: str) -> bool:
    """Renvoie un lien d'invitation depuis le dashboard admin."""
    return ajouter_au_groupe(telegram_id, nom)


def retirer_du_groupe(telegram_id):
    try:
        bot.ban_chat_member(GROUP_ID, telegram_id)
        time.sleep(0.5)
        bot.unban_chat_member(GROUP_ID, telegram_id)
        logger.info("Employé retiré du groupe : %s", telegram_id)
        return True
    except Exception as e:
        logger.error("Erreur retrait groupe : %s", e)
        return False


def process_group_message(message):
    if message.chat.id != GROUP_ID:
        return
    if message.from_user and message.from_user.is_bot:
        return

    text = message.text or message.caption or ""
    if not text.strip():
        return

    type_pointage = detector.detect_type(text)
    if type_pointage is None:
        return

    telegram_id = message.from_user.id
    liaison     = db.get_liaison(telegram_id)

    if not liaison:
        try:
            bot.send_message(
                telegram_id,
                "Compte non activé.\nEnvoyez votre *PRNO* en message privé pour l'activer."
            )
        except Exception:
            pass
        return

    prno         = liaison["prno"]
    nom_complet  = liaison["nom_complet"]
    msg_dt_local = datetime.fromtimestamp(message.date, tz=ZoneInfo("UTC")).astimezone(get_tz())
    date_local   = msg_dt_local.strftime("%Y-%m-%d")
    heure_locale = msg_dt_local.strftime("%H:%M:%S")
    session      = get_session(heure_locale)

    inserted = db.insert_pointage(
        message_id=message.message_id, telegram_id=telegram_id, prno=prno,
        date_local=date_local, heure_locale=heure_locale,
        type_pointage=type_pointage, session=session, raw_text=text[:500],
    )

    if inserted:
        type_label    = "Arrivée" if type_pointage == "arrivee" else "Départ"
        session_label = "matin" if session == "matin" else "après-midi"
        logger.info("%s — %s [%s] %s à %s", prno, type_label, session_label, date_local, heure_locale)

        # Émettre l'événement WebSocket temps réel
        payload = {
            "prno":          prno,
            "nom_complet":   nom_complet,
            "type_pointage": type_pointage,
            "type_label":    type_label,
            "session":       session,
            "session_label": session_label,
            "heure":         heure_locale[:5],
            "date":          date_local,
        }
        dashboard.emit_pointage(payload)

        threading.Thread(target=_regenerer_excel, args=(date_local,), daemon=True).start()


def process_private_message(message):
    if message.chat.type != "private":
        return
    if message.from_user and message.from_user.is_bot:
        return

    text        = (message.text or "").strip()
    telegram_id = message.from_user.id
    username    = message.from_user.username or ""
    prenom      = message.from_user.first_name or ""

    if not text:
        return

    if text.lower() in ["/start", "start"]:
        liaison = db.get_liaison(telegram_id)
        if liaison:
            bot.send_message(telegram_id,
                f"Déjà enregistré(e) : *{liaison['nom_complet']}*\nPRNO : `{liaison['prno']}`\n\n"
                f"Pointez avec *Bonjour* et *Au revoir* dans le groupe.")
        else:
            bot.send_message(telegram_id,
                f"Bienvenue sur le système de présence *KAROKA*{', '+prenom if prenom else ''} !\n\n"
                f"Envoyez votre *PRNO* (code fourni par le RH) pour activer votre compte.")
        return

    resultat = db.creer_liaison(telegram_id, text, username)
    bot.send_message(telegram_id, resultat["message"])

    if resultat.get("ok") and resultat.get("code") == "LIAISON_CREEE":
        employe = resultat["employe"]
        logger.info("Onboarding réussi : %s ↔ %s", telegram_id, text)
        threading.Thread(target=ajouter_au_groupe, args=(telegram_id, employe["nom_complet"]), daemon=True).start()


def process_member_update(message):
    if not message or message.chat.id != GROUP_ID:
        return
    left = message.left_chat_member
    if left and not left.is_bot:
        prno = db.retirer_liaison(left.id)
        if prno:
            logger.info("Sync : %s (prno=%s) retiré → désactivé", left.id, prno)


def _regenerer_excel(date_local):
    try:
        exporter.generate_excel(date_local)
    except Exception as e:
        logger.error("Erreur Excel : %s", e)


def polling_loop():
    last_update_id = int(db.get_meta("last_update_id", "0"))
    logger.info("Polling démarré (last_update_id=%s)...", last_update_id)
    while True:
        try:
            offset  = last_update_id + 1 if last_update_id > 0 else None
            updates = bot.get_updates(offset=offset, timeout=20, allowed_updates=["message", "chat_member"])
            for update in updates:
                msg = update.message
                if msg:
                    if msg.chat.type == "private":
                        process_private_message(msg)
                    elif msg.chat.type in ["group", "supergroup"]:
                        if msg.left_chat_member:
                            process_member_update(msg)
                        else:
                            process_group_message(msg)
                last_update_id = update.update_id
                db.set_meta("last_update_id", str(last_update_id))
        except telebot.apihelper.ApiTelegramException as e:
            logger.warning("Telegram API : %s", e)
            time.sleep(5)
        except (ConnectionResetError, ConnectionAbortedError) as e:
            # Connexion fermée par Telegram — normal en long polling
            logger.debug("Connexion réinitialisée par Telegram (normal)")
            time.sleep(2)
        except Exception as e:
            err_str = str(e)
            if "10054" in err_str or "Connection aborted" in err_str or "ConnectionReset" in err_str:
                logger.debug("Connexion réinitialisée par Telegram (normal)")
                time.sleep(2)
            else:
                logger.error("Erreur polling : %s", e)
                time.sleep(5)
        # Pas de sleep ici — long polling quasi temps réel


def main():
    logger.info("═══════════════════════════════════════")
    logger.info("  KAROKA Attendance Tracker — Démarrage")
    logger.info("═══════════════════════════════════════")

    if not BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN manquant !")
        sys.exit(1)
    if not GROUP_ID:
        logger.error("TELEGRAM_GROUP_ID manquant !")
        sys.exit(1)

    db.init_db()
    db.init_horaires_table()

    # Injecter le bot dans le dashboard pour le retrait de groupe
    dashboard.set_bot(bot, GROUP_ID)

    try:
        exporter.generate_excel()
    except Exception as e:
        logger.warning("Excel initial : %s", e)

    scheduler = BackgroundScheduler(timezone=TIMEZONE)
    scheduler.add_job(lambda: exporter.generate_excel(), "interval", hours=1, id="excel_backup")
    scheduler.start()

    # Dashboard Flask + SocketIO dans un thread séparé
    flask_thread = threading.Thread(target=dashboard.run_dashboard, daemon=True, name="flask")
    flask_thread.start()
    logger.info("Dashboard : http://%s:%s", os.getenv("DASHBOARD_HOST","0.0.0.0"), os.getenv("DASHBOARD_PORT","5000"))

    non_lies = db.get_employes_non_lies()
    if non_lies:
        logger.warning("%d employé(s) sans liaison : %s", len(non_lies), ", ".join(e["prno"] for e in non_lies))

    try:
        polling_loop()
    except KeyboardInterrupt:
        scheduler.shutdown()
        sys.exit(0)


if __name__ == "__main__":
    main()