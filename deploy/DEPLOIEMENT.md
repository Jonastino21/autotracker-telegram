# Déploiement sur le serveur Linux

## Installation (une fois)
```bash
sudo useradd -r -m -s /bin/bash karoka
sudo git clone https://github.com/Jonastino21/autotracker-telegram.git /opt/karoka/app
sudo chown -R karoka:karoka /opt/karoka
cd /opt/karoka/app
sudo -u karoka python3 -m venv venv
sudo -u karoka venv/bin/pip install -r requirements.txt
```
Copier depuis l'ancien poste : `.env` (avec `BRANCH_CREDENTIALS`) et `presences.db`, puis
`sudo chown karoka:karoka .env presences.db`.

## Démarrage automatique
```bash
sudo cp deploy/karoka.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now karoka
sudo systemctl status karoka       # état
journalctl -u karoka -f            # logs en direct
```
`Restart=always` relance l'appli après un plantage ; `enable` la lance à chaque démarrage du serveur.

## Mise à jour
```bash
cd /opt/karoka/app && sudo -u karoka git pull && sudo systemctl restart karoka
```

## Sauvegardes
```bash
sudo apt install sqlite3
sudo chmod +x /opt/karoka/app/deploy/backup.sh
( sudo crontab -l 2>/dev/null; echo "0 2 * * * /opt/karoka/app/deploy/backup.sh" ) | sudo crontab -
```
Copies dans `/var/backups/karoka` (30 jours). Copier régulièrement ce dossier sur une autre machine.

## Éviter les extinctions
- **Onduleur (UPS)** : protège des coupures de courant.
- **BIOS** : option « Restore on AC Power Loss » = *Power On*, pour que le serveur redémarre seul au retour du courant.
- Désactiver la mise en veille : `sudo systemctl mask sleep.target suspend.target hibernate.target`.
- **Une seule instance du bot** par token (sinon erreur Telegram 409) : arrêter l'ancienne copie.

## Ce qui est perdu pendant une panne
- Messages Telegram : rattrapés au redémarrage (Telegram les garde 24 h).
- Scans du boîtier ESP32 : perdus si le serveur est éteint (sauf mémoire tampon du boîtier). Régularisation possible via la page Maintenance.
