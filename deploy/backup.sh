#!/bin/sh
# Sauvegarde quotidienne de la base (copie cohérente via sqlite3 .backup).
# Cron : 0 2 * * * /opt/karoka/deploy/backup.sh
set -e
APP=/opt/karoka
DEST=${BACKUP_DIR:-/var/backups/karoka}
mkdir -p "$DEST"
sqlite3 "$APP/presences.db" ".backup '$DEST/presences_$(date +%Y%m%d).db'"
# Garde 30 jours
find "$DEST" -name 'presences_*.db' -mtime +30 -delete
