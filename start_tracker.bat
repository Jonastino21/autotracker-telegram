@echo off
REM ═══════════════════════════════════════════════════════════
REM   KAROKA Attendance Tracker — Lancement Windows
REM   Ce fichier est appelé par run_hidden.vbs (sans fenêtre)
REM ═══════════════════════════════════════════════════════════

REM Aller dans le dossier du script
cd /d "%~dp0"

REM Activer l'environnement virtuel si présent
if exist "venv\Scripts\activate.bat" (
    call venv\Scripts\activate.bat
)

REM Lancer le tracker
python tracker.py >> tracker.log 2>&1
