# 🏢 KAROKA — Système de Suivi des Présences via Telegram

Système complet de suivi de présence du personnel pour l'entreprise **KAROKA** (Madagascar, UTC+3).  
Les employés pointent via un groupe Telegram en écrivant "Bonjour" (arrivée) et "Au revoir" (départ).

---

## 📁 Structure des fichiers

```
karoka_tracker/
├── tracker.py          # Pipeline principal (Telegram → SQLite → Excel → Flask)
├── detector.py         # Détection mots-clés avec rapidfuzz (tolérance 85%)
├── database.py         # Toutes les opérations SQLite
├── exporter.py         # Génération Excel 6 onglets
├── dashboard.py        # Serveur Flask — Dashboard RH/PDG
├── templates/
│   └── index.html      # Interface dashboard complète
├── requirements.txt    # Dépendances Python
├── .env.example        # Template de configuration
├── start_tracker.bat   # Script lancement Windows
├── run_hidden.vbs      # Lanceur silencieux (sans terminal)
└── README.md
```

---

## ⚙️ Installation

### 1. Prérequis

- Python 3.11+ (inclut `zoneinfo`)
- Windows 10/11 (pour le démarrage automatique)
- Un bot Telegram créé via [@BotFather](https://t.me/BotFather)
- Le bot ajouté comme **administrateur** du groupe Telegram

### 2. Créer le bot Telegram

1. Ouvrir Telegram → chercher **@BotFather**
2. Envoyer `/newbot` → suivre les instructions
3. Copier le **token** fourni
4. Ajouter le bot à votre groupe Telegram
5. Donner les droits **Administrateur** au bot (pour lire les messages)

### 3. Obtenir l'ID du groupe

**Option A — @userinfobot :**
1. Ajouter [@userinfobot](https://t.me/userinfobot) au groupe
2. Il envoie l'ID automatiquement (nombre négatif, ex: `-1001234567890`)
3. Retirer ensuite userinfobot du groupe

**Option B — Via l'API :**
```
https://api.telegram.org/bot<VOTRE_TOKEN>/getUpdates
```
Envoyer un message dans le groupe puis chercher `"chat":{"id":...}`.

### 4. Cloner et configurer

```bash
# Cloner / copier le dossier karoka_tracker
cd karoka_tracker

# Créer l'environnement virtuel
python -m venv venv
venv\Scripts\activate  # Windows

# Installer les dépendances
pip install -r requirements.txt

# Copier et configurer .env
copy .env.example .env
```

Éditer `.env` :
```env
TELEGRAM_BOT_TOKEN=1234567890:ABCdefGHIjklMNOpqrSTUvwxYZ
TELEGRAM_GROUP_ID=-1001234567890
TIMEZONE=Indian/Antananarivo
DASHBOARD_URL=http://192.168.137.1:5000
DASHBOARD_HOST=0.0.0.0
DASHBOARD_PORT=5000
SHARED_FOLDER=                    # Optionnel : chemin réseau ou Google Drive
```

### 5. Premier lancement (test)

```bash
python tracker.py
```

Le dashboard est accessible sur `http://192.168.137.1:5000`.

---

## 🚀 Démarrage automatique Windows

Pour que le système démarre automatiquement sans fenêtre visible :

1. Appuyer sur `Win + R` → taper `shell:startup` → OK
2. Copier `run_hidden.vbs` dans ce dossier
3. Le tracker démarrera automatiquement à chaque connexion Windows

**Vérification :** Ouvrir le Gestionnaire des tâches → `python.exe` doit être visible.

---

## 📊 Fonctionnement

### Détection des pointages

| Message | Détecté comme |
|---------|---------------|
| `Bonjour` | Arrivée |
| `bjr`, `bonjours`, `bon jour` | Arrivée |
| `Au revoir` | Départ |
| `aurevoir`, `au rev`, `bonne journée` | Départ |
| Tolérance : 85% de similarité (rapidfuzz) |

### Sessions
- **Avant 13h00** → Session Matin
- **Après 13h00** → Session Après-midi (APM)

### Règle anti-doublons
- 1er "Bonjour" par session = Arrivée
- Dernier "Au revoir" par session = Départ
- `message_id` Telegram unique → `INSERT OR IGNORE`

### PRNO (Code employé)
- `@aldu` → PRNO = `aldu`
- Sans username → ID Telegram utilisé

---

## 🖥️ Dashboard Web

URL : `http://192.168.137.1:5000`

**6 onglets :**
- **Jour** — Tableau complet (PRNO, Nom, 4 pointages, durées, statut)
- **Semaine / Mois / Trimestre / Semestre / Année** — Vue grille colorée

**Cartes statistiques :**
- Total | Complets (4 pointages) | Incomplets (partiel) | Absents

**Fonctionnalités :**
- Sélecteur de date en haut à droite
- Recherche/filtre par nom ou PRNO
- Actualisation automatique toutes les 2 minutes
- Clic sur une date → modal avec détail complet
- Paramètre `?date=YYYY-MM-DD` → ouverture automatique du modal
- Téléchargement Excel via bouton ⬇

---

## 📂 Fichier Excel

Généré automatiquement à chaque pointage et toutes les heures.

**6 onglets :**
1. `Jour` — Vue tabulaire complète
2. `Semaine` — Grille colorée semaine courante
3. `Mois` — Grille colorée mois courant
4. `Trimestre` — Grille colorée trimestre courant
5. `Semestre` — Grille colorée semestre courant
6. `Année` — Grille colorée année courante

**Codes couleur :**
- 🟢 **Vert** — Complet (4 pointages)
- 🟠 **Orange** — Incomplet (partiel)
- 🔴 **Rouge** — Absent

Les cellules de dates sont des hyperliens vers `http://192.168.137.1:5000/?date=YYYY-MM-DD`.

### Sauvegarde partagée
Définir `SHARED_FOLDER` dans `.env` :
```env
# Dossier réseau
SHARED_FOLDER=\\192.168.1.10\RH\Presences

# Google Drive (avec Drive For Desktop installé)
SHARED_FOLDER=G:\Mon Drive\KAROKA\Presences
```

---

## 🔌 API REST

| Route | Description |
|-------|-------------|
| `GET /api/stats?date=YYYY-MM-DD` | Statistiques du jour (4 cartes) |
| `GET /api/jour?date=YYYY-MM-DD` | Résumé journalier complet |
| `GET /api/periode?periode=semaine&date=...` | Vue périodique (semaine/mois/...) |
| `GET /api/detail_jour?date=YYYY-MM-DD` | Détail pour modal |
| `GET /api/membres` | Liste des membres |
| `GET /api/download_excel` | Téléchargement Excel |
| `GET /api/health` | Statut du système |

---

## 🛠️ Maintenance

### Logs
Le fichier `tracker.log` contient tous les événements. Pour le consulter :
```bash
type tracker.log
# ou en temps réel :
Get-Content tracker.log -Wait  # PowerShell
```

### Ajouter des membres manuellement
Les membres sont synchronisés automatiquement depuis le groupe Telegram.  
En cas de besoin, modifier directement la table `membres` dans `presences.db`  
avec un outil comme [DB Browser for SQLite](https://sqlitebrowser.org/).

### Réinitialiser la base
```bash
del presences.db
python tracker.py  # La DB sera recréée
```

### Tester la détection
```bash
python detector.py
```

---

## 🌐 Réseau local

Le dashboard est accessible par **toutes les machines du réseau local** via :
```
http://192.168.137.1:5000
```

Vérifier que le **pare-feu Windows** autorise le port 5000 :
```
Panneau de configuration → Pare-feu Windows → Règles de trafic entrant
→ Nouvelle règle → Port → TCP 5000 → Autoriser
```

---

## 📦 Dépendances

| Package | Usage |
|---------|-------|
| `pyTelegramBotAPI` | Lecture des messages Telegram |
| `rapidfuzz` | Détection floue des mots-clés (seuil 85%) |
| `sqlite3` | Stockage local (stdlib Python) |
| `openpyxl` | Génération Excel avec mise en forme |
| `flask` | Dashboard web |
| `apscheduler` | Polling et sync membres planifiés |
| `python-dotenv` | Lecture du fichier .env |
| `zoneinfo` | Gestion timezone (stdlib Python 3.9+) |

---

## 🆘 Dépannage

**Le bot ne reçoit pas les messages :**
- Vérifier que le bot est bien admin du groupe
- Vérifier `TELEGRAM_GROUP_ID` (doit être négatif)
- Consulter `tracker.log`

**Dashboard inaccessible depuis le réseau :**
- Vérifier `DASHBOARD_HOST=0.0.0.0` dans `.env`
- Ouvrir le port 5000 dans le pare-feu Windows

**Excel non généré :**
- Vérifier les permissions d'écriture dans le dossier
- Vérifier `EXCEL_PATH` dans `.env`

---

*Développé pour KAROKA — Madagascar — Fuseau horaire Indian/Antananarivo (UTC+3)*
