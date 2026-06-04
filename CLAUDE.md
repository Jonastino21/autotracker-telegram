# CLAUDE.md

Guide pour travailler sur **KAROKA Présences** — suivi de présence (pointages) via Telegram + boîtier ESP32 (empreinte / badge / PIN), avec dashboard web et export Excel.

## Lancer / développer

- **Tout démarrer** : `python tracker.py` — c'est le point d'entrée. Il lance le dashboard Flask dans un thread (`dashboard.run_dashboard`) **et** la boucle de polling Telegram (`polling_loop`).
- **Dashboard seul** : `python dashboard.py` (Flask + SocketIO sur `DASHBOARD_PORT`, défaut 5000).
- **venv** : `venv/` est présent. Sous Windows : `start_tracker.bat` (ou `run_hidden.vbs` au démarrage).
- **Config** : variables dans `.env` (voir `env.example`) — `TELEGRAM_BOT_TOKEN`, `TELEGRAM_GROUP_ID`, `TIMEZONE` (défaut `Indian/Antananarivo`), `DB_PATH` (défaut `presences.db`), `RH_CREDENTIALS` (`user:pass,user2:pass2`), `EXCEL_PATH`, etc.

### Migrations base de données
Les migrations sont **idempotentes** et tournent **au démarrage** : `db.init_db()` (colonnes via `ALTER TABLE` en try/except) et `dashboard._init_liaisons_empreintes()` (table empreintes/PIN + reconstruction si besoin). Pour les appliquer sans redémarrer :
```
python -c "import database; database.init_db()"
python -c "import dashboard; dashboard._init_liaisons_empreintes()"
```
Faire ça appli **arrêtée** (ou redémarrer l'appli) de préférence.

## Architecture

| Fichier | Rôle |
|---|---|
| `tracker.py` | **Entrée**. Bot Telegram (onboarding, écoute du groupe, mots-clés présence) + lance le dashboard. `traiter_scan()` = logique commune d'ingestion d'un pointage (Telegram **et** webhook ESP32). |
| `dashboard.py` | Flask + SocketIO + auth (session locale **ou** JWT). API REST (présences, employés, horaires, rotation, fériés), **webhook ESP32** (`/api/...` PIN/empreinte/badge), gestion `liaisons_empreintes` (empreinte/PIN). |
| `database.py` | SQLite + **tout le moteur de planning** (voir ci-dessous). Statuts journaliers/période, rotation, catégories. |
| `parser.py` | Parsing du **code horaire** + calcul du temps réel plafonné. |
| `detector.py` | Détection floue (rapidfuzz) des mots-clés présence Telegram (« bonjour », « au revoir »…). |
| `exporter.py` | Génération Excel (6 onglets) avec liens dashboard. |
| `templates/` | `index.html` (dashboard présences jour/semaine/mois), `admin.html` (employés/horaires/rotation), `help.html`, `login.html`. Tailwind via CDN. |

Temps réel : `dashboard.emit_pointage` / `emit_admin_update` (SocketIO) ; le front se rafraîchit.

## Code horaire (`parser.py`)

Format d'un segment : `HHMMjjHHMM` où `jj` ∈ `lu ma me je ve sa di tj`. Séparateur `;`. Parenthèses = pause **ou** exclusion.
- `0800tj1200;1400tj1700;(di)` — journée standard, dimanche exclu.
- `1645tj0815` — nuit (chevauche minuit, détecté car fin < début).
- Suffixe **`+`** = fin le lendemain (J+1), **`++`** = fin à J+2. Ex. garde `0800di0815+` (dim 8h → lun 8h15 ≈ 24h15), `1645sa0815++` (sam 16h45 → lun 8h15 ≈ 39h30).
- Représentation interne d'une plage : `{debut, fin, fin_offset, minutes_net}` (`fin_offset` = nb de minuits franchis). Le calcul du réel (`calculer_temps_reel_plafonne`) raisonne sur un **axe horaire continu** (`_sur_axe`) et **plafonne** (pas de bonus avance ; franchise retard `GRACE_RETARD_MIN`=15 ; départ anticipé décompté).
- `est_dans_plage_horaire` = fenêtre d'admission ±1h (gère le chevauchement de minuit) — utilisée par le tracker pour accepter/refuser un pointage.

## Moteur de planning — 3 catégories d'employés (`database.py`)

La **catégorie** (`employes.categorie` : `standard` / `gardien` / `jardinier`) pilote la logique. `_planning_jour(emp, code, date_obj, feries)` renvoie `(code_effectif, force_statut)` ; `force_statut` ∈ `{None, Repos, Garde, Récup, Férié, Exclu}`.

### Standard
Suit son code horaire. Férié = payé (théo crédité).

### Gardien (`_planning_gardien`)
- Cycle **2 nuits / 1 repos** (`rotation_cycle` ex. `2/3`, `rotation_ref_date`). Position = `(jours_écoulés − jours_à_part) mod cycle`.
- **Jour « à part »** = dimanche **ou** férié en semaine : ne fait **jamais** avancer le cycle. Un férié tombant un dimanche est ignoré (c'est un dimanche).
- **Tour du dimanche** : rotation hebdo (`dimanche_tour_ref` + `dimanche_tour_cycle` = nb gardiens). **Tour des fériés** : par **rang** (`ferie_tour_rang`), fériés en semaine pris dans l'ordre chronologique.
- Le jour de tour → **garde** finissant **toujours le lendemain 8h15** ; début = bloc **continu** depuis la veille 16h45 (`++`) si la veille était travaillée, sinon **frais** à 8h00 (`+`). Les autres gardiens ce jour-là → `Exclu` (dimanche) ou `Férié` payé 1 nuit (férié semaine).

### Jardinier (`_planning_jardinier`)
- Horaire **standard** + **tour chaque dimanche** (travaille ses heures standard, on lève `(di)`) + **tour des fériés** (par rang, comme les gardiens).
- Chaque tour (dimanche **ou** férié) → **1 récupération** = le **mercredi suivant** ; cumulable (mer, jeu, ven…) si plusieurs tours visent le même mercredi ; **décalée** au jour ouvré suivant si le jour visé est dimanche/férié. Statut `Récup` (0h, échange).

### Heure d'effet (jour d'intégration)
`horaires.heure_effet` (HH:MM) : le **jour de la date d'effet**, les plages commencées **avant** l'heure d'effet sont créditées présentes jusqu'à l'heure d'effet (arrivée non captable car personne ajoutée en cours de journée), la suite est suivie par pointage (`_augmenter_jour_integration`). Ne joue **que** ce jour-là.

## Données (tables clés)

- `employes` (PK `prno` minuscule) : `nom_complet`, `categorie`, `rotation_cycle`, `rotation_ref_date`, `dimanche_tour_ref`, `dimanche_tour_cycle`, `ferie_tour_rang`, …
- `horaires` / `historique_horaires` : `code_horaire`, `date_effet`, `heure_effet`, `commentaire`. `get_horaire(prno, date)` résout : exceptions > actuel > historique.
- `exceptions_horaires` : horaire ponctuel pour une date.
- `pointages` : `prno`, `date_local`, `heure_locale`, `type_pointage` (arrivee/depart), `session` (matin/apm), `source` (telegram/fingerprint/badge/pin). Unique sur `(prno, date_local, type_pointage, session)`.
- `liaisons_empreintes` : `fingerprint_id` (INTEGER), **`pin_id` (TEXT)**, `prno`. ⚠️ Le **PIN est une chaîne** pour préserver les zéros de tête (`061019` ≠ `61019`) — ne jamais le `parseInt`/`int()`.
- `liaisons` : liaison Telegram. `jours_feries` : `date_str`, `libelle`.

## Conventions & pièges

- `prno` toujours `.strip().lower()`.
- Renommer un PRNO (`modifier_employe`) répercute sur **toutes** les tables liées (FK off le temps de la transaction).
- FK activées (`PRAGMA foreign_keys=ON`) ; pour renommer/reconstruire, désactiver hors transaction.
- Statuts UI (`index.html`) : maps de badges/cellules (`badge-*`, `cel-*`) — ajouter tout nouveau statut dans ces maps **et** le CSS.
- Les fonctions d'agrégat principales : `get_resume_jour_avec_horaires(date)` et `get_resume_periode_avec_synthese(deb, fin)`.
- Dates internes en ISO `yyyy-mm-dd` ; l'UI affiche/saisit en `yyyy/mm/dd`.

## Git
Le dépôt travaille en commits directs sur `main`. Messages de commit en français, terminés par la ligne `Co-Authored-By`.
