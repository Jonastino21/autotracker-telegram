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

La **catégorie** (`employes.categorie` : `standard` / `gardien` / `jardinier`) pilote la logique. `_planning_jour(emp, code, date_obj, feries)` renvoie `(code_effectif, force_statut)` ; `force_statut` ∈ `{None, Repos, Récup, Férié}` (les gardiens ne renvoient plus que `None` ou `Repos`).

### Standard
Suit son code horaire. Férié = payé (théo crédité).

### Gardien (`_planning_gardien`)
- **Rotation de nuit 2 nuits / 1 repos** (`rotation_cycle` ex. `2/3`, `rotation_ref_date`), qui tourne **en continu tous les jours** (dimanche et férié inclus). Position = `(jours_écoulés) mod cycle` — **plus de « jour à part »**. Quand le cycle est en travail → **nuit `16h45 → lendemain 08h15`** (overnight auto-détecté, `fin < début`).
- **En plus**, tour de **JOUR `08h00 → 17h00`** : le **dimanche** (rotation hebdo `dimanche_tour_ref` + `dimanche_tour_cycle` = nb gardiens) **et** les **fériés en semaine** (par **rang** `ferie_tour_rang`, fériés pris dans l'ordre chronologique). C'est **additif** : un gardien peut cumuler le jour (8-17h) **et** sa nuit (16h45→…) le même jour → code à 2 segments `1645di0815;0800di1700`.
- La **nuit** du dimanche/férié est assurée par le gardien que le **cycle** désigne (nuit ordinaire), pas par le gardien de tour.
- **Repos** du cycle sans tour → `Repos`. Plus de statut `Garde`/`Exclu`/`Férié` pour les gardiens (un férié au repos = simple `Repos`, non payé). Les jours travaillés → `Complet`/`Incomplet`/`Absent` selon les pointages.
- ⚠️ Léger recouvrement théorique de 15 min quand jour (→17h00) + nuit (16h45→) coexistent (24h30 au lieu de 24h15). Toléré.
- **Échanges / remplacements** (`remplacements_garde`, appliqué dans `_planning_gardien` par-dessus `_planning_gardien_base`) : titulaire remplacé un jour → statut `Échange` (0h, non pénalisé) ; le remplaçant fait le service du titulaire (cumulé au sien) et en touche les heures. Échange avec rattrapage = **2 remplacements** (un par date). Saisie : Admin → onglet « Échanges de garde ».
- Départ du petit matin (08h15) : rattaché à la garde **de la veille** (`_ajouter_departs_nuit`) et **retiré** du jour suivant (`_retirer_departs_veille`) → pas de « Fin » fantôme le lendemain.

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
- `remplacements_garde` : `date_str`, `prno_titulaire`, `prno_remplacant`, `motif`. Unique sur `(date_str, prno_titulaire)`. Échanges de garde entre gardiens (cf. moteur gardien).

## Conventions & pièges

- `prno` toujours `.strip().lower()`.
- Renommer un PRNO (`modifier_employe`) répercute sur **toutes** les tables liées (FK off le temps de la transaction).
- FK activées (`PRAGMA foreign_keys=ON`) ; pour renommer/reconstruire, désactiver hors transaction.
- Statuts UI (`index.html`) : maps de badges/cellules (`badge-*`, `cel-*`) — ajouter tout nouveau statut dans ces maps **et** le CSS.
- Les fonctions d'agrégat principales : `get_resume_jour_avec_horaires(date)` et `get_resume_periode_avec_synthese(deb, fin)`.
- Dates internes en ISO `yyyy-mm-dd` ; l'UI affiche/saisit en `yyyy/mm/dd`.

## Git
Le dépôt travaille en commits directs sur `main`. Messages de commit en français, terminés par la ligne `Co-Authored-By`.
