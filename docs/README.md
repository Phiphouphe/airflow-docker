# ✈️ DST Airlines — Flight Delay Prediction Pipeline

> Pipeline de prédiction de retards de vols en production · Airflow · FastAPI · MLflow · Docker · AWS EC2

---

## 📋 Table des matières

- [Vue d'ensemble](#-vue-densemble)
- [Architecture](#-architecture)
- [Stack technique](#-stack-technique)
- [Structure du projet](#-structure-du-projet)
- [Prérequis](#-prérequis)
- [Installation & démarrage](#-installation--démarrage)
- [Variables d'environnement](#-variables-denvironnement)
- [Services & ports](#-services--ports)
- [Pipeline Airflow](#-pipeline-airflow)
- [Modèle ML](#-modèle-ml)
- [Monitoring](#-monitoring)
- [CI/CD](#-cicd)
- [Workflow Git](#-workflow-git)
- [Limitations connues](#-limitations-connues)
- [Perspectives & Roadmap](#-perspectives--roadmap)

---

## 🎯 Vue d'ensemble

DST Airlines est un projet de Data Engineering de bout en bout qui prédit les retards de vols à l'arrivée pour **5 aéroports français** : Nice (NCE), Lyon (LYS), Marseille (MRS), Toulouse (TLS) et Bordeaux (BOD).

Le pipeline couvre l'ensemble du cycle de vie de la donnée :

- **Ingestion** automatisée via Apache Airflow (schedules décalés de 15 min par aéroport pour respecter le rate limiting de l'API Air France)
- **Transformation** en plusieurs couches PostgreSQL (raw → staging → analytics → ref → ml) selon le modèle en médaillon
- **Entraînement** automatique de 3 modèles de classification (XGBoost, Logistic Regression, Random Forest) avec sélection du meilleur
- **Serving** via une API FastAPI + interface Streamlit
- **Tracking** des expériences ML via MLflow (registry, versioning, promotion en production)
- **Monitoring** via Prometheus, Grafana, Alertmanager et Pushgateway
- **Déploiement** sur AWS EC2 avec Docker Compose et CI/CD via GitHub Actions

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        AWS EC2 (Ubuntu)                         │
│                                                                 │
│  ┌─────────────┐    ┌──────────────────────────────────────┐   │
│  │   GitHub    │───▶│           Docker Compose             │   │
│  │   Actions   │    │                                      │   │
│  │  (CI/CD)    │    │  ┌─────────────────────────────┐    │   │
│  └─────────────┘    │  │       Apache Airflow         │    │   │
│                     │  │  ┌──────────┐ ┌───────────┐ │    │   │
│                     │  │  │Scheduler │ │  Worker   │ │    │   │
│                     │  │  │    ↓     │ │ (Celery)  │ │    │   │
│                     │  │  │  Redis   │→│           │ │    │   │
│                     │  │  └──────────┘ └───────────┘ │    │   │
│                     │  │  ┌──────────┐ ┌───────────┐ │    │   │
│                     │  │  │API Server│ │ Triggerer │ │    │   │
│                     │  │  │  :8080   │ │           │ │    │   │
│                     │  │  └──────────┘ └───────────┘ │    │   │
│                     │  └────────────┬────────────────┘    │   │
│                     │               │                      │   │
│                     │  ┌────────────▼──────────────────┐  │   │
│                     │  │        PostgreSQL ×2           │  │   │
│                     │  │  airflow+mlflow | api_db       │  │   │
│                     │  │  :5432 (interne) | :15432      │  │   │
│                     │  └───────────────────────────────-┘  │   │
│                     │                                      │   │
│                     │  ┌──────────┐  ┌──────────────────┐ │   │
│                     │  │  MLflow  │  │     FastAPI      │ │   │
│                     │  │  :5000   │  │     :8000        │ │   │
│                     │  └──────────┘  └──────────────────┘ │   │
│                     │                                      │   │
│                     │  ┌──────────┐  ┌────────────────┐  │   │
│                     │  │Streamlit │  │   Prometheus   │  │   │
│                     │  │  :8501   │  │   :9090        │  │   │
│                     │  └──────────┘  │   Grafana :3000│  │   │
│                     │                │   Alertmgr:9093│  │   │
│                     │                └────────────────┘  │   │
│                     └──────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Stack technique

| Catégorie | Technologies |
|---|---|
| **Orchestration** | Apache Airflow 3.0.6 · CeleryExecutor · Redis 7.2 |
| **Base de données** | PostgreSQL 16 (×2 instances) · PGAdmin 4 |
| **ML** | XGBoost · Logistic Regression · Random Forest · MLflow · Scikit-learn |
| **API** | FastAPI · Uvicorn |
| **Interface** | Streamlit · Power BI |
| **Monitoring** | Prometheus · Grafana · Alertmanager · Pushgateway |
| **Conteneurisation** | Docker · Docker Compose |
| **CI/CD** | GitHub Actions (CI automatique + CD manuel) |
| **Infra** | AWS EC2 Ubuntu 22.04 — t3.medium |
| **Alerting** | Gmail via Alertmanager |

---

## 📁 Structure du projet

```
airflow-docker/
├── .github/
│   └── workflows/          # CI (tests + lint) et CD (déploiement manuel)
├── alertmanager/
│   └── alertmanager.yml    # Config alertes email Gmail
├── api/                    # Service FastAPI
│   └── dockerfile
├── app/                    # Logique applicative partagée (tasks, helpers)
├── config/
│   └── airflow.cfg         # Configuration Airflow
├── dags/                   # 15 DAGs Airflow
│   ├── API_BORDEAUX_flights_raw.py
│   ├── API_LYON_flights_raw.py
│   ├── API_MARSEILLE_flights_raw.py
│   ├── API_NICE_flights_raw.py
│   ├── API_TOULOUSE_flights_raw.py
│   ├── API_weather_raw.py
│   ├── ALL_flights_raw_ready.py   # DAG fan-in synchronisation
│   ├── All_flights_staging.py
│   ├── Cities_weather_staging.py
│   ├── All_flights_analytics.py
│   ├── ML_training_raw_flights.py
│   ├── ML_predict_scheduled_flights.py
│   ├── iata_reference_import.py
│   ├── weather_codes_import.py
│   └── github_kpis_dag.py
├── data/                   # Données de référence
├── docs/                   # Documentation complémentaire (ADR, architecture)
├── mlflow_folder/          # Config MLflow (dockerfile + artifacts)
├── prometheus/
│   ├── prometheus.yml      # Config scraping (interval 15s)
│   └── alert_rules.yml     # Règles : AirflowDown, DAGFailed, FastAPIDown
├── scripts/                # Scripts utilitaires
├── streamlit/              # Interface utilisateur Streamlit
├── tests/                  # 7 fichiers de tests (pytest)
├── webhook/                # Service de déploiement (non configuré côté GitHub)
├── .env.example            # Template des variables d'environnement
├── docker-compose.yaml     # 18 services Docker
├── dockerfile              # Image Airflow custom
├── init-db.sql             # Initialisation des schémas PostgreSQL
├── Makefile                # Commandes raccourcis
└── requirements.txt        # Dépendances Python
```

---

## ✅ Prérequis

- Docker ≥ 24.x
- Docker Compose ≥ 2.x
- 4 Go de RAM minimum disponibles pour Docker
- 2 CPUs minimum
- 20 Go d'espace disque minimum recommandé
- Un fichier `.env` configuré (voir section dédiée)

---

## 🚀 Installation & démarrage

### 1. Cloner le repo

```bash
git clone https://github.com/Phiphouphe/airflow-docker.git
cd airflow-docker
```

### 2. Configurer les variables d'environnement

```bash
cp ".env .example" .env
# Éditer .env avec toutes les valeurs requises
```

### 3. Créer le fichier UID Airflow (Linux uniquement)

```bash
echo "AIRFLOW_UID=$(id -u)" >> .env
```

### 4. Initialiser et démarrer

```bash
# Initialisation de la base Airflow (première fois uniquement)
docker compose up airflow-init

# Démarrage de tous les services
docker compose up -d

# Vérifier l'état des services
docker compose ps
```

### Via le Makefile

```bash
make create   # docker compose down + up -d
make restart  # docker restart $(docker ps -q)
make clean    # nettoyage complet (attention : destructif)
make prune    # nettoyage léger
```

---

## 🔐 Variables d'environnement

Copier `.env .example` en `.env` et renseigner les valeurs suivantes :

| Variable | Description |
|---|---|
| `AIRFLOW_UID` | UID utilisateur Linux (résultat de `id -u`) |
| `POSTGRES_DB_API` | Nom de la base PostgreSQL pour l'API |
| `POSTGRES_USER_API` | Utilisateur PostgreSQL API |
| `POSTGRES_PASSWORD_API` | Mot de passe PostgreSQL API |
| `POSTGRES_API_HOST_PORT` | Port exposé sur l'hôte pour la base API (ex: 15432) |
| `PGADMIN_DEFAULT_EMAIL` | Email de connexion PGAdmin |
| `PGADMIN_DEFAULT_PASSWORD` | Mot de passe PGAdmin |
| `PGADMIN_PORT` | Port exposé pour PGAdmin (ex: 5050) |
| `GRAFANA_ADMIN_PASSWORD` | Mot de passe admin Grafana |
| `GMAIL_APP_PASSWORD` | App password Gmail pour Alertmanager |
| `GITHUB_TOKEN` | Token GitHub pour les DAGs |
| `GITHUB_REPO` | Repo GitHub source des données |
| `_AIRFLOW_WWW_USER_USERNAME` | Login interface Airflow (défaut : `airflow`) |
| `_AIRFLOW_WWW_USER_PASSWORD` | Mot de passe interface Airflow (défaut : `airflow`) |

> ⚠️ Ne jamais committer le fichier `.env`. Il est inclus dans le `.gitignore`.

---

## 🌐 Services & ports

| Service | Port VM | Port interne | Description |
|---|---|---|---|
| Airflow UI | 8080 | 8080 | Interface d'orchestration des DAGs |
| FastAPI | 8000 | 8000 | API de prédiction — docs : `/docs` |
| Streamlit | 8501 | 8501 | Interface utilisateur |
| MLflow | 5000 | 5000 | Tracking des expériences ML |
| Grafana | 3000 | 3000 | Dashboards de monitoring |
| Prometheus | 9090 | 9090 | Métriques brutes |
| Pushgateway | 9091 | 9091 | Push métriques depuis les DAGs |
| Alertmanager | 9093 | 9093 | Gestion des alertes |
| PostgreSQL API | 15432 | 5432 | Base données métier |
| PGAdmin | 5050 | 80 | Interface PostgreSQL |
| Redis | — | 6379 | Broker Celery (interne uniquement) |
| PostgreSQL Airflow | — | 5432 | Metadata Airflow + MLflow (interne uniquement) |
| Flower (optionnel) | 5555 | 5555 | Monitoring Celery workers |

> Pour démarrer Flower : `docker compose --profile flower up -d`

---

## 🔄 Pipeline Airflow

Le pipeline est organisé autour de **5 aéroports** avec des schedules décalés de 15 minutes pour respecter le rate limiting de l'API Air France (1 requête/seconde) :

| Aéroport | Code | Schedule ingestion |
|---|---|---|
| Nice Côte d'Azur | NCE | `0 6 * * *` |
| Lyon Saint-Exupéry | LYS | `15 6 * * *` |
| Marseille Provence | MRS | `30 6 * * *` |
| Toulouse Blagnac | TLS | `45 6 * * *` |
| Bordeaux Mérignac | BOD | `0 7 * * *` |

### Les 15 DAGs

| DAG | Rôle |
|---|---|
| `API_*_flights_raw` (×5) | Ingestion vols par aéroport |
| `API_weather_raw` | Ingestion météo (Open-Meteo) |
| `ALL_flights_raw_ready` | Synchronisation fan-in — attend les 5 aéroports |
| `All_flights_staging` | Nettoyage et typage |
| `Cities_weather_staging` | Nettoyage météo |
| `All_flights_analytics` | Enrichissement et agrégations |
| `ML_training_raw_flights` | Entraînement des 3 modèles (5h40 quotidien) |
| `ML_predict_scheduled_flights` | Prédictions (trigger Data-Aware) |
| `iata_reference_import` | Chargement des codes IATA (unique) |
| `weather_codes_import` | Chargement des codes météo (unique) |
| `github_kpis_dag` | KPIs GitHub |

### Chaîne de traitement

```
[API_*_flights_raw] ×5 ──┐
[API_weather_raw]         ├──▶ [ALL_flights_raw_ready] ──▶ [staging] ──▶ [analytics] ──▶ [ML]
[REF : IATA + météo] ────┘
```

Les DAGs sont chaînés via des **Dataset triggers** (Airflow 2.4+).

### Schémas PostgreSQL

| Schéma | Tables | Rôle |
|---|---|---|
| `raw` | `raw_flights`, `raw_weather`, `scheduled_flights`, `scheduled_weather` | Données brutes ingérées |
| `staging` | `staging_flights`, `staging_weather`, `scheduled_flights`, `scheduled_weather` | Données nettoyées et typées |
| `analytics` | `raw_flights`, `scheduled_flights` | Données enrichies et agrégées |
| `ref` | `iata_delay_codes`, `weather_codes` | Référentiels statiques |
| `ml` | `flight_predictions` | Prédictions générées par le modèle |

---

## 🤖 Modèle ML

- **Algorithmes** : 3 modèles de classification entraînés à chaque cycle — XGBoost, Logistic Regression, Random Forest
- **Sélection automatique** : le modèle avec le meilleur score est promu en Production dans MLflow
- **Cible** : retard à l'arrivée > 15 minutes (1 = retard, 0 = ponctuel)
- **Métrique** : F1-score (choisi pour gérer le déséquilibre des classes)
- **Tracking** : MLflow — paramètres, métriques, artifacts, registry (Staging → Production → Archived)
- **Entraînement** : MLTrainTask déclenché à 5h40 chaque matin sur `analytics.raw_flights`
- **Inférence** : MLPredictTask déclenché par Dataset trigger sur `analytics.scheduled_flights` + ShortCircuitOperator (vérifie les 5 aéroports)
- **Réentraînement continu** : les modèles sont réentraînés quotidiennement sur les données les plus récentes — mitigation native de la dérive des données

---

## 📊 Monitoring

Stack **Prometheus / Grafana / Alertmanager** :

- **Prometheus** scrape les métriques toutes les 15 secondes depuis 4 sources : Airflow (`:8080/admin/metrics`), FastAPI (`:8000/metrics`), Pushgateway (`:9091`), Prometheus lui-même
- **Pushgateway** : les DAGs ML poussent leurs métriques métier (durée d'exécution, nombre de prédictions, % retards prédits, temps d'inférence)
- **Grafana** : dashboards de suivi en temps réel
- **Alertmanager** : routage des alertes vers email Gmail avec `send_resolved: true`

### Règles d'alerte

| Alerte | Condition | Sévérité |
|---|---|---|
| `AirflowDown` | `up{job="airflow"} == 0` pendant 1 min | critical |
| `DAGFailed` | `dag_last_status failed > 0` pendant 1 min | warning |
| `FastAPIDown` | `up{job="fastapi"} == 0` pendant 1 min | critical |

---

## ⚙️ CI/CD

Deux workflows GitHub Actions séparés :

### CI — automatique (push / PR)

Déclenché sur `feature_philistine`, `develop`, `main` :

1. **Lint & qualité** : flake8, black, bandit
2. **Tests unitaires** : pytest + coverage sur 7 fichiers de tests
3. **DAG integrity tests** : validation de l'intégrité des 15 DAGs
4. **Docker build check** : build des images API et Streamlit

### CD — déploiement manuel

Déclenché manuellement depuis GitHub Actions :

1. Tests critiques avant déploiement
2. Connexion SSH à l'EC2
3. `git pull` + `docker compose build api streamlit`
4. `docker compose up -d` + restart workers Airflow
5. Health checks automatiques : API (`:8000/health`), Streamlit (`:8501`), DAGs Airflow

---

## 🌿 Workflow Git

```
feature/xxx  ──┐
               ▼
             develop  ──▶  main  ──▶  EC2 (CD manuel via GitHub Actions)
```

- `feature/*` : développement isolé par fonctionnalité
- `develop` : branche d'intégration
- `main` : branche de production

---

## ⚠️ Limitations connues

| Limitation | Détail |
|---|---|
| Pas de staging | Déploiement direct en production — pas d'environnement de validation intermédiaire |
| EC2 single node | Pas de haute disponibilité — une panne de l'instance arrête l'ensemble du système |
| Secrets en `.env` | En production réelle : AWS Secrets Manager ou HashiCorp Vault |
| Volume de données limité | ~2 000 vols depuis mars 2026 — contraint par le rate limiting de l'API Air France |

---

## 🔭 Perspectives & Roadmap

| Priorité | Évolution | Impact |
|---|---|---|
| 1 | **Terraform** | Provisionner EC2, VPC, Security Groups, S3 en une commande — reproductibilité totale |
| 2 | **Ansible** | Automatiser la configuration de l'EC2 après provisionnement (Docker, .env, services) |
| 3 | **Environnements dev / staging / prod** | Une VM dédiée par environnement pour valider avant de déployer en production |
| 4 | **Modèle de régression** | Prédire le retard en minutes, pas seulement retard / pas retard |
| 5 | **Blue / Green Deployment** | 2 VM en production + AWS ALB — bascule instantanée, zéro interruption de service |

---

## 👤 Auteur

**Philistine Serour** — Data Engineer  
Projet de fin de formation RNCP niveau 7 — Datascientest  
[GitHub](https://github.com/Phiphouphe/airflow-docker)
