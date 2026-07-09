# Guide de déploiement — DST Airlines

## Table des matières

- [Architecture de déploiement actuelle](#architecture-de-déploiement-actuelle)
- [Prérequis](#prérequis)
- [Déploiement initial sur EC2](#déploiement-initial-sur-ec2)
- [CI/CD — pipeline GitHub Actions](#cicd--pipeline-github-actions)
- [Commandes utiles](#commandes-utiles)
- [Perspectives & Roadmap](#perspectives--roadmap)

---

## Architecture de déploiement actuelle

```
Developer
    │
    │  git push
    ▼
GitHub (branches : feature/* → develop → main)
    │
    │  GitHub Actions
    │  ├── CI automatique (push / PR)
    │  │   ├── Lint & qualité (flake8, black, bandit)
    │  │   ├── Tests unitaires (pytest, 7 fichiers)
    │  │   ├── DAG integrity tests (17 DAGs)
    │  │   └── Docker build check (API + Streamlit)
    │  │
    │  └── CD manuel (workflow_dispatch)
    │      ├── Tests critiques avant déploiement
    │      ├── SSH → EC2 → git pull + docker compose build/up
    │      └── Health checks (API, Streamlit, Airflow)
    ▼
AWS EC2 (Ubuntu 22.04 — t3.medium)
    │
    └── Docker Compose (18 services)
```

Le déploiement repose sur une **instance EC2 unique** (single node) avec Docker Compose. Le pipeline CD est déclenché **manuellement** depuis l'interface GitHub Actions après validation du CI.

---

## Prérequis

### Sur la machine locale

- Git
- Accès SSH à l'instance EC2 (clé `.pem`)

### Sur l'instance EC2

- Ubuntu 22.04+
- Docker ≥ 24.x
- Docker Compose ≥ 2.x
- Git
- 4 Go RAM minimum (8 Go recommandé)
- 2 CPUs minimum
- 20 Go disque minimum

---

## Déploiement initial sur EC2

### 1. Provisionner l'instance EC2

Depuis la console AWS :

- AMI : Ubuntu 22.04 LTS
- Type : `t3.medium` minimum (2 vCPU, 4 Go RAM)
- Ouvrir les ports dans le Security Group :

| Port | Service |
|---|---|
| 22 | SSH |
| 8080 | Airflow UI |
| 8000 | FastAPI |
| 8501 | Streamlit |
| 5000 | MLflow |
| 3000 | Grafana |
| 9090 | Prometheus |
| 9091 | Pushgateway |
| 9093 | Alertmanager |
| 15432 | PostgreSQL API |
| 5050 | PGAdmin |
| 9099 | Webhook (auto-healing) |

### 2. Installer Docker sur EC2

```bash
sudo apt update
sudo apt install -y docker.io
sudo systemctl enable --now docker
sudo usermod -aG docker $USER
# Se déconnecter puis reconnecter pour appliquer le groupe docker

# Installer Docker Compose v2
sudo apt install -y docker-compose-plugin
```

### 3. Cloner le repo

```bash
git clone https://github.com/Phiphouphe/airflow-docker.git
cd airflow-docker
```

### 4. Configurer les variables d'environnement

```bash
cp ".env .example" .env
# Éditer .env avec toutes les valeurs requises
echo "AIRFLOW_UID=$(id -u)" >> .env
```

### 5. Configurer les secrets GitHub Actions

Dans le repo GitHub → Settings → Secrets and variables → Actions :

| Secret | Description |
|---|---|
| `EC2_HOST` | IP publique de l'instance EC2 |
| `EC2_USER` | Utilisateur SSH (ex: `ubuntu`) |
| `EC2_SSH_KEY` | Contenu de la clé `.pem` |

### 6. Premier démarrage

```bash
# Initialisation Airflow (une seule fois)
docker compose up airflow-init

# Démarrage de tous les services
docker compose up -d

# Vérifier l'état des services
docker compose ps
```

### 7. Vérifier les services

| Service | URL |
|---|---|
| Airflow | http://`<EC2_IP>`:8080 |
| FastAPI docs | http://`<EC2_IP>`:8000/docs |
| Streamlit | http://`<EC2_IP>`:8501 |
| MLflow | http://`<EC2_IP>`:5000 |
| Grafana | http://`<EC2_IP>`:3000 |
| PGAdmin | http://`<EC2_IP>`:5050 |
| Webhook | http://<EC2_IP>:9099/webhook |

> ⚠️ Changer les credentials par défaut Airflow (`airflow/airflow`) et Grafana avant toute exposition publique.

---

## CI/CD — pipeline GitHub Actions

### CI — automatique

Déclenché sur push vers `feature_philistine`, `develop`, `main` et sur les pull requests vers `develop` et `main` :

```
push / PR
    │
    ├── Job 1 : Lint & Code Quality
    │   ├── flake8 (syntax & style)
    │   ├── black (formatting check)
    │   └── bandit (security check)
    │
    ├── Job 2 : Unit Tests (nécessite Job 1)
    │   ├── pytest tests/test_db_extraction.py
    │   ├── pytest tests/test_duplicate_remover.py
    │   ├── pytest tests/test_date_converter.py
    │   ├── pytest tests/test_parquet_to_snapshot2.py
    │   ├── pytest tests/test_api.py
    │   ├── pytest tests/test_integration.py
    │   └── pytest tests/ --cov (coverage global)
    │
    ├── Job 3 : DAG Integrity Tests (nécessite Job 1)
    │   └── pytest tests/test_dags.py (17 DAGs)
    │
    └── Job 4 : Docker Build Check (nécessite Job 2)
        ├── docker build ./api
        └── docker build ./streamlit
```

### CD — déploiement manuel

Déclenché manuellement depuis GitHub Actions (`workflow_dispatch`) :

```
Déclenchement manuel
    │
    ├── Job 1 : Pre-deployment Checks
    │   ├── pytest tests/test_db_extraction.py
    │   └── pytest tests/test_api.py
    │
    └── Job 2 : Deploy (nécessite Job 1)
        ├── SSH → EC2 : git pull origin main
        ├── docker compose build api streamlit
        ├── docker compose up -d api streamlit
        ├── docker compose restart airflow-worker airflow-dag-processor
        ├── Health check API (curl :8000/health)
        ├── Health check Streamlit (curl :8501)
        └── Vérification DAGs chargés
```

---

## Commandes utiles

```bash
# Démarrer tous les services
make create          # docker compose down + up -d

# Redémarrer les containers actifs
make restart         # docker restart $(docker ps -q)

# Logs en temps réel
docker compose logs -f

# Logs d'un service spécifique
docker compose logs -f airflow-scheduler
docker compose logs -f airflow-worker

# État des services
docker compose ps

# Arrêt propre
docker compose down

# Arrêt avec suppression des volumes (reset complet)
docker compose down -v

# Nettoyage léger
make prune

# Nettoyage complet (containers, images, volumes — destructif)
make prune-all

# Espace disque Docker
make docker-storage

# Démarrer Flower (monitoring Celery)
docker compose --profile flower up -d
```

---

## Perspectives & Roadmap

### 1. Terraform — Infrastructure as Code

**Objectif** : provisionner l'infrastructure AWS de manière reproductible et versionnée.

```
terraform/
├── main.tf          # EC2, VPC, Security Groups, S3
├── variables.tf     # Paramètres (région, type instance…)
├── outputs.tf       # IP publique, IDs ressources
└── backend.tf       # State stocké en S3
```

Ce que Terraform gérerait :
- Création des instances EC2 (dev / staging / prod)
- Configuration du Security Group (ports ouverts)
- Création du bucket S3 pour les artifacts MLflow
- Gestion du réseau (VPC, subnet, Internet Gateway)

```bash
terraform init
terraform plan
terraform apply
```

### 2. Ansible — Configuration Management

**Objectif** : automatiser la configuration de l'instance EC2 après provisionnement.

```
ansible/
├── inventory.ini        # Hôtes cibles
├── playbook.yml         # Playbook principal
└── roles/
    ├── docker/          # Installation Docker
    ├── app/             # Clone du repo + .env
    └── monitoring/      # Configuration Prometheus/Grafana
```

```bash
ansible-playbook -i inventory.ini playbook.yml
```

### 3. Environnements dev / staging / prod

**Objectif** : isoler les environnements pour valider avant de déployer en production.

```
dev (local)  →  staging (1 EC2)  →  prod (2 EC2 + Load Balancer)
```

- **Dev** : développement local
- **Staging** : 1 EC2 identique à la prod pour valider
- **Prod** : 2 EC2 derrière un AWS ALB pour haute disponibilité

### 4. Blue / Green Deployment

**Objectif** : zéro downtime lors des déploiements, avec rollback instantané.

```
Internet
    │
    ▼
Load Balancer (AWS ALB)
    │
    ├──▶ EC2 Blue  (version N)     ← trafic actif
    └──▶ EC2 Green (version N+1)   ← en préparation
```

Processus :
1. La version Green est déployée et testée en parallèle
2. Le Load Balancer bascule le trafic vers Green
3. Blue reste en standby pour rollback immédiat si besoin

### Ordre de priorité

| Priorité | Outil | Impact |
|---|---|---|
| 1 | Terraform | Reproductibilité totale de l'infra |
| 2 | Ansible | Éliminer les étapes manuelles sur EC2 |
| 3 | Environnements séparés | Validation avant production |
| 4 | Blue/Green complet | Zéro downtime en production |
