# Architecture Decision Records — DST Airlines

Les ADRs (Architecture Decision Records) documentent les choix techniques structurants du projet, leur contexte et leur justification.

---

## ADR-001 — Apache Airflow comme orchestrateur

**Statut** : Accepté

**Contexte**  
Le pipeline nécessite d'orchestrer des tâches dépendantes les unes des autres (ingestion → staging → analytics → ML) sur 5 aéroports, avec des schedules décalés et une gestion des échecs.

**Décision**  
Utiliser Apache Airflow 3.0.6 avec CeleryExecutor.

**Justification**
- Gestion native des dépendances entre tâches via les DAGs (Directed Acyclic Graphs)
- Dataset triggers (Airflow 2.4+) pour chaîner les DAGs sans polling actif
- CeleryExecutor permet l'exécution parallèle des tâches sur plusieurs workers
- UI riche pour monitorer les runs, relancer des tâches en échec, visualiser les logs
- Standard de l'industrie en Data Engineering

**Alternatives considérées**
- Cron jobs simples : pas de gestion des dépendances ni de retry automatique
- Prefect : moins mature en entreprise au moment du projet
- Luigi : moins de fonctionnalités et communauté plus réduite

---

## ADR-002 — CeleryExecutor + Redis comme broker

**Statut** : Accepté

**Contexte**  
Airflow propose plusieurs executors : SequentialExecutor (1 tâche à la fois), LocalExecutor (parallélisme local), CeleryExecutor (workers distribués).

**Décision**  
Utiliser CeleryExecutor avec Redis comme message broker.

**Justification**
- Exécution parallèle des DAGs des 5 aéroports simultanément
- Redis est léger, rapide et parfaitement adapté au rôle de broker de messages
- Architecture scalable : on peut ajouter des workers Celery sans changer la config

**Alternatives considérées**
- LocalExecutor : suffisant pour un seul aéroport, pas pour 5 en parallèle
- KubernetesExecutor : surdimensionné pour une infra single-node EC2

---

## ADR-003 — Deux instances PostgreSQL séparées

**Statut** : Accepté

**Contexte**  
Le projet a deux besoins distincts en base de données : la base interne d'Airflow et MLflow (métadonnées, logs, états des DAGs, registry ML) et la base métier pour les données de vols et les prédictions de l'API.

**Décision**  
Deux instances PostgreSQL séparées : `postgres` (Airflow + MLflow) et `postgres_api` (données métier — vols, prédictions).

**Justification**
- Isolation des données : une corruption de la base Airflow n'affecte pas les données métier
- Performances : les requêtes Airflow n'entrent pas en compétition avec les requêtes applicatives
- Sécurité : credentials distincts pour chaque base
- Bonne pratique recommandée par la documentation officielle Airflow

**Alternatives considérées**
- Une seule instance PostgreSQL avec plusieurs databases : plus simple mais couplage fort

---

## ADR-004 — MLflow pour le tracking des expériences ML

**Statut** : Accepté

**Contexte**  
Le pipeline entraîne 3 modèles (XGBoost, Logistic Regression, Random Forest) et sélectionne automatiquement le meilleur. Il faut tracer les expériences, comparer les métriques et versionner les modèles.

**Décision**  
Utiliser MLflow comme plateforme de tracking et de model registry.

**Justification**
- Tracking natif des paramètres, métriques et artifacts pour chaque run
- Comparaison visuelle des 3 modèles sur une même interface
- Model Registry pour versionner et promouvoir le meilleur modèle (Staging → Production → Archived)
- Intégration directe avec Scikit-learn et XGBoost
- Backend store sur PostgreSQL et artifact store sur volume Docker

**Alternatives considérées**
- Weights & Biases : payant au-delà d'un certain usage
- DVC : orienté versioning de données, moins adapté au tracking d'expériences
- Logs manuels : non reproductible et difficile à comparer

---

## ADR-005 — Sélection automatique du meilleur modèle

**Statut** : Accepté

**Contexte**  
Plutôt que de fixer un algorithme unique, l'objectif est de toujours servir le modèle le plus performant pour la prédiction de retards.

**Décision**  
Entraîner 3 modèles de classification (XGBoost, Logistic Regression, Random Forest) à chaque cycle quotidien et sélectionner automatiquement celui avec le meilleur score.

**Justification**
- Robustesse : si un modèle se dégrade sur de nouvelles données, un autre peut prendre le relais
- Automatisation complète sans intervention manuelle
- MLflow permet de comparer les runs et de promouvoir le modèle gagnant via le Model Registry
- Le F1-score est utilisé comme métrique de sélection (plutôt que l'accuracy) en raison du déséquilibre des classes
- Le réentraînement quotidien constitue une stratégie de mitigation native de la dérive des données

**Alternatives considérées**
- Modèle unique fixe : plus simple mais moins résilient aux dérives de données

---

## ADR-006 — Prometheus + Grafana + Alertmanager pour le monitoring

**Statut** : Accepté

**Contexte**  
Un pipeline en production doit être observable : détecter les échecs, monitorer les performances et alerter en cas d'anomalie.

**Décision**  
Stack Prometheus / Grafana / Alertmanager / Pushgateway.

**Justification**
- Prometheus scrape nativement les métriques des services exposant un endpoint `/metrics`
- Pushgateway permet aux DAGs Airflow (jobs éphémères) de pousser leurs métriques métier
- Grafana offre des dashboards configurables
- Alertmanager gère le routage des alertes email (Gmail) avec `send_resolved: true`
- Trois règles d'alerte configurées : AirflowDown, DAGFailed, FastAPIDown
- Stack open source, standard de l'industrie

**Alternatives considérées**
- Datadog : puissant mais payant
- ELK Stack : orienté logs plutôt que métriques
- Monitoring Airflow natif seul : insuffisant pour monitorer l'ensemble de la stack

---

## ADR-007 — FastAPI pour le serving des prédictions

**Statut** : Accepté

**Contexte**  
Le modèle ML doit être accessible via une API pour que l'interface Streamlit puisse afficher les prédictions.

**Décision**  
Utiliser FastAPI comme framework API.

**Justification**
- Documentation Swagger auto-générée (`/docs`) sans effort supplémentaire
- Typage fort via Pydantic — validation automatique des inputs
- Performances élevées (ASGI)
- Intégration naturelle avec les modèles Scikit-learn et MLflow
- Authentification intégrée pour sécuriser les accès

**Alternatives considérées**
- Flask : plus simple mais pas de validation automatique ni de documentation native
- Django REST Framework : trop lourd pour un service de prédiction

---

## ADR-008 — Service Webhook pour le déploiement

**Statut** : En attente de configuration

**Contexte**  
Le CI/CD doit permettre un redéploiement automatique sur EC2 à chaque push sur `main`, sans accès SSH direct dans le pipeline GitHub Actions.

**Décision**  
Un service `webhook` est présent dans l'infrastructure Docker Compose. Il écoute sur le port 9099 et a accès au Docker socket de l'hôte pour piloter Docker directement.

**État actuel**  
Le service est déployé et opérationnel côté EC2, mais le webhook GitHub n'est pas encore configuré côté repository. Le déploiement est actuellement déclenché manuellement via GitHub Actions (workflow CD).

**Justification**
- Le service webhook a accès au Docker socket — peut piloter Docker directement
- Découple le pipeline GitHub Actions du SSH direct sur EC2
- Solution légère, sans dépendance externe

**Alternatives considérées**
- SSH direct dans GitHub Actions (solution actuelle en CD manuel) : fonctionnel mais nécessite une intervention humaine
- AWS CodeDeploy : plus robuste mais complexité et coût supplémentaires

---

## ADR-009 — Docker Compose comme orchestrateur de conteneurs

**Statut** : Accepté

**Contexte**  
18 services doivent tourner ensemble sur une instance EC2 unique, avec des dépendances de démarrage, des healthchecks et des volumes partagés.

**Décision**  
Utiliser Docker Compose.

**Justification**
- Définition déclarative de l'ensemble de la stack en un seul fichier
- Gestion native des dépendances (`depends_on` avec `condition: service_healthy`)
- Volumes nommés pour la persistance des données (PostgreSQL, MLflow artifacts)
- Suffisant pour une infrastructure single-node
- Makefile pour simplifier les commandes courantes

**Alternatives considérées**
- Kubernetes : pertinent pour scaler horizontalement mais surdimensionné pour une instance unique
- Docker Swarm : compromis possible mais moins de documentation et d'adoption

---

## ADR-010 — Schedules décalés par aéroport

**Statut** : Accepté

**Contexte**  
L'API Air France impose une limite stricte de **1 requête par seconde**. Déclencher les DAGs des 5 aéroports simultanément entraînerait des erreurs 429 (Too Many Requests) et des données manquantes.

**Décision**  
Décaler les cron schedules de chaque aéroport de 15 minutes.

**Justification**
- Respect de la contrainte de rate limiting imposée par l'API Air France
- Évite les erreurs 429 et garantit l'intégrité des données ingérées
- Implémentation simple : modifier uniquement le schedule dans chaque DAG
- Aucun coût supplémentaire en infrastructure

**Alternatives considérées**
- Déclenchement simultané : entraîne des rejets de l'API et des données manquantes
- Retry avec backoff exponentiel : complexifie les DAGs sans résoudre le problème à la source
