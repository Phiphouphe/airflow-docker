# Architecture DST Airlines

## 1. Architecture globale des services

```mermaid
graph TB
    subgraph CICD["⚙️ CI/CD"]
        GH[GitHub Actions\nCI automatique + CD manuel]
    end

    subgraph AIRFLOW["🔄 Orchestration — Apache Airflow 3.0.6"]
        SCHED[Scheduler]
        WORKER[Worker Celery]
        TRIGGER[Triggerer]
        DAGPROC[DAG Processor]
        APISVR[API Server :8080]
        REDIS[(Redis :6379\nbroker Celery)]
        SCHED -->|queue| REDIS
        REDIS --> WORKER
    end

    subgraph DB["🗄️ Bases de données"]
        PG_AF[(PostgreSQL\nairflow + mlflow)]
        PG_API[(PostgreSQL API\nflight_predictions :15432)]
        PGADMIN[PGAdmin :5050]
    end

    subgraph ML["🤖 Machine Learning"]
        MLFLOW[MLflow :5000\nregistry + tracking]
    end

    subgraph SERVING["🌐 Serving"]
        API[FastAPI :8000]
        STREAM[Streamlit :8501]
    end

    subgraph MONITORING["📊 Monitoring"]
        PROM[Prometheus :9090]
        GRAF[Grafana :3000]
        PUSH[Pushgateway :9091]
        ALERT[Alertmanager :9093]
        PROM --> GRAF
        PROM --> ALERT
    end

    GH -->|SSH + docker compose| AIRFLOW

    AIRFLOW --> PG_AF
    AIRFLOW --> PG_API
    AIRFLOW -->|train + predict| MLFLOW
    AIRFLOW --> PUSH

    PG_API --> API
    API --> STREAM

    PUSH --> PROM
    ALERT -->|email Gmail| GMAIL[📧 Gmail]

    PGADMIN --> PG_AF
    PGADMIN --> PG_API
```

---

## 2. Pipeline de données — du brut au serving

```mermaid
flowchart LR
    subgraph SOURCES["📡 Sources externes"]
        FLIGHTS[API Air France\nNCE·LYS·MRS·TLS·BOD]
        WEATHER[API Open-Meteo\nmétéo par aéroport]
        REF_SRC[Référentiels\nIATA + codes météo]
    end

    subgraph RAW["raw"]
        R1[raw_flights\nraw_scheduled]
        R2[raw_weather\nscheduled_weather]
    end

    subgraph STAGING["staging"]
        S1[staging_flights\nscheduled_flights]
        S2[staging_weather\nscheduled_weather]
    end

    subgraph REF["ref"]
        REF1[iata_delay_codes]
        REF2[weather_codes]
    end

    subgraph ANALYTICS["analytics"]
        A1[raw_flights]
        A2[scheduled_flights]
    end

    subgraph ML_SCHEMA["ml"]
        ML1[Dataset entraînement\n— analytics.raw_flights]
        ML2[Dataset inférence\n— analytics.scheduled_flights]
        ML3[flight_predictions]
    end

    subgraph MLFLOW_BOX["MLflow"]
        EXP[Expériences & métriques\n3 modèles comparés]
        MODEL[Meilleur modèle\nXGBoost / LogReg / RandomForest]
    end

    subgraph SERVING_BOX["Serving"]
        API2[FastAPI :8000]
        UI[Streamlit :8501]
    end

    FLIGHTS -->|DAG ingestion décalé 15min| R1
    WEATHER -->|DAG météo| R2
    REF_SRC -->|chargement unique| REF1
    REF_SRC -->|chargement unique| REF2

    R1 -->|DAG staging| S1
    R2 -->|DAG staging| S2

    S1 -->|DAG analytics + jointure REF| A1
    S2 -->|DAG analytics + jointure REF| A2
    REF1 & REF2 --> A1

    A1 -->|MLTrainTask 5h40| EXP
    EXP --> MODEL
    MODEL -->|promu en Production| MLFLOW_BOX

    A2 -->|MLPredictTask Data-Aware| ML3
    MODEL -->|charge modèle Production| ML3

    ML3 -->|flight_predictions| API2
    API2 --> UI
```

---

## 3. Les 15 DAGs — chaîne d'orchestration

```mermaid
flowchart TD
    subgraph INGESTION["Ingestion — schedules décalés 15min"]
        NCE[API_NICE_flights_raw\n0h00]
        LYS[API_LYON_flights_raw\n0h15]
        MRS[API_MARSEILLE_flights_raw\n0h30]
        TLS[API_TOULOUSE_flights_raw\n0h45]
        BOD[API_BORDEAUX_flights_raw\n1h00]
        WEA[API_weather_raw]
    end

    subgraph REF_DAGS["Référentiels — chargement unique"]
        IATA[iata_reference_import]
        WCOD[weather_codes_import]
    end

    subgraph SYNC["Synchronisation"]
        FANIN[ALL_flights_raw_ready\nfan-in — attend les 5 aéroports]
    end

    subgraph TRANSFORM["Transform — PostgreSQL"]
        STG[All_flights_staging\nCities_weather_staging]
        ANA[All_flights_analytics]
    end

    subgraph ML_DAGS["Machine Learning"]
        TRAIN[ML_training_raw_flights\n5h40 quotidien]
        PRED[ML_predict_scheduled_flights\nData-Aware trigger]
    end

    subgraph UTILS["Utilitaires"]
        KPI[github_kpis_dag]
    end

    NCE & LYS & MRS & TLS & BOD & WEA --> FANIN
    IATA & WCOD --> ANA
    FANIN --> STG --> ANA --> TRAIN
    ANA --> PRED
```

---

## 4. Règles d'alerte Monitoring

| Alerte | Expression PromQL | Délai | Sévérité | Action |
|---|---|---|---|---|
| `AirflowDown` | `up{job="airflow"} == 0` | 1 min | critical | Email Gmail |
| `DAGFailed` | `sum(airflow_dag_last_status{status="failed"}) > 0` | 1 min | warning | Email Gmail |
| `FastAPIDown` | `up{job="fastapi"} == 0` | 1 min | critical | Email Gmail |

> `send_resolved: true` — une notification est envoyée aussi à la résolution de l'alerte.
