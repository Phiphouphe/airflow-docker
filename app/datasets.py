from airflow.datasets import Dataset

# ===========================================================================
# RAW — tables alimentées par les DAGs API (API_*_flights_raw, API_weather_raw)
# ===========================================================================
raw_flights_table           = Dataset("postgres://postgres_api/flight_dw/raw/raw_flights")
raw_scheduled_flights_table = Dataset("postgres://postgres_api/flight_dw/raw/scheduled_flights")
raw_weather_table           = Dataset("postgres://postgres_api/flight_dw/raw/raw_weather")
raw_scheduled_weather_table = Dataset("postgres://postgres_api/flight_dw/raw/scheduled_weather")

# ===========================================================================
# SIGNAUX PAR VILLE — indiquent qu'une ville a fini d'écrire dans raw_flights
# Produits par chaque DAG API_*_flights_raw
# Consommés par le DAG sentinelle ALL_flights_raw_ready
# ===========================================================================
raw_flights_nice_done       = Dataset("signal://raw_flights_nice_done")
raw_flights_lyon_done       = Dataset("signal://raw_flights_lyon_done")
raw_flights_marseille_done  = Dataset("signal://raw_flights_marseille_done")
raw_flights_toulouse_done   = Dataset("signal://raw_flights_toulouse_done")
raw_flights_bordeaux_done   = Dataset("signal://raw_flights_bordeaux_done")

raw_scheduled_flights_nice_done      = Dataset("signal://raw_scheduled_flights_nice_done")
raw_scheduled_flights_lyon_done      = Dataset("signal://raw_scheduled_flights_lyon_done")
raw_scheduled_flights_marseille_done = Dataset("signal://raw_scheduled_flights_marseille_done")
raw_scheduled_flights_toulouse_done  = Dataset("signal://raw_scheduled_flights_toulouse_done")
raw_scheduled_flights_bordeaux_done  = Dataset("signal://raw_scheduled_flights_bordeaux_done")

# ===========================================================================
# SIGNAUX GLOBAUX — toutes les villes ont fini d'écrire dans raw_flights
# Produits par le DAG sentinelle ALL_flights_raw_ready
# Consommés par le DAG ALL_flights_staging
# ===========================================================================
raw_flights_all_cities_ready           = Dataset("signal://raw_flights_all_cities_ready")
raw_scheduled_flights_all_cities_ready = Dataset("signal://raw_scheduled_flights_all_cities_ready")

# ===========================================================================
# STAGING — tables alimentées par les DAGs de transformation
#           (ALL_flights_staging, Cities_weather_staging)
# ===========================================================================
stg_flights_table           = Dataset("postgres://postgres_api/flight_dw/staging/raw_flights")
stg_scheduled_flights_table = Dataset("postgres://postgres_api/flight_dw/staging/scheduled_flights")
stg_weather_table           = Dataset("postgres://postgres_api/flight_dw/staging/raw_weather")
stg_scheduled_weather_table = Dataset("postgres://postgres_api/flight_dw/staging/scheduled_weather")

# ===========================================================================
# ANALYTICS — tables alimentées par le DAG d'enrichissement métier
#             (NICE_flights_analytics)
# ===========================================================================
ana_flights_table           = Dataset("postgres://postgres_api/flight_dw/analytics/raw_flights")
ana_scheduled_flights_table = Dataset("postgres://postgres_api/flight_dw/analytics/scheduled_flights")

# ===========================================================================
# REF — tables de référentiel alimentées par les DAGs manuels
#       (iata_reference_import, weather_codes_import)
# ===========================================================================
ref_iata_delay_codes_table  = Dataset("postgres://postgres_api/flight_dw/ref/iata_delay_codes")
ref_weather_codes_table     = Dataset("postgres://postgres_api/flight_dw/ref/weather_codes")

# ===========================================================================
# ML — dataset représentant le modèle ML entraîné
#      (ML_training_raw_flights → ML_predict_scheduled_flights)
# ===========================================================================
ml_model_dataset            = Dataset("file:///mlflow/models/latest")