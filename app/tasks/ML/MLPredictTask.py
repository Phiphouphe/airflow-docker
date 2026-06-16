import logging
import time
import pytz
import pandas as pd

import mlflow
from mlflow.tracking import MlflowClient
from sqlalchemy import text

import app.helper as helper

from datetime import timedelta

from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator
from app.static.connector_db import ConnectorDb


class MLPredictTask(PythonOperator):

    def __init__(
        self,
        input_file: str,
        features: list,
        experiment_name: str = "ML_Experiment",
        model_registry_name: str = "ml_model",
        model_dir: str = "/opt/airflow/mlruns",
        task_id: str = "ML_predict_task",
        execution_timeout: timedelta = timedelta(minutes=10),
        **kwargs_op,
        ):
        """
        Tâche Airflow générique pour faire des prédictions avec le modèle en Production.

        Arguments :
        - input_file : fichier ou table avec les données à prédire
        - features : colonnes/features à utiliser
        - experiment_name : nom de l'expérience MLflow
        - model_registry_name : nom du modèle dans le MLflow Registry
        - model_dir : répertoire où sont stockés les modèles
        """
        self._input_file = input_file
        self._features = features
        self._experiment_name = experiment_name
        self._model_registry_name = model_registry_name
        self._model_dir = model_dir

        super().__init__(
            task_id=task_id,
            python_callable=self._run,
            execution_timeout=execution_timeout,
            **kwargs_op,
        )

    def _run(self, **context):

        try:
            mlflow.set_tracking_uri("http://mlflow:5000")
            client = MlflowClient()
            logging.info(f"Using tracking URI: http://mlflow:5000")

            # Créer ou sélectionner l'expérience MLflow
            if not mlflow.get_experiment_by_name(self._experiment_name):
                mlflow.create_experiment(
                    self._experiment_name,
                    artifact_location=f"mlflow-artifacts:/{self._experiment_name}"
                )
            mlflow.set_experiment(self._experiment_name)

            # Charger le modèle en Production depuis le Registry
            model_uri = f"models:/{self._model_registry_name}/Production"
            logging.info(f"Loading model from: {model_uri}")
            pipeline = mlflow.sklearn.load_model(model_uri)
            logging.info(f"✅ Modèle chargé depuis le Registry : {model_uri}")

            # Déterminer le nom du modèle
            classifier = pipeline.named_steps.get('classifier')
            if classifier:
                model_name = type(classifier).__name__.replace('Classifier', '').replace('Regressor', '')
                name_mapping = {
                    'RandomForest': 'RandomForest',
                    'LogisticRegression': 'LogisticRegression',
                    'XGB': 'XGBoost'
                }
                model_name = name_mapping.get(model_name, model_name)
            else:
                model_name = type(pipeline).__name__

            # Charger les données à prédire
            df = helper.load_parquet_to_df(self.dag.dag_id, self._input_file)
            X = df[self._features]

            # Mesurer le temps d'inférence
            start_time = time.time()
            predictions = pipeline.predict(X)
            inference_time = round(time.time() - start_time, 4)

            # Calculer la distribution des prédictions
            num_predictions = len(predictions)
            pred_series = pd.Series(predictions)
            value_counts = pred_series.value_counts()
            pct_delayed = round((value_counts.get(1, 0) / num_predictions) * 100, 2)
            pct_not_delayed = round((value_counts.get(0, 0) / num_predictions) * 100, 2)

            logging.info(f"\n{'='*50}")
            logging.info(f"📊 Résultats des prédictions :")
            logging.info(f"   - Nombre de prédictions  : {num_predictions}")
            logging.info(f"   - % retards prédits      : {pct_delayed}%")
            logging.info(f"   - % non retards prédits  : {pct_not_delayed}%")
            logging.info(f"   - Temps d'inférence      : {inference_time}s")
            logging.info(f"{'='*50}")

            # Récupérer la version du modèle en Production
            production_versions = client.get_latest_versions(self._model_registry_name, stages=["Production"])
            model_version = production_versions[0].version if production_versions else None

            # Log dans MLflow
            run_name = f"{model_name}_prediction_run"
            artifact_path = f"{model_name.lower()}_predictions"
            with mlflow.start_run(run_name=run_name) as run:
                mlflow.log_param("model_used", model_name)
                mlflow.log_param("model_uri", model_uri)
                mlflow.log_param("input_file", self._input_file)
                mlflow.log_param("features", str(self._features))
                mlflow.log_metric("num_predictions", num_predictions)
                mlflow.log_metric("pct_delayed", pct_delayed)
                mlflow.log_metric("pct_not_delayed", pct_not_delayed)
                mlflow.log_metric("inference_time_sec", inference_time)

                pred_df_mlflow = pd.DataFrame({"predictions": predictions})
                pred_df_mlflow.to_csv("/tmp/predictions.csv", index=False)
                mlflow.log_artifact("/tmp/predictions.csv", artifact_path)

                if production_versions:
                    prod_version = production_versions[0].version
                    client.set_model_version_tag(self._model_registry_name, prod_version, "last_prediction_run_id", run.info.run_id)
                    client.set_model_version_tag(self._model_registry_name, prod_version, "last_num_predictions", str(num_predictions))
                    client.set_model_version_tag(self._model_registry_name, prod_version, "last_pct_delayed", str(pct_delayed))
                    client.set_model_version_tag(self._model_registry_name, prod_version, "last_inference_time_sec", str(inference_time))
                    logging.info(f"✅ Tags mis à jour sur la version {prod_version} du modèle en Production")

                run_id = run.info.run_id

            # Écriture des prédictions en base de données
            try:
                engine = ConnectorDb.get_db_engine("flight_dw_postgres")

                pred_db = df[["flight_number", "date", "dep_hour", "origin_airport", "destination_airport",
                            "departure_time_block", "day_of_week", "month", "is_cancelled"]].copy()
                pred_db["is_delayed"] = predictions

                paris = pytz.timezone("Europe/Paris")
                pred_db["prediction_date"] = pd.Timestamp.now(tz=paris).replace(tzinfo=None)

                pred_db["run_id"] = run_id
                pred_db["model_name"] = model_name
                pred_db["model_version"] = model_version
                pred_db = pred_db.rename(columns={"date": "flight_date"})

                with engine.begin() as conn:
                    # Créer le schéma ml si nécessaire
                    conn.execute(text("CREATE SCHEMA IF NOT EXISTS ml"))

                    # Créer la table si elle n'existe pas
                    conn.execute(text("""
                        CREATE TABLE IF NOT EXISTS ml.flight_predictions (
                            id                   SERIAL PRIMARY KEY,
                            flight_number        VARCHAR,
                            flight_date          DATE,
                            dep_hour             INTEGER,
                            origin_airport       VARCHAR,
                            destination_airport  VARCHAR,
                            departure_time_block VARCHAR,
                            day_of_week          INTEGER,
                            month                INTEGER,
                            is_cancelled         BOOLEAN,
                            is_delayed           BOOLEAN,
                            prediction_date      TIMESTAMP DEFAULT NOW(),
                            run_id               VARCHAR,
                            model_name           VARCHAR,
                            model_version        VARCHAR,
                            CONSTRAINT uq_flight_prediction 
                                UNIQUE (flight_number, flight_date)
                        )
                    """))

                    # Upsert ligne par ligne
                    for _, row in pred_db.iterrows():
                        conn.execute(text("""
                            INSERT INTO ml.flight_predictions 
                                (flight_number, flight_date, dep_hour, origin_airport, destination_airport,
                                departure_time_block, day_of_week, month, is_cancelled,
                                is_delayed, prediction_date, run_id, model_name, model_version)
                            VALUES 
                                (:flight_number, :flight_date, :dep_hour, :origin_airport, :destination_airport,
                                :departure_time_block, :day_of_week, :month, :is_cancelled,
                                :is_delayed, :prediction_date, :run_id, :model_name, :model_version)
                            ON CONFLICT (flight_number, flight_date)
                            DO UPDATE SET
                                flight_number = EXCLUDED.flight_number,
                                dep_hour = EXCLUDED.dep_hour,
                                is_delayed = EXCLUDED.is_delayed,
                                prediction_date = EXCLUDED.prediction_date,
                                run_id = EXCLUDED.run_id,
                                model_name = EXCLUDED.model_name,
                                model_version = EXCLUDED.model_version
                            """), {
                            "flight_number": row["flight_number"],
                            "flight_date": row["flight_date"],
                            "dep_hour": int(row["dep_hour"]),
                            "origin_airport": row["origin_airport"],
                            "destination_airport": row["destination_airport"],
                            "departure_time_block": row["departure_time_block"],
                            "day_of_week": int(row["day_of_week"]),
                            "month": int(row["month"]),
                            "is_cancelled": bool(row["is_cancelled"]),
                            "is_delayed": bool(row["is_delayed"]),
                            "prediction_date": row["prediction_date"],
                            "run_id": row["run_id"],
                            "model_name": row["model_name"],
                            "model_version": str(row["model_version"]),
                        })

                logging.info(f"✅ {len(pred_db)} prédictions écrites en base (ml.flight_predictions)")

            except Exception as e:
                logging.warning(f"⚠️ Impossible d'écrire les prédictions en base : {e}")

            return {
                "model_name": model_name,
                "model_uri": model_uri,
                "num_predictions": num_predictions,
                "pct_delayed": pct_delayed,
                "pct_not_delayed": pct_not_delayed,
                "inference_time_sec": inference_time,
                "predictions": predictions.tolist(),
            }

        except Exception as e:
            raise AirflowFailException(f"Erreur MLPredictTask {self.task_id}: {e}")