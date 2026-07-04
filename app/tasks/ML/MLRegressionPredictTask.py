import logging
import mlflow
import mlflow.sklearn
import pandas as pd
import numpy as np
import psycopg2

import app.helper as helper

from datetime import timedelta

from airflow.hooks.base import BaseHook
from airflow.providers.standard.operators.python import PythonOperator
from mlflow.tracking import MlflowClient


class MLRegressionPredictTask(PythonOperator):
    """
    Prédit le nombre de minutes de retard pour les vols prédits en retard
    par le modèle de classification (is_delayed = True).
    Écrit predicted_delay_minutes dans ml.flight_predictions.
    """

    def __init__(
        self,
        input_file: str,
        experiment_name: str,
        model_registry_name: str,
        features: list,
        task_id: str = "ML_regression_predict_scheduled_flights_task",
        execution_timeout: timedelta = timedelta(minutes=10),
        **kwargs_op,
    ):
        self._input_file = input_file
        self._experiment_name = experiment_name
        self._model_registry_name = model_registry_name
        self._features = features

        super().__init__(
            task_id=task_id,
            python_callable=self._run,
            execution_timeout=execution_timeout,
            **kwargs_op,
        )

    def _run(self, **context):
        try:
            # ── Chargement des données du jour ────────────────────────────────
            df = helper.load_parquet_to_df(self.dag.dag_id, self._input_file)
            logging.info(f"✅ Fichier Parquet chargé : {len(df)} lignes")

            # ── Connexion PostgreSQL ──────────────────────────────────────────
            conn_config = BaseHook.get_connection("flight_dw_postgres")
            conn = psycopg2.connect(
                host=conn_config.host,
                port=conn_config.port,
                dbname=conn_config.schema,
                user=conn_config.login,
                password=conn_config.password,
            )
            cursor = conn.cursor()

            # ── Récupère les vols prédits en retard aujourd'hui ───────────────
            cursor.execute("""
                SELECT flight_number, flight_date, origin_airport
                FROM ml.flight_predictions
                WHERE is_delayed = TRUE
                AND flight_date = CURRENT_DATE
            """)
            delayed_flights = cursor.fetchall()

            if not delayed_flights:
                logging.info("ℹ️ Aucun vol prédit en retard aujourd'hui.")
                conn.close()
                return {"predicted": 0}

            logging.info(f"📊 {len(delayed_flights)} vols prédits en retard")

            # ── Filtre le DataFrame sur les vols en retard ────────────────────
            delayed_keys = {(str(r[0]), str(r[2])) for r in delayed_flights}
            df_delayed = df[
                df.apply(
                    lambda row: (str(row["flight_number"]), str(row["origin_airport"])) in delayed_keys,
                    axis=1
                )
            ].copy()

            if df_delayed.empty:
                logging.warning("⚠️ Aucun vol en retard trouvé dans le Parquet")
                conn.close()
                return {"predicted": 0}

            # ── Chargement du modèle de régression depuis MLflow ──────────────
            mlflow.set_tracking_uri("http://mlflow:5000")
            client = MlflowClient()

            versions = client.get_latest_versions(self._model_registry_name, stages=["Production"])
            if not versions:
                logging.warning("⚠️ Aucun modèle de régression en Production.")
                conn.close()
                return {"predicted": 0}

            model_version = versions[0].version
            model_name = versions[0].tags.get("model_name", "unknown")
            model_uri = f"models:/{self._model_registry_name}/Production"
            model = mlflow.sklearn.load_model(model_uri)
            logging.info(f"✅ Modèle chargé : {model_name} version {model_version}")

            # ── Prédiction ────────────────────────────────────────────────────
            available_features = [f for f in self._features if f in df_delayed.columns]
            X = df_delayed[available_features]
            predicted_minutes = model.predict(X)
            predicted_minutes = np.maximum(predicted_minutes, 0)
            df_delayed["predicted_delay_minutes"] = predicted_minutes.round(1)

            # ── Mise à jour de ml.flight_predictions ──────────────────────────
            updated = 0
            for _, row in df_delayed.iterrows():
                cursor.execute("""
                    UPDATE ml.flight_predictions
                    SET predicted_delay_minutes = %s,
                        regression_model_name = %s,
                        regression_model_version = %s
                    WHERE flight_number = %s
                    AND flight_date = CURRENT_DATE
                    AND origin_airport = %s
                    AND is_delayed = TRUE
                """, (
                    float(row["predicted_delay_minutes"]),
                    model_name,
                    str(model_version),
                    str(row["flight_number"]),
                    str(row["origin_airport"]),
                ))
                updated += cursor.rowcount

            conn.commit()
            logging.info(f"✅ predicted_delay_minutes mis à jour pour {updated} vols")
            cursor.close()
            conn.close()

            return {
                "predicted": updated,
                "model_name": model_name,
                "model_version": model_version,
            }

        except Exception as e:
            logging.error(f"❌ Erreur MLRegressionPredictTask : {e}")
            raise