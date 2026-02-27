import logging
import time
import pandas as pd

import mlflow
from mlflow.tracking import MlflowClient

import app.helper as helper

from datetime import timedelta

from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator

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
            client = MlflowClient()  # ← ajouté
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

            # Log dans Airflow
            logging.info(f"\n{'='*50}")
            logging.info(f"📊 Résultats des prédictions :")
            logging.info(f"   - Nombre de prédictions  : {num_predictions}")
            logging.info(f"   - % retards prédits      : {pct_delayed}%")
            logging.info(f"   - % non retards prédits  : {pct_not_delayed}%")
            logging.info(f"   - Temps d'inférence      : {inference_time}s")
            logging.info(f"{'='*50}")

            # Log dans MLflow
            run_name = f"{model_name}_prediction_run"
            artifact_path = f"{model_name.lower()}_predictions"
            with mlflow.start_run(run_name=run_name) as run:
                # Params
                mlflow.log_param("model_used", model_name)
                mlflow.log_param("model_uri", model_uri)
                mlflow.log_param("input_file", self._input_file)
                mlflow.log_param("features", str(self._features))

                # Métriques
                mlflow.log_metric("num_predictions", num_predictions)
                mlflow.log_metric("pct_delayed", pct_delayed)
                mlflow.log_metric("pct_not_delayed", pct_not_delayed)
                mlflow.log_metric("inference_time_sec", inference_time)

                # Artefact
                pred_df = pd.DataFrame({"predictions": predictions})
                pred_df.to_csv("/tmp/predictions.csv", index=False)
                mlflow.log_artifact("/tmp/predictions.csv", artifact_path)

                # Tagger la version du modèle en Production avec les stats du dernier run
                production_versions = client.get_latest_versions(self._model_registry_name, stages=["Production"])
                if production_versions:
                    prod_version = production_versions[0].version
                    client.set_model_version_tag(self._model_registry_name, prod_version, "last_prediction_run_id", run.info.run_id)
                    client.set_model_version_tag(self._model_registry_name, prod_version, "last_num_predictions", str(num_predictions))
                    client.set_model_version_tag(self._model_registry_name, prod_version, "last_pct_delayed", str(pct_delayed))
                    client.set_model_version_tag(self._model_registry_name, prod_version, "last_inference_time_sec", str(inference_time))
                    logging.info(f"✅ Tags mis à jour sur la version {prod_version} du modèle en Production")

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