import logging
import pandas as pd

import mlflow

import app.helper as helper

from datetime import timedelta

from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator

class MLPredictTask(PythonOperator):

    def __init__(
        self,
        input_file: str,
        features: list,
        model_dir: str = "/opt/airflow/mlruns",
        task_id: str = "ML_predict_task",
        execution_timeout: timedelta = timedelta(minutes=10),
        **kwargs_op,
        ):
        """
        Tâche Airflow générique pour faire des prédictions avec le modèle entraîné.
        Le modèle est chargé depuis un chemin fixe.

        Arguments :
        - input_file : fichier ou table avec les données à prédire
        - features : colonnes/features à utiliser
        - model_dir : répertoire où sont stockés les modèles
        """
        self._input_file = input_file
        self._features = features
        self._model_dir = model_dir

        super().__init__(
            task_id=task_id,
            python_callable=self._run,
            execution_timeout=execution_timeout,
            **kwargs_op,
        )

    def _run(self, **context):
        
        try:
            # Set MLflow tracking
            mlflow.set_tracking_uri("http://mlflow:5000")
            experiment_name = "Flight_Delay_Prediction"
            if not mlflow.get_experiment_by_name(experiment_name):
                mlflow.create_experiment(
                    experiment_name,
                    artifact_location="mlflow-artifacts:/Flight_Delay_Prediction"
                )
            mlflow.set_experiment(experiment_name)

            # Load the model from MLflow Registry (latest version)
            model_uri = "models:/flight_delay_model/latest"
            pipeline = mlflow.sklearn.load_model(model_uri)

            # Déterminer le nom du modèle à partir du pipeline
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
                model_name = "Unknown"

            # Charger les données à prédire
            df = helper.load_parquet_to_df(self.dag.dag_id, self._input_file)
            X = df[self._features]

            # Faire les prédictions
            predictions = pipeline.predict(X)
            logging.info(f"Prédictions calculées pour {len(predictions)} lignes")

            # Log to MLflow
            run_name = f"{model_name}_prediction_run"
            artifact_path = f"{model_name.lower()}_flight_delay_predictions"
            with mlflow.start_run(run_name=run_name):
                mlflow.log_param("model_used", model_name)
                mlflow.log_param("input_file", self._input_file)
                mlflow.log_param("num_predictions", len(predictions))
                mlflow.log_param("features", str(self._features))

                pred_df = pd.DataFrame({"predictions": predictions})
                pred_df.to_csv("/tmp/predictions.csv", index=False)
                mlflow.log_artifact("/tmp/predictions.csv", artifact_path)

            return {
                "model_name": model_name,
                "model_uri": model_uri,
                "predictions": predictions.tolist(),
            }

        except Exception as e:
            raise AirflowFailException(f"Erreur MLPredictTask {self.task_id}: {e}")