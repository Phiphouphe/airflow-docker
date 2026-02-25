import logging
import joblib
import os

import mlflow
import mlflow.sklearn

import app.helper as helper

from datetime import timedelta

from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator
from sklearn.model_selection import train_test_split

class MLTrainTask(PythonOperator):

    def __init__(
        self,
        input_file: str,
        features: list,
        target: str,
        models: dict,
        test_size: float = 0.2,
        model_dir: str = "/opt/airflow/models",
        task_id: str = "ml_train_task",
        execution_timeout: timedelta = timedelta(minutes=20),
        **kwargs_op,
        ):
        """
        Entraîne plusieurs modèles ML sur une table historique Postgres,
        sélectionne le meilleur modèle et renvoie XCom.

        Arguments:
        - input_file (str): Chemin du fichier source.
        - features (list): Liste des noms de colonnes à utiliser comme features.
        - target (str): Nom de la colonne cible.
        - models (dict): Dictionnaire de modèles ML à entraîner.
        - test_size (float, optional): Proportion des données à utiliser pour le test. Par defaut: 0.2.
        - task_id (str, optional): Identifiant de la tâche Airflow. Par defaut: "ml_train_task".
        - execution_timeout (timedelta, optional): Durée maximale d'exécution de la tâche. Par defaut: 20 minutes
        """
        self._input_file = input_file
        self._features = features
        self._target = target
        self._models = models
        self._test_size = test_size
        self._model_dir = model_dir

        super().__init__(
            task_id=task_id,
            python_callable=self._run,
            execution_timeout=execution_timeout,
            **kwargs_op,
        )

    def _run(self):

        # Charger les données depuis le fichier source
        df = helper.load_parquet_to_df(self.dag.dag_id, self._input_file)

        # Traiter les données pour l'entrainement des modèles ML
        try:
            X = df[self._features]
            y = df[self._target]

            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=self._test_size, random_state=42
            )

            # Set MLflow experiment
            mlflow.set_tracking_uri("file:///opt/airflow/mlruns")
            mlflow.set_experiment("Flight Delay Prediction")

            # Entraîner les modèles et évaluer leurs performances
            results = []
            for name, model in self._models.items():
                logging.info(f"Entraînement du modèle {name}...")
                model.fit(X_train, y_train)
                score = model.score(X_test, y_test)
                logging.info(f"📊 {name} score: {score}")

                results.append({"model_name": name, "score": score, "model_object": model})

            # Sélection du meilleur modèle
            best = max(results, key=lambda x: x["score"])
            os.makedirs(self._model_dir, exist_ok=True)
            model_path = f"{self._model_dir}/best_model.pkl"

            # Sauvegarde du pipeline sur disque (pour compatibilité)
            joblib.dump(best["model_object"], model_path)
            logging.info(f"✅ Meilleur modèle sauvegardé : {model_path}")

            # Log the best model to MLflow
            with mlflow.start_run(run_name=f"{best['model_name']}_best_model_run"):
                mlflow.log_param("model_type", best["model_name"])
                mlflow.log_param("features", str(self._features))
                mlflow.log_param("target", self._target)
                mlflow.log_param("test_size", self._test_size)
                mlflow.log_metric("test_accuracy", best["score"])
                mlflow.sklearn.log_model(best["model_object"], "model")

                # Register the model in MLflow Registry
                model_uri = f"runs:/{mlflow.active_run().info.run_id}/model"
                mlflow.register_model(model_uri, "flight_delay_model")

            # Retourne un XCom (retour natif de PythonOperator)
            return {"model_name": best["model_name"], "model_path": model_path, "score": best["score"]}

        except Exception as e:
            raise AirflowFailException(f"Erreur MLTrainTask {self.task_id}: {e}")