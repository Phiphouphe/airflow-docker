import logging

import mlflow
import mlflow.sklearn

import app.helper as helper

from datetime import timedelta, datetime

from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator
from sklearn.model_selection import train_test_split
from mlflow.tracking import MlflowClient

class MLTrainTask(PythonOperator):

    def __init__(
        self,
        input_file: str,
        features: list,
        target: str,
        models: dict,
        experiment_name: str = "ML_Experiment",        
        model_registry_name: str = "ml_model",         
        test_size: float = 0.2,
        staging_threshold: float = 0.7,
        task_id: str = "ml_train_task",
        execution_timeout: timedelta = timedelta(minutes=20),
        **kwargs_op,
        ):
        """
        Tâche Airflow générique pour entraîner des modèles ML sur un dataset donné et les enregistrer dans MLflow.
        Le meilleur modèle est automatiquement sélectionné et comparé à la Production.

        Arguments :
        - input_file : fichier ou table avec les données d'entraînement
        - features : colonnes/features à utiliser pour l'entraînement
        - target : colonne cible à prédire
        - models : dictionnaire de modèles ML à entraîner
        - experiment_name : nom de l'expérience MLflow
        - model_registry_name : nom du modèle dans le MLflow Registry
        - test_size : proportion des données à utiliser pour le test
        - staging_threshold : seuil minimum pour aller en Staging
        - task_id : identifiant de la tâche Airflow
        - execution_timeout : durée maximale d'exécution de la tâche
        """
        self._input_file = input_file
        self._features = features
        self._target = target
        self._models = models
        self._experiment_name = experiment_name
        self._model_registry_name = model_registry_name
        self._test_size = test_size
        self._staging_threshold = staging_threshold

        super().__init__(
            task_id=task_id,
            python_callable=self._run,
            execution_timeout=execution_timeout,
            **kwargs_op,
        )

    def _run(self):

        df = helper.load_parquet_to_df(self.dag.dag_id, self._input_file)

        try:
            X = df[self._features]
            y = df[self._target]

            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=self._test_size, random_state=42, stratify=y,
            )

            mlflow.set_tracking_uri("http://mlflow:5000")
            client = MlflowClient()
            logging.info(f"Using tracking URI: http://mlflow:5000")

            if not mlflow.get_experiment_by_name(self._experiment_name):
                mlflow.create_experiment(
                    self._experiment_name,
                    artifact_location=f"mlflow-artifacts:/{self._experiment_name}"
                )
            mlflow.set_experiment(self._experiment_name)
            logging.info(f"Experiment: {self._experiment_name}")

            # Entraîner et logger tous les modèles
            results = []
            for name, model in self._models.items():
                try:
                    logging.info(f"\n{'='*50}")
                    logging.info(f"Entraînement du modèle {name}...")
                    model.fit(X_train, y_train)
                    score = model.score(X_test, y_test)
                    logging.info(f"📊 {name} score: {score:.4f}")

                    artifact_path = f"{name.lower()}_flight_delay"
                    with mlflow.start_run(run_name=f"{name}_run") as run:
                        mlflow.log_param("model_type", name)
                        mlflow.log_param("features", str(self._features))
                        mlflow.log_param("target", self._target)
                        mlflow.log_param("test_size", self._test_size)
                        mlflow.log_metric("test_accuracy", score)
                        mlflow.sklearn.log_model(model, artifact_path)
                        run_id = run.info.run_id

                    self._log_model_artifacts(client, run_id, artifact_path)

                    results.append({
                        "model_name": name,
                        "score": score,
                        "model_object": model,
                        "run_id": run_id,
                        "artifact_path": artifact_path
                    })

                except Exception as e:
                    logging.warning(f"⚠️ Modèle {name} ignoré : {e}")
                    continue

            if not results:
                raise AirflowFailException(f"Erreur MLTrainTask {self.task_id}: Aucun modèle n'a pu être entraîné.")

            # Sélection du meilleur modèle
            best = max(results, key=lambda x: x["score"])

            logging.info(f"\n{'='*50}")
            logging.info(f"🏆 Meilleur modèle sélectionné : {best['model_name']} (score: {best['score']:.4f})")
            logging.info(f"Registering model from: runs:/{best['run_id']}/{best['artifact_path']}")
            logging.info(f"Model name: {self._model_registry_name}")

            model_uri = f"runs:/{best['run_id']}/{best['artifact_path']}"
            mv = mlflow.register_model(model_uri, self._model_registry_name)

            # Ajout des tags
            client.set_model_version_tag(self._model_registry_name, mv.version, "model_type", best["model_name"])
            client.set_model_version_tag(self._model_registry_name, mv.version, "test_accuracy", str(round(best["score"], 4)))
            client.set_model_version_tag(self._model_registry_name, mv.version, "trained_at", datetime.now().strftime("%Y-%m-%d %H:%M"))
            client.set_model_version_tag(self._model_registry_name, mv.version, "features", str(self._features))
            client.set_model_version_tag(self._model_registry_name, mv.version, "dag_id", self.dag.dag_id)

            # Vérification du seuil minimum
            if best["score"] < self._staging_threshold:
                client.transition_model_version_stage(
                    name=self._model_registry_name,
                    version=mv.version,
                    stage="Archived",
                )
                logging.warning(
                    f"⚠️ Score {best['score']:.4f} sous le seuil {self._staging_threshold}. "
                    f"Modèle archivé directement."
                )
                return {
                    "model_name": best["model_name"],
                    "score": best["score"],
                    "model_version": mv.version,
                    "stage": "Archived"
                }

            # 1. Récupérer le score de Production AVANT de toucher à quoi que ce soit
            best_production_score = self._get_best_production_score(client, self._model_registry_name)
            logging.info(f"🏆 Meilleur score en Production actuel : {best_production_score:.4f}")

            # 2. Archiver Staging et None uniquement (Production protégée)
            self._archive_old_versions(client, self._model_registry_name, mv.version)

            # 3. Passer la nouvelle version en Staging
            client.transition_model_version_stage(
                name=self._model_registry_name,
                version=mv.version,
                stage="Staging",
            )
            logging.info(f"📋 Version {mv.version} ({best['model_name']}) passée en Staging avec score {best['score']:.4f}")

            # 4. Comparer et décider
            if best["score"] > best_production_score:
                # Archiver l'ancien modèle en Production seulement maintenant
                self._archive_production(client, self._model_registry_name, mv.version)
                client.transition_model_version_stage(
                    name=self._model_registry_name,
                    version=mv.version,
                    stage="Production",
                )
                logging.info(f"🚀 Version {mv.version} ({best['model_name']}) passée en Production (score: {best['score']:.4f} > {best_production_score:.4f})")
                stage = "Production"
            else:
                logging.info(
                    f"📋 Version {mv.version} reste en Staging. "
                    f"Score {best['score']:.4f} n'améliore pas la Production ({best_production_score:.4f})"
                )
                stage = "Staging"

            return {
                "model_name": best["model_name"],
                "score": best["score"],
                "model_version": mv.version,
                "stage": stage
            }

        except AirflowFailException:
            raise
        except Exception as e:
            raise AirflowFailException(f"Erreur MLTrainTask {self.task_id}: {e}")

    def _get_best_production_score(self, client: MlflowClient, model_name: str) -> float:
        """Retourne le meilleur score parmi les modèles actuellement en Production.
        Si aucun modèle en Production ou si erreur, retourne 0.0."""
        try:
            versions = client.get_latest_versions(model_name, stages=["Production"])
            if not versions:
                return 0.0
            scores = []
            for v in versions:
                score_tag = v.tags.get("test_accuracy")
                if score_tag:
                    scores.append(float(score_tag))
            return max(scores) if scores else 0.0
        except Exception:
            return 0.0

    def _archive_old_versions(self, client: MlflowClient, model_name: str, new_version: str):
        """Archive toutes les versions en Staging et None sauf la nouvelle version.
        La Production est protégée et ne sera archivée que si le nouveau modèle est meilleur."""
        try:
            all_versions = client.search_model_versions(f"name='{model_name}'")
            for version in all_versions:
                if (
                    str(version.version) != str(new_version)
                    and version.current_stage not in ["Archived", "Production"]  # ← Production protégée
                ):
                    client.transition_model_version_stage(
                        name=model_name,
                        version=version.version,
                        stage="Archived"
                    )
                    logging.info(f"✅ Version {version.version} (était en {version.current_stage}) archivée")
        except Exception as e:
            logging.warning(f"⚠️ Impossible d'archiver les anciennes versions : {e}")

    def _archive_production(self, client: MlflowClient, model_name: str, new_version: str):
        """Archive uniquement les versions en Production car le nouveau modèle est meilleur."""
        try:
            versions = client.get_latest_versions(model_name, stages=["Production"])
            for version in versions:
                if str(version.version) != str(new_version):
                    client.transition_model_version_stage(
                        name=model_name,
                        version=version.version,
                        stage="Archived"
                    )
                    logging.info(f"✅ Version {version.version} (Production) archivée")
        except Exception as e:
            logging.warning(f"⚠️ Impossible d'archiver la Production : {e}")

    def _log_model_artifacts(self, client: MlflowClient, run_id: str, artifact_path: str):
        """Affiche les artefacts d'un modèle dans les logs Airflow."""
        logging.info(f"Found run ID: {run_id}")
        logging.info(f"Available artifacts:")
        logging.info(f"1. {artifact_path} (dir)")
        artifacts = client.list_artifacts(run_id, artifact_path)
        for artifact in artifacts:
            logging.info(f"   - {artifact.path}")
