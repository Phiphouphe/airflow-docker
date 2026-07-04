import logging
import mlflow
import mlflow.sklearn
import pandas as pd
import numpy as np

import app.helper as helper

from datetime import timedelta

from mlflow.tracking import MlflowClient
from airflow.operators.python import PythonOperator
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import LinearRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from xgboost import XGBRegressor


class MLRegressionTrainTask(PythonOperator):
    """
    Entraîne plusieurs modèles de régression pour prédire le nombre de minutes
    de retard sur les vols réellement en retard (is_delayed = True).
    Sélectionne le meilleur modèle selon le MAE (plus bas = meilleur).
    """

    def __init__(
        self,
        input_file: str,
        experiment_name: str,
        model_registry_name: str,
        features: list,
        target: str = "delay_minutes",
        test_size: float = 0.2,
        staging_threshold: float = 25.0,
        task_id: str = "ML_regression_train_raw_flights_task",
        execution_timeout: timedelta = timedelta(minutes=20),
        **kwargs_op,
    ):
        """
        Args:
            input_file        : nom du fichier Parquet d'entrée
            experiment_name   : nom de l'expérience MLflow
            model_registry_name : nom du registre MLflow
            features          : liste des colonnes features
            target            : colonne cible (delay_minutes)
            test_size         : proportion du jeu de test (défaut 0.2)
            staging_threshold : MAE maximum pour aller en Staging (défaut 20 min)
        """
        self._input_file = input_file
        self._experiment_name = experiment_name
        self._model_registry_name = model_registry_name
        self._features = features
        self._target = target
        self._test_size = test_size
        self._staging_threshold = staging_threshold

        super().__init__(
            task_id=task_id,
            python_callable=self._run,
            execution_timeout=execution_timeout,
            **kwargs_op,
        )

    def _run(self, **context):
        try:
            # ── Chargement des données ────────────────────────────────────────
            df = helper.load_parquet_to_df(self.dag.dag_id, self._input_file)
            logging.info(f"✅ Fichier Parquet chargé : {len(df)} lignes")

            # ── Filtrage sur les vols en retard uniquement ────────────────────
            df_delayed = df[df["is_delayed"]].copy()
            df_delayed = df_delayed.dropna(subset=[self._target])
            logging.info(f"📊 Vols en retard disponibles pour l'entraînement : {len(df_delayed)} lignes")

            if len(df_delayed) < 50:
                raise ValueError(
                    f"Pas assez de vols en retard pour entraîner un modèle de régression "
                    f"({len(df_delayed)} lignes, minimum 50 requis)"
                )

            # ── Préparation des features ──────────────────────────────────────
            available_features = [f for f in self._features if f in df_delayed.columns]
            missing = set(self._features) - set(available_features)
            if missing:
                logging.warning(f"⚠️ Features manquantes ignorées : {missing}")

            X = df_delayed[available_features]
            y = df_delayed[self._target]

            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=self._test_size, random_state=42
            )

            # ── Préprocesseur ─────────────────────────────────────────────────
            categorical_features = [
                f for f in available_features
                if X[f].dtype == "object" or str(X[f].dtype) == "category"
            ]
            numeric_features = [
                f for f in available_features
                if f not in categorical_features
            ]

            preprocessor = ColumnTransformer(
                transformers=[
                    ("num", StandardScaler(), numeric_features),
                    ("cat", OneHotEncoder(handle_unknown="ignore"), categorical_features),
                ]
            )

            # ── Modèles à entraîner ───────────────────────────────────────────
            models_dict = {
                "LinearRegression": Pipeline([
                    ("preprocessor", preprocessor),
                    ("regressor", LinearRegression())
                ]),
                "RandomForestRegressor": Pipeline([
                    ("preprocessor", preprocessor),
                    ("regressor", RandomForestRegressor(n_estimators=100, random_state=42))
                ]),
                "GradientBoostingRegressor": Pipeline([
                    ("preprocessor", preprocessor),
                    ("regressor", GradientBoostingRegressor(n_estimators=100, random_state=42))
                ]),
                "XGBRegressor": Pipeline([
                    ("preprocessor", preprocessor),
                    ("regressor", XGBRegressor(n_estimators=100, random_state=42, verbosity=0))
                ]),
            }

            # ── Connexion MLflow ──────────────────────────────────────────────
            mlflow.set_experiment(self._experiment_name)
            logging.info(f"Experiment: {self._experiment_name}")

            results = []

            for name, model in models_dict.items():
                logging.info("=" * 50)
                logging.info(f"Entraînement du modèle {name}...")

                model.fit(X_train, y_train)
                y_pred = model.predict(X_test)

                mae = mean_absolute_error(y_test, y_pred)
                rmse = np.sqrt(mean_squared_error(y_test, y_pred))
                r2 = r2_score(y_test, y_pred)

                logging.info(
                    f"📊 {name} — MAE: {mae:.2f} min | RMSE: {rmse:.2f} min | R²: {r2:.4f}"
                )

                artifact_path = f"{name.lower()}_delay_regression"

                with mlflow.start_run(run_name=f"{name}_regression_run") as run:
                    mlflow.log_param("model_type", name)
                    mlflow.log_param("features", str(available_features))
                    mlflow.log_param("target", self._target)
                    mlflow.log_param("test_size", self._test_size)
                    mlflow.log_metric("mae", mae)
                    mlflow.log_metric("rmse", rmse)
                    mlflow.log_metric("r2", r2)
                    mlflow.sklearn.log_model(model, artifact_path)
                    run_id = run.info.run_id

                logging.info(f"Found run ID: {run_id}")

                results.append({
                    "model_name": name,
                    "mae": mae,
                    "rmse": rmse,
                    "r2": r2,
                    "run_id": run_id,
                    "artifact_path": artifact_path,
                })

            # ── Sélection du meilleur modèle (MAE le plus bas) ───────────────
            best = min(results, key=lambda x: x["mae"])
            logging.info(f"🏆 Meilleur modèle : {best['model_name']} (MAE: {best['mae']:.2f} min)")

            # ── Vérification du seuil minimum ─────────────────────────────────
            if best["mae"] > self._staging_threshold:
                logging.warning(
                    f"⚠️ MAE {best['mae']:.2f} min au-dessus du seuil {self._staging_threshold} min. "
                    f"Le modèle n'est pas enregistré."
                )
                return {
                    "model_name": best["model_name"],
                    "mae": best["mae"],
                    "status": "below_threshold",
                }

            # ── Enregistrement dans MLflow Registry ───────────────────────────
            model_uri = f"runs:/{best['run_id']}/{best['artifact_path']}"
            mv = mlflow.register_model(model_uri, self._model_registry_name)

            client = MlflowClient()
            client.set_model_version_tag(
                self._model_registry_name, mv.version, "mae", str(round(best["mae"], 2))
            )
            client.set_model_version_tag(
                self._model_registry_name, mv.version, "model_name", best["model_name"]
            )

            # ── Comparaison avec la Production actuelle ───────────────────────
            best_production_mae = self._get_best_production_mae(client, self._model_registry_name)
            logging.info(f"🏆 Meilleur MAE en Production actuel : {best_production_mae:.2f} min")

            client.transition_model_version_stage(
                name=self._model_registry_name,
                version=mv.version,
                stage="Staging",
            )
            logging.info(
                f"📋 Version {mv.version} ({best['model_name']}) passée en Staging "
                f"(MAE: {best['mae']:.2f} min)"
            )

            # MAE plus bas = meilleur modèle
            if best["mae"] < best_production_mae:
                for v in client.get_latest_versions(self._model_registry_name, stages=["Production"]):
                    client.transition_model_version_stage(
                        name=self._model_registry_name,
                        version=v.version,
                        stage="Archived",
                    )
                    logging.info(f"✅ Version {v.version} (était en Production) archivée")

                client.transition_model_version_stage(
                    name=self._model_registry_name,
                    version=mv.version,
                    stage="Production",
                )
                logging.info(
                    f"🚀 Version {mv.version} ({best['model_name']}) passée en Production "
                    f"(MAE: {best['mae']:.2f} < {best_production_mae:.2f})"
                )
            else:
                logging.info(
                    f"📋 Version {mv.version} reste en Staging. "
                    f"MAE {best['mae']:.2f} n'améliore pas la Production ({best_production_mae:.2f})"
                )

            return {
                "model_name": best["model_name"],
                "mae": best["mae"],
                "model_version": mv.version,
            }

        except Exception as e:
            raise Exception(f"Erreur MLRegressionTrainTask {self.task_id}: {e}")

    def _get_best_production_mae(self, client: MlflowClient, model_name: str) -> float:
        """Récupère le MAE du modèle actuellement en Production."""
        try:
            versions = client.get_latest_versions(model_name, stages=["Production"])
            if not versions:
                logging.info("Aucun modèle en Production — premier entraînement.")
                return float("inf")
            v = versions[0]
            tag_mae = v.tags.get("mae")
            if tag_mae:
                return float(tag_mae)
            return float("inf")
        except Exception:
            return float("inf")

