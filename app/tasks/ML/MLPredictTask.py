import logging
import joblib

import app.helper as helper

from datetime import timedelta

from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator


class MLPredictTask(PythonOperator):
    """
    Charge le meilleur modèle depuis le fichier sauvegardé et prédit sur de nouvelles données.
    """

    def __init__(
        self,
        input_file: str,
        output_file: str,
        model_xcom_task_id: str,
        features: list,
        task_id: str = "ml_predict_task",
        execution_timeout: timedelta = timedelta(minutes=10),
        **kwargs_op,
    ):
        self._input_file = input_file
        self._output_file = output_file
        self._model_xcom_task_id = model_xcom_task_id
        self._features = features

        super().__init__(
            task_id=task_id,
            python_callable=self._run,
            execution_timeout=execution_timeout,
            **kwargs_op,
        )

    def _run(self, **context):

        try:
            # Récupère XCom du meilleur modèle
            ti = context["ti"]
            model_info = ti.xcom_pull(task_ids=self._model_xcom_task_id)
            model_path = model_info["model_path"]
            logging.info(f"Chargement du modèle : {model_path}")

            # Charge le pipeline
            model = joblib.load(model_path)

            # Charge les données
            df = helper.load_parquet_to_df(self.dag.dag_id, self._input_file)
            X = df[self._features]

            # Prédictions
            df["prediction"] = model.predict(X)

            # Sauvegarde les prédictions
            helper.generate_parquet_to_temp(self.dag.dag_id, df, self._output_file)
            logging.info(f"✅ Prédictions sauvegardées dans : {self._output_file}")

        except Exception as e:
            raise AirflowFailException(f"Erreur MLPredictTask {self.task_id}: {e}")