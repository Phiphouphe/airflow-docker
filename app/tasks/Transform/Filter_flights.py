import logging
import pandas as pd

from datetime import timedelta

from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator

import app.helper as helper


class Filter_flights(PythonOperator):

    def __init__(
        self,
        input_parquet_file: str,
        output_parquet_file_raw: str,
        output_parquet_file_scheduled: str,
        execution_timeout: timedelta = timedelta(seconds=30),
        task_id: str = "filter_flights",
        **kwargs,
    ):
        """
        Filtre les données de vols en deux catégories :
        - raw_flights : vols avec actual_departure NOT NULL
        - scheduled_flights : vols avec actual_departure IS NULL

        Arguments :
        - input_parquet_file (str) : Nom du fichier Parquet d'entrée.
        - output_parquet_file_raw (str) : Nom du fichier Parquet pour raw_flights.
        - output_parquet_file_scheduled (str) : Nom du fichier Parquet pour scheduled_flights.
        - execution_timeout (timedelta, optionnel) : Durée maximale d'exécution. Par défaut 30 secondes.
        - task_id (str, optionnel) : Identifiant de la tâche Airflow. Par défaut "filter_flights".
        """
        self._input_file = input_parquet_file
        self._output_file_raw = output_parquet_file_raw
        self._output_file_scheduled = output_parquet_file_scheduled

        super().__init__(
            task_id=task_id,
            python_callable=self._run,
            execution_timeout=execution_timeout,
            **kwargs,
        )

    def _run(self):

        # Charger le parquet d'entrée
        df = helper.load_parquet_to_df(self.dag.dag_id, self._input_file, have_file_security=True)

        if "actual_departure" not in df.columns:
            raise AirflowFailException("❌ Colonne 'actual_departure' introuvable dans le DataFrame")

        # Filtrer en deux catégories
        df_raw = df[df["actual_departure"].notna()].copy()
        df_scheduled = df[df["actual_departure"].isna()].copy()

        logging.info(f"✅ Filtrage : {len(df_raw)} vols dans raw_flights, {len(df_scheduled)} vols dans scheduled_flights")

        # Sauvegarde des deux fichiers
        helper.generate_parquet_to_temp(
            self.dag.dag_id,
            df_raw,
            self._output_file_raw,
        )

        helper.generate_parquet_to_temp(
            self.dag.dag_id,
            df_scheduled,
            self._output_file_scheduled,
        )
