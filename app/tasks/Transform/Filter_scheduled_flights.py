import logging
import pandas as pd

from datetime import timedelta

from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator

import app.helper as helper


class Filter_scheduled_flights(PythonOperator):

    def __init__(
        self,
        input_parquet_file: str,
        output_parquet_file: str = "scheduled_flights",
        execution_timeout: timedelta = timedelta(seconds=30),
        task_id: str = "filter_scheduled_flights",
        **kwargs,
    ):
        """
        Filtre les données d'un fichier Parquet pour ne garder que les vols non arrivés (actual_arrival IS NULL).

        Arguments :
        - input_parquet_file (str) : Nom du fichier Parquet d'entrée.
        - output_parquet_file (str) : Nom du fichier Parquet pour les vols filtrés. Par défaut "scheduled_flights".
        - execution_timeout (timedelta, optionnel) : Durée maximale d'exécution. Par défaut 30 secondes.
        - task_id (str, optionnel) : Identifiant de la tâche Airflow. Par défaut "filter_scheduled_flights".
        """
        self._input_file = input_parquet_file
        self._output_file = output_parquet_file

        super().__init__(
            task_id=task_id,
            python_callable=self._run,
            execution_timeout=execution_timeout,
            **kwargs,
        )

    def _run(self):

        # Charger le parquet d'entrée
        df = helper.load_parquet_to_df(self.dag.dag_id, self._input_file, have_file_security=True)

        if "actual_arrival" not in df.columns:
            raise AirflowFailException("❌ Colonne 'actual_arrival' introuvable dans le DataFrame")
        
        # DEBUG : Vérifie ce que pandas voit dans actual_arrival
        # print("✅ DEBUG", df[["flight_number", "actual_arrival"]])

        # Filtrer en deux catégories
        df_filter = df[df["actual_arrival"].isna()].copy()

        # DEBUG : Vérifie ce que pandas voit dans actual_arrival
        # print("✅ DEBUG", df_filter[["flight_number", "actual_arrival"]])

        logging.info(f"✅ Filtrage : {len(df_filter)} vols dans scheduled_flights")

        # Sauvegarde des deux fichiers
        helper.generate_parquet_to_temp(
            self.dag.dag_id,
            df_filter,
            self._output_file,
            empty_security=False,
        )
