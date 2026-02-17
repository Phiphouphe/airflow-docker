import logging
import pandas as pd

from datetime import timedelta

from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator

import app.helper as helper


class Merge_files(PythonOperator):

    def __init__(
        self,
        input_parquet_file_1: str,
        input_parquet_file_2: str,
        output_parquet_file: str,
        execution_timeout: timedelta = timedelta(seconds=30),
        task_id: str = "merge_files",
        **kwargs,
    ):
        """
        Fusionne deux fichiers Parquet en un seul fichier Parquet.

        Arguments :
        - input_parquet_file_1 (str) : Nom du premier fichier Parquet d'entrée.
        - input_parquet_file_2 (str) : Nom du deuxième fichier Parquet d'entrée.
        - output_parquet_file (str) : Nom du fichier Parquet de sortie fusionné.
        - execution_timeout (timedelta, optionnel) : Durée maximale d'exécution. Par défaut 30 secondes.
        - task_id (str, optionnel) : Identifiant de la tâche Airflow. Par défaut "merge_files".
        """
        self._input_file_1 = input_parquet_file_1
        self._input_file_2 = input_parquet_file_2
        self._output_file = output_parquet_file

        super().__init__(
            task_id=task_id,
            python_callable=self._run,
            execution_timeout=execution_timeout,
            **kwargs,
        )

    def _run(self):

        # Charger les deux fichiers parquet
        df_1 = helper.load_parquet_to_df(self.dag.dag_id, self._input_file_1, have_file_security=True)
        df_2 = helper.load_parquet_to_df(self.dag.dag_id, self._input_file_2, have_file_security=True)

        logging.info(f"✅ Chargement : {len(df_1)} lignes (file 1), {len(df_2)} lignes (file 2)")

        # Fusionner les deux DataFrames
        df_merged = pd.concat([df_1, df_2], ignore_index=True)
        print("Extrait des premières lignes: ", df_merged.head())

        logging.info(f"✅ Fusion : {len(df_merged)} lignes au total")

        # Sauvegarde du fichier fusionné
        helper.generate_parquet_to_temp(
            self.dag.dag_id,
            df_merged,
            self._output_file,
        )
