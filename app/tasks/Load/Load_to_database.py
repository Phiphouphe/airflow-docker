import logging
import requests
import time
import pandas as pd

from datetime import datetime, timedelta

from airflow.exceptions import AirflowFailException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator

import app.helper as helper

class Load_to_database(PythonOperator):
    
    def __init__(
        self,
        table_name: str,
        input_parquet_file: str,
        execution_timeout: timedelta = timedelta(minutes=5),
        task_id: str = "Load_to_Postgres",
    ):
        """
        Charge les données depuis un fichier Parquet vers une table PostgreSQL.

        Arguments :
        - table_name (str) : Nom de la table PostgreSQL cible.
        - input_parquet_file (str) : Chemin du fichier Parquet source.
        - execution_timeout (timedelta, optionnel) : Durée maximale d’exécution de la tâche Airflow. Par défaut 5 minutes.
        - task_id (str, optionnel) : Identifiant de la tâche Airflow. Par défaut "Load_to_Postgres".
        """
        self.table_name = table_name
        self.input_file = input_parquet_file
        
        super().__init__(
            task_id=task_id,
            python_callable=self._run,
            execute_timeout=execution_timeout,
            **kwargs,
        )

    def _run(self):
        # Marquer le début de l'exécution
        self._start_time = time.time()

        # Charger le fichier Parquet
        df = helper.load_parquet_to_df(self.dag_id, self.__input_csv_file, have_empty_security=True)

        if df.empty:
            raise AirflowFailException(f"Le fichier {self.input_file} est vide, rien à charger dans {self.table_name}.")
