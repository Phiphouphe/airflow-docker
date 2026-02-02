import logging
import requests
import time
import pendulum
import pandas as pd

from datetime import datetime, timedelta

from sqlalchemy import create_engine
from airflow.exceptions import AirflowFailException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator

import app.helper as helper

class Load_to_database(PythonOperator):
    
    def __init__(
        self,
        table_name: str,
        input_parquet_file: str,
        database_conn_id: str = "postgres_api",
        execution_timeout: timedelta = timedelta(minutes=5),
        task_id: str = "Load_to_database",
        **kwargs,
    ):
        """
        Charge les données depuis un fichier Parquet vers une table PostgreSQL.

        Arguments :
        - table_name (str) : Nom de la table PostgreSQL cible.
        - input_parquet_file (str) : Chemin du fichier Parquet source.
        - database_conn_id (str, optionnel) : Identifiant de connexion Airflow pour la base de données PostgreSQL. Par défaut "postgres_api".
        - execution_timeout (timedelta, optionnel) : Durée maximale d’exécution de la tâche Airflow. Par défaut 5 minutes.
        - task_id (str, optionnel) : Identifiant de la tâche Airflow. Par défaut "Load_to_database".
        """
        self._table_name = table_name
        self._input_file = input_parquet_file
        self._db_conn_id = database_conn_id
        self._task_id = task_id
        
        super().__init__(
            task_id=task_id,
            python_callable=self._run,
            execution_timeout=execution_timeout,
            **kwargs,
        )

    def _run(self):
        # Marquer le début de l'exécution
        self._start_time = time.time()

        # Charger le fichier Parquet
        df = helper.load_parquet_to_df(self.dag_id, self._input_file, have_file_security=True)

        if df.empty:
            raise AirflowFailException(f"Le fichier {self._input_file} est vide, rien à charger dans {self._table_name}.")
        
        # Connexion à la base de données PostgreSQL via Airflow Hook
        hook = PostgresHook(postgres_conn_id=self._db_conn_id)
        engine = create_engine(hook.get_uri())

        # Créer les colonnes techniques si elles n'existent pas
        now_utc = pendulum.now("UTC").naive()  # Converti en datetime standard
        df['created_date'] = now_utc
        df['updated_date'] = now_utc

        df['WEEK_PHOTO'] = df['created_date'].apply(lambda x: pendulum.instance(x).week_of_year)
        df['YEAR_PHOTO'] = df['created_date'].apply(lambda x: pendulum.instance(x).year)

        # Charger les données dans la table PostgreSQL
        try:
            df.to_sql(
                self._table_name, 
                engine, 
                if_exists='append', 
                index=False,
                method="multi",
                chunksize=1000,
                )
            logging.info(f"Chargement de {len(df)} lignes dans la table {self._table_name} réussi.")
        except Exception as e:
            logging.error(f"Erreur lors du chargement des données dans la table {self._table_name} : {e}")
            raise AirflowFailException(f"Échec du chargement des données dans la table {self._table_name}.")
        
        # Marquer la fin de l'exécution
        self._end_time = time.time()
        logging.info(f"Tâche {self._task_id} exécutée en {self._end_time - self._start_time:.2f} secondes.")
