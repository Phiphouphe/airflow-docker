import logging
import time
import pendulum
import pandas as pd

import app.helper as helper

from datetime import datetime, timedelta
from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator

from app.static.connector_db import ConnectorDb


class Load_to_database(PythonOperator):
    
    def __init__(
        self,
        table_name: str,
        input_parquet_file: str,
        database_conn_id: str = "postgres_api",
        if_exists: str = "append",
        have_chunksize: int = 1000,
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
        - if_exists (str, optionnel) : Comportement si la table existe déjà ("fail", "replace", "append"). Par défaut "append".
        - have_chunksize (int, optionnel) : Nombre de lignes à insérer par lot. Par défaut 1000.
        - execution_timeout (timedelta, optionnel) : Durée maximale d’exécution de la tâche Airflow. Par défaut 5 minutes.
        - task_id (str, optionnel) : Identifiant de la tâche Airflow. Par défaut "Load_to_database".
        """
        self._table_name = table_name
        self._input_file = input_parquet_file
        self._db_conn_id = database_conn_id
        self._if_exists = if_exists
        self._have_chunksize = have_chunksize
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
        
        # Récupérer le moteur de connexion à la base de données
        engine = ConnectorDb.get_db_engine(self._db_conn_id)

        # Créer les colonnes techniques si elles n'existent pas
        now_utc = pendulum.now("UTC").naive()  # Converti en datetime standard
        df['created_date'] = now_utc
        df['updated_date'] = now_utc

        df['week_photo'] = df['created_date'].apply(lambda x: pendulum.instance(x).week_of_year)
        df['year_photo'] = df['created_date'].apply(lambda x: pendulum.instance(x).year)

        # Charger les données dans la table PostgreSQL
        try:
            df.to_sql(
                self._table_name, 
                engine, 
                if_exists=self._if_exists, 
                index=False,
                method="multi",
                chunksize=self._have_chunksize,
                )
            logging.info(f"Chargement de {len(df)} lignes dans la table {self._table_name} réussi.")
        except Exception as e:
            logging.error(f"Erreur lors du chargement des données dans la table {self._table_name} : {e}")
            raise AirflowFailException(f"Échec du chargement des données dans la table {self._table_name}.")
        
        # Marquer la fin de l'exécution
        self._end_time = time.time()
        logging.info(f"Tâche {self._task_id} exécutée en {self._end_time - self._start_time:.2f} secondes.")
