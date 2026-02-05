import logging
import pandas as pd

import app.helper as helper

from datetime import timedelta
from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator

from app.static.connector_db import ConnectorDb


class DB_extraction(PythonOperator):

    def __init__(
            self,
            table_name: str,
            schema_name: str,
            output_parquet_file: str,
            database_conn_id: str = "flight_dw_postgres",
            columns: list[str] | str = "*",   
            where_clause: str | None = None,          
            limit_clause: int | None = None,         
            query: str = None,
            execution_timeout: timedelta = timedelta(minutes=5),
            task_id: str = "DB_extraction",
            **kwargs,
        ):
        """
        Extrait des données depuis une base de données PostgreSQL et génère un fichier temporaire au format Parquet.
        
        Arguments :
        - table_name (str) : Nom de la table PostgreSQL source.
        - schema_name (str) : Nom du schéma PostgreSQL source.
        - output_parquet_file (str) : Nom du fichier de sortie.
        - database_conn_id (str, optionnel) : Identifiant de connexion Airflow pour la base de données PostgreSQL. Par défaut "postgres_api".
        - columns (list[str] | str, optionnel) : Colonnes à sélectionner. Par défaut "*".
        - where_clause (str | None, optionnel) : Clause WHERE SQL. Par défaut None.
        - limit_clause (int | None, optionnel) : Clause LIMIT SQL. Par défaut None.
        - query (str, optionnel) : Requête SQL personnalisée. Par défaut None. Si fournie, les autres paramètres sont ignorés.
        - execution_timeout (timedelta, optionnel) : Durée maximale d’exécution de la tâche Airflow. Par défaut 5 minutes.
        - task_id (str, optionnel) : Identifiant de la tâche Airflow. Par défaut "DB_extraction".
        """

        self._table_name = table_name
        self._schema_name = schema_name
        self._output_parquet_file = output_parquet_file
        self._db_conn_id = database_conn_id
        self._columns = columns
        self._where_clause = where_clause
        self._limit_clause = limit_clause
        self._query = query

        super().__init__(
            task_id=task_id,
            python_callable=self._run,
            execution_timeout=execution_timeout,
            **kwargs,
        )

    def _run(self):

        # Génération de la requête
        query = self._build_query()
        logging.info(f"Exécution de la requête SQL : {query}")

        # Récupération de l'engine SQLAlchemy
        engine = ConnectorDb.get_db_engine(self._db_conn_id)
        
        # Extraction des données depuis la base de données
        try:
            df = pd.read_sql(query, engine)
        except Exception as e:
            logging.error(f"Erreur lors de l'extraction depuis la base de données : {e}")
            raise AirflowFailException("Erreur lors de l'extraction depuis la base de données")

        # Debug: vérifier les colonnes et un aperçu des données
        logging.info("Colonnes récupérées : %s", df.columns.tolist())
        logging.info("Aperçu des données (head) :\n%s", df.head(10).to_string())

        # Nom du fichier parquet temporaire
        file_name = self._output_parquet_file.rsplit(".", 1)[0]
        logging.info(f"Nom du fichier temporaire généré : {file_name}")

        # Sauvegarde du fichier (généralement vers un fichier temporaire)
        helper.generate_parquet_to_temp(
            self.dag.dag_id,
            df,
            file_name,
        )

    def _build_query(self) -> str:
        """Construit dynamiquement la requête si self._query est None."""
        if self._query:
            return self._query
        
        # Normaliser les colonnes en minuscules pour Postgres
        if isinstance(self._columns, list):
            columns_part = ", ".join([c.strip() for c in self._columns])
        else:
            columns_part = self._columns.strip()
        
        query = f"SELECT {columns_part} FROM {self._schema_name}.{self._table_name}"

        if self._where_clause:
            query += f" WHERE {self._where_clause}"

        if self._limit_clause:
            query += f" LIMIT {self._limit_clause}"

        return query
    