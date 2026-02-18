import logging
import time

import app.helper as helper

from datetime import timedelta
from sqlalchemy import text, inspect

from airflow.operators.python import PythonOperator

from app.static.connector_db import ConnectorDb

class Parquet_to_snapshot(PythonOperator):

    def __init__(
        self,
        input_parquet_file: str,
        table_name: str,
        schema: str = "raw",
        database_conn_id: str = "flight_dw_postgres",
        chunksize: int = 1000,
        task_id : str = "Parquet_to_snapshot",
        **kwargs
    ):
        """Insère un fichier Parquet dans PostgreSQL avec suppression uniquement du jour courant.
        
           Arguments :
           - input_parquet_file (str) : Nom du fichier Parquet d'entrée.
           - table_name (str) : Nom de la table cible dans PostgreSQL.
           - schema (str, optionnel) : Schéma de la table cible. Par défaut "raw".
           - database_conn_id (str, optionnel) : Identifiant de la connexion à la base de données dans Airflow. Par défaut "flight_dw_postgres". 
           - chunksize (int, optionnel) : Nombre de lignes à insérer par chunk pour éviter les problèmes de mémoire. Par défaut 1000. 
           - task_id (str, optionnel) : Identifiant de la tâche Airflow. Par défaut "Parquet_to_snapshot".   
        """
        self._input_parquet_file = input_parquet_file
        self._table_name = table_name
        self._chunksize = chunksize
        self._schema = schema
        self._db_conn_id = database_conn_id
        
        # Générer le Dataset Airflow pour les outlets
        outlets = [helper.get_postgres_dataset(self._db_conn_id, self._table_name, self._schema)]
        
        super().__init__(
            task_id=task_id,
            python_callable=self._run,
            execution_timeout=timedelta(minutes=10),
            outlets=outlets,
            **kwargs
        )

    def _run(self, **context):
        self._start_time = time.time()

        # Charger le fichier Parquet
        df = helper.load_parquet_to_df(self.dag_id, self._input_parquet_file, have_file_security=True)

        # Si le DataFrame est vide après chargement ou filtrage, on logue et on sort proprement
        if df.empty:
            logging.info(f"ℹ️ Le fichier {self._input_parquet_file} est vide pour {self._table_name.upper()}, rien à insérer. La task est considérée comme réussie.")
            return

        # Créer l'engine ici une seule fois
        engine = ConnectorDb.get_db_engine(self._db_conn_id)

        # Vérifier colonne DATE_PHOTO
        if 'date_photo' not in df.columns:
            raise ValueError("❌ La colonne 'date_photo' est requise dans le Parquet")

        # Récupérer les dates uniques
        unique_dates = df['date_photo'].unique()
        logging.info(f"date_photo unique: {unique_dates}")
        if len(unique_dates) > 1:
            raise ValueError("❌ Le Parquet doit contenir une seule valeur unique pour DATE_PHOTO")
        date_photo = unique_dates[0]

        with engine.begin() as conn:
            inspector = inspect(engine)
            tables = inspector.get_table_names(schema=self._schema)

            if self._table_name in tables:
                logging.info(f"🗑️ Suppression des anciennes données pour date_photo={date_photo}")
                delete_query = text(f"""
                    DELETE FROM "{self._schema}"."{self._table_name}"
                    WHERE "date_photo" = :date_photo
                """)
                conn.execute(delete_query, {"date_photo": date_photo})
            else:
                logging.warning(f"⚠️ La table {self._schema}.{self._table_name} n'existe pas. Pas de suppression.")

            # Insertion avec chunks
            total_rows = len(df)
            logging.info(f"📥 Insertion de {total_rows} lignes dans {self._schema}.{self._table_name}...")
            for start in range(0, total_rows, self._chunksize):
                end = min(start + self._chunksize, total_rows)
                chunk_df = df.iloc[start:end]
                chunk_df.to_sql(
                    name=self._table_name,
                    con=engine,
                    schema=self._schema,
                    if_exists="append",
                    index=False,
                    method="multi",
                )
                logging.info(f"✅ Insertion lignes {start+1}-{end} terminée")

        # Temps total
        total_time = time.time() - self._start_time
        logging.info(f"⏱️  Temps total d'exécution: {total_time:.2f}s")

        logging.info(f"✅ Données insérées avec succès pour DATE_PHOTO={date_photo}")
