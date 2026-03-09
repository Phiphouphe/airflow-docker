import logging
import time

import app.helper as helper

from datetime import timedelta
from sqlalchemy import text, inspect

from airflow.operators.python import PythonOperator
from airflow.models import Variable
from app.static.connector_db import ConnectorDb


class Parquet_to_snapshot2(PythonOperator):
    def __init__(
        self,
        input_parquet_file: str,
        table_name: str,
        schema: str = "raw",
        database_conn_id: str = "flight_dw_postgres",
        chunksize: int = 1000,
        mode: str = "raw",  # "raw" ou "scheduled"
        api_type: str = "airfrance",  # "openmeteo" ou "airfrance"
        task_id: str = "Parquet_to_snapshot2",
        **kwargs
    ):
        """
        Insère un fichier Parquet dans PostgreSQL avec suppression adaptée au mode.
        
        Arguments :
        - input_parquet_file : nom du fichier Parquet source
        - table_name : nom de la table cible
        - schema : schéma cible (par défaut "raw")
        - database_conn_id : identifiant de connexion Airflow (default "flight_dw_postgres")
        - chunksize : nombre de lignes par insertion
        - mode : "raw" = append avec suppression uniquement de la date du parquet
                 "scheduled" = suppression de toutes les dates <= date du parquet
        - api_type : "openmeteo" ou "airfrance"
        - task_id : identifiant de la tâche Airflow
        """
        self._input_parquet_file = input_parquet_file
        self._table_name = table_name
        self._schema = schema
        self._db_conn_id = database_conn_id
        self._chunksize = chunksize
        self._mode = mode
        self._api_type = api_type

        super().__init__(
            task_id=task_id,
            python_callable=self._run,
            execution_timeout=timedelta(minutes=10),
            **kwargs,
        )

    def _run(self, **context):
        self._start_time = time.time()

        # Charger le fichier Parquet
        df = helper.load_parquet_to_df(self.dag_id, self._input_parquet_file, have_file_security=True)

        if df.empty:
            logging.info(f"ℹ️ Le fichier {self._input_parquet_file} est vide pour {self._table_name.upper()}, rien à insérer.")
            return

        # Créer l'engine PostgreSQL
        engine = ConnectorDb.get_db_engine(self._db_conn_id)

        # Vérifier colonne DATE_PHOTO
        if 'date_photo' not in df.columns:
            raise ValueError("❌ La colonne 'date_photo' est requise dans le Parquet")

        # Vérifier qu'il n'y a qu'une date unique dans le parquet
        unique_dates = df['date_photo'].unique()
        logging.info(f"date_photo unique: {unique_dates}")
        if len(unique_dates) > 1:
            raise ValueError("❌ Le Parquet doit contenir une seule valeur unique pour DATE_PHOTO")
        date_photo = unique_dates[0]

        with engine.begin() as conn:
            try:
                inspector = inspect(engine)
                tables = inspector.get_table_names(schema=self._schema)

                if self._table_name in tables:
                    if self._api_type == "openmeteo":
                        for airport_iata in df['airport_iata'].unique():
                            try:
                                if self._mode == "scheduled":
                                    logging.info(f"🗑️ Suppression Open-Meteo scheduled pour {airport_iata} date <= {date_photo}")
                                    delete_query = text(f"""
                                        DELETE FROM "{self._schema}"."{self._table_name}"
                                        WHERE "date_photo" <= :date_photo
                                        AND "airport_iata" = :airport_iata
                                    """)
                                else:
                                    logging.info(f"ℹ️ Suppression Open-Meteo raw pour {airport_iata} date = {date_photo}")
                                    delete_query = text(f"""
                                        DELETE FROM "{self._schema}"."{self._table_name}"
                                        WHERE "date_photo" = :date_photo
                                        AND "airport_iata" = :airport_iata
                                    """)
                                conn.execute(delete_query, {"date_photo": date_photo, "airport_iata": airport_iata})
                            except Exception as e:
                                logging.error(f"❌ Erreur suppression Open-Meteo pour {airport_iata}: {e}")

                    else:  # airfrance
                        for origin_airport in df['origin_airport'].unique():
                            try:
                                if self._mode == "scheduled":
                                    logging.info(f"🗑️ Air France scheduled : suppression anciennes données date <= {date_photo} pour {origin_airport}")
                                    delete_query = text(f"""
                                        DELETE FROM "{self._schema}"."{self._table_name}"
                                        WHERE "date_photo" <= :date_photo
                                        AND "origin_airport" = :origin_airport
                                    """)
                                else:
                                    logging.info(f"ℹ️ Air France raw : suppression ancienne date = {date_photo} pour {origin_airport}")
                                    delete_query = text(f"""
                                        DELETE FROM "{self._schema}"."{self._table_name}"
                                        WHERE "date_photo" = :date_photo
                                        AND "origin_airport" = :origin_airport
                                    """)
                                conn.execute(delete_query, {"date_photo": date_photo, "origin_airport": origin_airport})
                            except Exception as e:
                                logging.error(f"❌ Erreur suppression Air France pour {origin_airport}: {e}")
                else:
                    logging.warning(f"⚠️ La table {self._schema}.{self._table_name} n'existe pas. Pas de suppression.")

                # Insertion par chunks
                total_rows = len(df)
                logging.info(f"📥 Insertion de {total_rows} lignes dans {self._schema}.{self._table_name}...")
                for start in range(0, total_rows, self._chunksize):
                    end = min(start + self._chunksize, total_rows)
                    chunk_df = df.iloc[start:end]
                    try:
                        chunk_df.to_sql(
                            name=self._table_name,
                            con=engine,
                            schema=self._schema,
                            if_exists="append",
                            index=False,
                            method="multi",
                        )
                        logging.info(f"✅ Insertion lignes {start+1}-{end} terminée")
                    except Exception as e:
                        logging.error(f"❌ Erreur insertion lignes {start+1}-{end}: {e}")

            except Exception as e:
                logging.error(f"❌ Erreur globale dans Parquet_to_snapshot2: {e}")
                raise

        # Sauvegarder date_photo dans une Variable Airflow pour les DAGs déclenchés par asset
        # Permet aux DAGs suivants de filtrer les données par date_photo sans dépendre du contexte Airflow
        try:
            Variable.set(f"date_photo_{self._table_name}", str(date_photo))
            logging.info(f"📤 Variable Airflow mise à jour : date_photo_{self._table_name} = {date_photo}")
        except Exception as e:
            logging.warning(f"⚠️ Impossible de mettre à jour la Variable Airflow : {e}")

        # Temps total
        total_time = time.time() - self._start_time
        logging.info(f"⏱️ Temps total d'exécution: {total_time:.2f}s")
        logging.info(f"✅ Données insérées avec succès pour DATE_PHOTO={date_photo}, mode={self._mode}, api_type={self._api_type}")