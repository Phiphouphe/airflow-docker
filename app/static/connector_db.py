import logging

from sqlalchemy import create_engine
from airflow.providers.postgres.hooks.postgres import PostgresHook


class ConnectorDb:

    @staticmethod
    def get_db_engine(conn_id: str):
        logging.info("Création de l'engine PostgreSQL (conn_id=%s)", conn_id)

        hook = PostgresHook(postgres_conn_id=conn_id)
        engine = create_engine(hook.get_uri())

        logging.info("Engine PostgreSQL créé avec succès (conn_id=%s)", conn_id)
        
        return engine
