import logging

from sqlalchemy.engine import Engine
from airflow.datasets import Dataset
from app.static.connector_db import ConnectorDb

def get_postgres_dataset(conn_id: str, table: str, schema: str = "raw") -> Dataset:
    """ Génère un Dataset Airflow au format postgres://database.schema.table
    à partir d'un SQLAlchemy Engine utilisant postgresql+psycopg2.

    Args:
        conn_id: Identifiant de la connexion à la base de données dans Airflow.
        table (str): Nom de la table.
        schema (str, optional): Nom du schéma. Defaults to "raw".

    Returns:
        Dataset: Instance de Dataset Airflow.
    """
    try:
        # Récupération de l'engine via ConnectorDb
        engine: Engine = ConnectorDb.get_db_engine(conn_id)
        url = engine.url

        host = url.host
        database = url.database

        if not host or not database:
            raise ValueError("Host ou database manquant dans la connexion")

        dataset_uri = f"postgres://{host}/{database}/{schema}/{table}"
        logging.info(f"Dataset PostgreSQL créé : {dataset_uri}")

        return Dataset(dataset_uri)

    except Exception as e:
        raise ValueError(f"Erreur lors de la génération du Dataset PostgreSQL: {e}")