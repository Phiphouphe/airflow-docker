import logging
import pandas as pd

import app.helper as helper

from datetime import datetime, timedelta

from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator

class DateConverter(PythonOperator):

    def __init__(
        self,
        input_file: str,
        output_file: str,
        timestamp_columns: list = None,
        date_columns: list = None,
        execution_timeout: timedelta = timedelta(seconds=30),
        task_id: str = "DateConverter",
        **kwargs,
    ):
        """
        Convertit les colonnes de type date d'un dataframe en format datetime et génère un fichier de destination.

        Arguments :
        - input_file (str) : Nom du fichier source.
        - output_file (str) : Nom du fichier de destination.
        - execution_timeout (timedelta, optionnel) : Durée maximale d’exécution de la tâche Airflow. Par défaut 30 secondes.
        - task_id (str, optionnel) : Identifiant de la tâche Airflow. Par défaut "DateConverter".
        """
        self._input_file = input_file
        self._output_file = output_file
        self._timestamp_columns = timestamp_columns or []
        self._date_columns = date_columns or []
        
        super().__init__(
            task_id=task_id,
            python_callable=self._run,
            execution_timeout=execution_timeout,
            **kwargs,
        )

    def _run(self):

        # Charger les données depuis le fichier source
        df = helper.load_parquet_to_df(self.dag.dag_id, self._input_file)

        # Voir les lignes avant transformation
        print("Type avant transformation", type(df))
        print("Lignes avant transformation", df.head(5))

        # Conversion des dates 
        try:
            # --- TIMESTAMP AVEC OFFSET → UTC ---
            for col in self._timestamp_columns:
                if col not in df.columns:
                    logging.warning(f"Colonne '{col}' introuvable.")
                    continue

                before = df[col].dtype

                df[col] = pd.to_datetime(
                    df[col],
                    errors="coerce",
                    utc=True)

                logging.info(f"[TIMESTAMP] {col} | {before} -> {df[col].dtype}")

            # --- DATE CALENDRIER ---
            for col in self._date_columns:
                if col not in df.columns:
                    logging.warning(f"Colonne '{col}' introuvable.")
                    continue

                before = df[col].dtype

                df[col] = pd.to_datetime(
                    df[col],
                    errors="coerce").dt.date  # garde uniquement la date

                logging.info(f"[DATE] {col} | {before} -> {df[col].dtype}")

        except Exception as e:
            raise AirflowFailException(f"Erreur lors de la conversion des dates : {e}")

        # Sauvegarde du fichier (généralement vers un fichier temporaire)
        helper.generate_parquet_to_temp(
            self.dag.dag_id,
            df,
            self._output_file,
        )