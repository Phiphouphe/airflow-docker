import logging
import pandas as pd

import app.helper as helper

from datetime import datetime, timedelta

from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator

class DuplicateRemover(PythonOperator):

    def __init__(
        self,
        input_file: str,
        output_file: str,
        key_columns: list,
        keep: str = "last", 
        null_threshold_percent: float = 50.0,
        execution_timeout: timedelta = timedelta(seconds=30),
        task_id: str = "DuplicateRemover",
        **kwargs,
    ):
        """
        Supprime les doublons d'un DataFrame en fonction de colonnes clés et génère un fichier Parquet de sortie.

        Arguments :
        - input_file (str) : Nom du fichier source Parquet à traiter.
        - output_file (str) : Nom du fichier Parquet de sortie après suppression des doublons.
        - key_columns (list) : Liste des colonnes utilisées pour identifier les doublons.
        - keep (str, optionnel) : Stratégie pour conserver les doublons. Par défaut "last": conserve la dernière occurrence
        - execution_timeout (timedelta, optionnel) : Durée maximale d’exécution de la tâche Airflow. Par défaut 30 secondes.
        - task_id (str, optionnel) : Identifiant de la tâche Airflow. Par défaut "DuplicateRemover".
        """

        self._input_file = input_file
        self._output_file = output_file
        self._key_columns = key_columns
        self._keep = keep
        self._threshold_percent = null_threshold_percent

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

        # Suppression des doublons
        try:
            before_count = len(df)
            df = df.drop_duplicates(subset=self._key_columns, keep=self._keep)
            after_count = len(df)

            logging.info(f"Suppression des doublons terminée : {before_count - after_count} doublons supprimés.")

        except Exception as e:
            raise AirflowFailException(f"Erreur lors de la suppression des doublons : {e}")

        # ✅ Contrôle qualité
        helper.check_nulls(
            df,
            columns=None,
            threshold_percent=self._threshold_percent,
        )

        # Sauvegarde du fichier (généralement vers un fichier temporaire)
        helper.generate_parquet_to_temp(
            self.dag.dag_id,
            df,
            self._output_file,
        )