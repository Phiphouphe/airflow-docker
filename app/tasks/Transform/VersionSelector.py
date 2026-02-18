import logging
import pandas as pd

import app.helper as helper

from datetime import timedelta

from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator



class VersionSelector(PythonOperator):

    def __init__(
        self,
        input_file: str,
        output_file: str,
        key_columns: list,
        date_column: str,
        execution_timeout: timedelta = timedelta(seconds=30),
        task_id: str = "VersionSelector",
        **kwargs,
    ):
        """
        Garde, pour chaque objet métier défini par `key_columns`, la version la plus récente 
        selon une colonne de référence temporelle (ex: `date_photo`) et génère un fichier Parquet de sortie.

        Arguments :
        - input_file (str) : Fichier Parquet source à traiter.
        - output_file (str) : Fichier Parquet de sortie contenant uniquement les dernières versions.
        - key_columns (list) : Colonnes identifiant un objet métier unique.
        - date_column (str) : Colonne qui détermine la version la plus récente (ex: date_photo).
        - execution_timeout (timedelta, optionnel) : Durée maximale d’exécution de la tâche Airflow. Par défaut 30 secondes.
        - task_id (str, optionnel) : Identifiant de la tâche Airflow. Par défaut "VersionSelector".
        - **kwargs : Arguments supplémentaires transmis au PythonOperator.
        """
        self._input_file = input_file
        self._output_file = output_file
        self._key_columns = key_columns
        self._date_column = date_column

        super().__init__(
            task_id=task_id,
            python_callable=self._run,
            execution_timeout=execution_timeout,
            **kwargs,
        )

    def _run(self):
        
        # Charger le DataFrame
        df = helper.load_parquet_to_df(self.dag.dag_id, self._input_file)
        logging.info(f"Aperçu avant sélection des versions :\n{df.head(5)}")

        try:
            if self._date_column not in df.columns:
                raise AirflowFailException(f"Colonne '{self._date_column}' introuvable.")

            # Trier par date croissante et garder la dernière ligne par clé
            df_sorted = df.sort_values(self._date_column)
            df_latest = df_sorted.drop_duplicates(subset=self._key_columns, keep="last")

            logging.info(
                f"Version la plus récente sélectionnée pour chaque objet métier "
                f"selon {self._key_columns} et {self._date_column}"
            )

        except Exception as e:
            raise AirflowFailException(f"Erreur lors de la sélection des versions : {e}")

        # Sauvegarde du DataFrame dans dossier temporaire
        helper.generate_parquet_to_temp(
            self.dag.dag_id,
            df_latest,
            self._output_file,
        )
