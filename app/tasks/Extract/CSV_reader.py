import pandas as pd
import logging

import app.helper as helper

from datetime import timedelta

from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator



class CsvReader(PythonOperator):

    def __init__(
            self,
            csv_file_path: str,
            output_file: str,
            sep: str = ",",
            encoding: str = "utf-8",
            execution_timeout:timedelta = timedelta(seconds=30),
            task_id: str = "csv_reader",
            **kwargs,
       ):  
        """
        Lit un fichier CSV et génère un fichier temporaire au format Parquet.

        Arguments :
        - csv_file_path (str) : Chemin du fichier CSV à lire.
        - output_file (str) : Nom du fichier Parquet de sortie.
        - sep (str, optionnel) : Séparateur de colonnes du fichier CSV. Par défaut ",".
        - encoding (str, optionnel) : Encodage du fichier CSV. Par défaut "utf-8".
        - execution_timeout (timedelta, optionnel) : Durée maximale d’exécution de la tâche Airflow. Par défaut 30 secondes.
        - task_id (str, optionnel) : Identifiant de la tâche Airflow. Par défaut "csv_reader".
        """
    
        self._csv_file_path = csv_file_path
        self._output_file = output_file
        self._sep = sep
        self._encoding = encoding

        super().__init__(
            task_id=task_id,
            python_callable=self._run,
            execution_timeout=execution_timeout,
            **kwargs,
        )

    def _run(self):

        # Lecture du fichier CSV et conversion en DataFrame

        logging.info("🚀 Début lecture du fichier CSV")
        logging.info(f"📄 Chemin fichier : {self._csv_file_path}")
        logging.info(f"⚙️ Séparateur : '{self._sep}' | Encodage : {self._encoding}")

        try:
            df = pd.read_csv(self._csv_file_path, sep=self._sep, encoding=self._encoding)
        except Exception as e:
            logging.error(f"❌ Erreur lors de la lecture du fichier CSV : {e}")
            logging.exception(e)
            raise AirflowFailException("Impossible de lire le fichier CSV")  

        # Sauvegarde du fichier (généralement vers un fichier temporaire)
        helper.generate_parquet_to_temp(
            self.dag.dag_id,
            df,
            self._output_file,
        )