import logging
import requests
import pandas as pd

from datetime import datetime, timedelta

from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator

import app.helper as helper

class API_transform_data(PythonOperator):

    def __init__(
        self,
        input_parquet_file: str,
        output_parquet_file : str,
        transform_function: callable,
        execution_timeout: timedelta = timedelta(seconds=30),
        task_id: str = "API_transform_data",
        **kwargs,
    ):
        """
        Transforme les données extraites depuis une API externe et génère un fichier temporaire au format Parquet.

        Arguments :
        - input_parquet_file (str) : Nom du fichier Parquet d'entrée.
        - output_parquet_file (str) : Nom du fichier Parquet de sortie.
        - transform_function (callable) : Fonction appliquée aux données brutes avant conversion en DataFrame.
        - execution_timeout (timedelta, optionnel) : Durée maximale d’exécution de la tâche Airflow. Par défaut 30 secondes.
        - task_id (str, optionnel) : Identifiant de la tâche Airflow. Par défaut "API_transform_data".
        """
        self.input_file = input_parquet_file
        self.output_file = output_parquet_file
        self.transform_function = transform_function
        
        super().__init__(
            task_id=task_id,
            python_callable=self._run,
            # execute_timeout=execution_timeout,
            **kwargs,
        )

    def _run(self):

        # Charger le parquet d'entrée
        df = helper.load_parquet_to_df(self.dag.dag_id, self.input_file, have_file_security=True)

        # Transformation des données (à partir de la fonction fournie)
        try:
            df = self.transform_function(df)
            logging.info(f"✅ Transformation appliquée : {len(df)} lignes conservées.")
            logging.info(f"✅ Transformation appliquée avec succès.")
        except Exception as e:
            raise AirflowFailException(f"❌ Erreur lors de l'application de la transformation : {e}")
           
        # Sauvegarde du fichier (généralement vers un fichier temporaire)
        helper.generate_parquet_to_temp(
            self.dag.dag_id,
            df,
            self.output_file,
        )