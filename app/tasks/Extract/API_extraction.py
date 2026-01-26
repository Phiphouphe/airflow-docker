import logging
import requests
import pandas as pd

from datetime import datetime, timedelta

from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator

import app.helper as helper

class API_extraction(PythonOperator):

    def __init__(
        self, 
        url_api: str,
        headers: dict,
        api_params: dict,
        output_parquet_file : str,
        transform_function: callable = None,
        execution_timeout: timedelta = timedelta(seconds=30),
        task_id: str = "API_extraction",
        **kwargs
    ) -> None:
        """
        Extrait des données depuis une API externe et génère un fichier temporaire au format CSV ou Parquet.

        Arguments :
        - url_api (str) : URL de l’API à interroger.
        - headers (dict) : Headers HTTP à inclure dans la requête.
        - api_params (dict) : Paramètres de requête envoyés à l’API.
        - output_parquet_file (str) : Nom du fichier de sortie.
        - transform_function (callable, optionnel) : Fonction appliquée aux données brutes avant conversion en DataFrame. Par défaut None.
        - execution_timeout (timedelta, optionnel) : Durée maximale d’exécution de la tâche Airflow. Par défaut 30 secondes.
        - task_id (str, optionnel) : Identifiant de la tâche Airflow. Par défaut "API_extraction".
        """
        self.url_api = url_api
        self.headers = headers
        self.api_params = api_params
        self.transform_function = transform_function
        self.output_file = output_parquet_file
        
        super().__init__(
            task_id=task_id,
            python_callable=self._run,
            # execute_timeout=execution_timeout,
            **kwargs,
        )

    def _run(self):

        # Extraction des données via l'API
        try:
            df = helper.fetch_api_to_df(
                url_api=self.url_api,
                headers=self.headers,
                params=self.api_params,
                transform_function=self.transform_function
            )
        except AirflowFailException as e:
            logging.error(f"❌ Erreur lors de l'extraction API : {e}")
            raise

        # Retirer l'extension du fichier de sortie pour générer le nom de fichier temporaire
        file_name = self.output_file.rsplit(".", 1)[0]

        # Sauvegarde du fichier (généralement vers un fichier temporaire)
        helper.generate_parquet_to_temp(
            self.dag.dag_id,
            df,
            file_name,
        )