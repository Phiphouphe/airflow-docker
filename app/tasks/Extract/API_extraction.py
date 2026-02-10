import logging
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
        flight_mode: str = "scheduled",
        execution_timeout: timedelta = timedelta(seconds=60),
        task_id: str = "API_extraction",
        **kwargs
    ) -> None:
        """
        Extrait des données depuis une API externe et génère un fichier temporaire au format Parquet.

        Arguments :
        - url_api (str) : URL de l’API à interroger.
        - headers (dict) : Headers HTTP à inclure dans la requête.
        - api_params (dict) : Paramètres de requête envoyés à l’API.
        - output_parquet_file (str) : Nom du fichier de sortie.
        - transform_function (callable, optionnel) : Fonction appliquée aux données brutes avant conversion en DataFrame. Par défaut None.
        - flight_mode (str, optionnel) : Mode de vol pour définir les plages de temps de l'API. "scheduled" pour les vols du jour même, "raw" pour les vols du jour précédent. Par défaut "scheduled".
        - execution_timeout (timedelta, optionnel) : Durée maximale d’exécution de la tâche Airflow. Par défaut 60 secondes.
        - task_id (str, optionnel) : Identifiant de la tâche Airflow. Par défaut "API_extraction".
        """
        self._url_api = url_api
        self._headers = headers
        self._api_params = api_params
        self._transform_function = transform_function
        self._output_file = output_parquet_file
        self._flight_mode = flight_mode
        
        super().__init__(
            task_id=task_id,
            python_callable=self._run,
            execution_timeout=execution_timeout,
            **kwargs,
        )

    def _run(self, **context):

        # Récupération de la date d'exécution pour définir les plages de temps de l'API
        logical_date = context['logical_date']
        
        # Si flight_mode = raw, récupérer J-1, sinon J
        if self._flight_mode == "raw":
            date_ref = logical_date - timedelta(days=1)
        else:
            date_ref = logical_date
        
        start = date_ref.replace(hour=0, minute=0, second=0, microsecond=0)
        end = date_ref.replace(hour=23, minute=59, second=59, microsecond=0)
        
        self._api_params["startRange"] = start.isoformat()
        self._api_params["endRange"] = end.isoformat()

        # Extraction des données via l'API
        try:
            df = helper.fetch_api_to_df(
                url_api=self._url_api,
                headers=self._headers,
                params=self._api_params,
                transform_function=self._transform_function
            )
        except AirflowFailException as e:
            logging.error(f"❌ Erreur lors de l'extraction API : {e}")
            raise

        # Retirer l'extension du fichier de sortie pour générer le nom de fichier temporaire
        file_name = self._output_file.rsplit(".", 1)[0]

        # Sauvegarde du fichier (généralement vers un fichier temporaire)
        helper.generate_parquet_to_temp(
            self.dag.dag_id,
            df,
            file_name,
        )