import logging

import app.helper as helper

from datetime import timedelta

from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator


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
        **kwargs,
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
        self._url_api = url_api
        self._headers = headers
        self._api_params = api_params
        self._transform_function = transform_function
        self._output_file = output_parquet_file
        
        super().__init__(
            task_id=task_id,
            python_callable=self._run,
            execution_timeout=execution_timeout,
            **kwargs,
        )

    def _run(self, **context):
            
        # 1. Récupère la date logique du run
        execution_date = context['execution_date']

        # 2. Récupère le créneau horaire (8, 14, 20) depuis les paramètres de la task
        hour = self._api_params.pop("schedule_hour")

        # 3. Calcule le début et la fin du créneau en fonction du jour du run
        startRange, endRange = self.get_api_time_range(hour, execution_date)

        # 4. Mets ces valeurs dans api_params pour l'appel à l'API
        self._api_params["startRange"] = startRange
        self._api_params["endRange"] = endRange

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
        logging.info(f"Nom du fichier temporaire généré : {file_name}")

        # Sauvegarde du fichier (généralement vers un fichier temporaire)
        helper.generate_parquet_to_temp(
            self.dag.dag_id,
            df,
            file_name,
        )

    def get_api_time_range(self, schedule_hour, execution_date):
        base_date = execution_date.start_of('day')

        if schedule_hour == 8:
            start = base_date.add(hours=8)
            end = base_date.add(hours=13, minutes=59, seconds=59)
        elif schedule_hour == 14:
            start = base_date.add(hours=14)
            end = base_date.add(hours=19, minutes=59, seconds=59)
        elif schedule_hour == 20:
            start = base_date.add(hours=20)
            end = base_date.add(days=1, hours=7, minutes=59, seconds=59)
        else:
            raise ValueError("Schedule hour non supporté")

        return start.to_iso8601_string(), end.to_iso8601_string()