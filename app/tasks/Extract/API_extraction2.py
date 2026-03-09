import logging
from datetime import timedelta

import app.helper as helper

from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator

class API_extraction2(PythonOperator):
    
    def __init__(
        self, 
        url_api: str,
        headers: dict = None,
        api_params: dict = None,
        output_parquet_file: str = None,
        transform_function: callable = None,
        flight_mode: str = "raw",
        api_type: str = "airfrance",  # "airfrance" ou "openmeteo"
        execution_timeout: timedelta = timedelta(seconds=240),
        task_id: str = "API_extraction2",
        **kwargs
    ) -> None:
        """
        API_extraction2 générique pour Air France ou Open-Meteo.

        Arguments :
        - url_api : URL de l’API
        - headers : headers HTTP
        - api_params : params de l'API
        - output_parquet_file : fichier de sortie
        - transform_function : fonction pour transformer les données brutes
        - flight_mode : "raw" = hier, "scheduled" = aujourd'hui
        - api_type : "airfrance" ou "openmeteo"
        """
        self._url_api = url_api
        self._headers = headers or {}
        self._api_params = api_params or {}
        self._transform_function = transform_function
        self._output_file = output_parquet_file
        self._flight_mode = flight_mode
        self._api_type = api_type
        
        super().__init__(
            task_id=task_id,
            python_callable=self._run,
            execution_timeout=execution_timeout,
            **kwargs,
        )

    def _run(self, **context):
        logical_date = context['logical_date']
        logging.info(f"logical_date: {logical_date}")

        # Calculer la date de référence selon flight_mode
        if self._flight_mode == "raw":
            date_ref = logical_date - timedelta(days=1)
        else:
            date_ref = logical_date

        start = date_ref.replace(hour=0, minute=0, second=0, microsecond=0)
        end = date_ref.replace(hour=23, minute=59, second=59, microsecond=0)

        # ⚠️ On clone les params pour éviter effet de bord
        api_params = self._api_params.copy()

        # Adapter les params selon le type d'API
        if self._api_type == "airfrance":

            api_params["startRange"] = start.isoformat()
            api_params["endRange"] = end.isoformat()

            # Affichage pour debug
            print(f"✈️ startRange: {api_params['startRange']}, endRange: {api_params['endRange']}")

        elif self._api_type == "openmeteo":

            api_params["start_date"] = start.date().isoformat()
            api_params["end_date"] = end.date().isoformat()

            # Affichage pour debug
            print(f"🌦️ start_date: {api_params['start_date']}, end_date: {api_params['end_date']}")

            if "daily" not in api_params:
                raise AirflowFailException("Pour Open-Meteo, il faut passer la liste daily dans api_params['daily']")
            
            api_params.setdefault("timezone", "Europe/Paris")
        
        else:
            raise AirflowFailException(f"Type d'API inconnu : {self._api_type}")

        logging.info(f"API params: {api_params}")

        # Extraction des données via l'API
        try:
            df_or_dict = helper.fetch_api_to_df2(
                url_api=self._url_api,
                headers=self._headers,
                params=api_params,
                transform_function=self._transform_function,
                api_type=self._api_type,
            )
        except AirflowFailException as e:
            logging.error(f"❌ Erreur lors de l'extraction API : {e}")
            raise

        # Gestion des fichiers de sortie
        if isinstance(df_or_dict, dict):
            for key, df in df_or_dict.items():
                helper.generate_parquet_to_temp(
                    self.dag.dag_id,
                    df,
                    f"{self._output_file}_{key}"
                )
        else:
            helper.generate_parquet_to_temp(
                self.dag.dag_id,
                df_or_dict,
                self._output_file
            )
