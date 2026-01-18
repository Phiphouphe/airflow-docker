import logging
import requests
import pandas as pd

from datetime import datetime
from airflow.exceptions import AirflowFailException
from airflow.providers.common.python.operators.python import PythonOperator

import app.helper as helper

class API_extraction(PythonOperator):

    def __init__(
        self, 
        url_api: str,
        headers: dict,
        params: dict,
        output_csv_file : str,
        transform_function: callable = None,
        execution_timeout: timedelta = timedelta(seconds=30),
        task_id: str = "API_extraction",
        **kwargs
    ) -> None:
        
        self.url_api = url_api
        self.headers = headers
        self.params = params
        self.transform_function = transform_function
        self.output_file = output_csv_file
        
        super().__init__(
            task_id=task_id,
            python_callable=self._run,
            execute_timeout=execution_timeout,
            **kwargs
        )

    def _run(self):

        # Extraction des données via l'API
        try:
            df = helper.fetch_api_to_df(
                url_api=self.url_api,
                headers=self.headers,
                params=self.params,
                transform_function=self.transform_function
            )
        except AirflowFailException as e:
            logging.error(f"❌ Erreur lors de l'extraction API : {e}")
            raise

        # Sauvegarde du fichier (généralement vers un fichier temporaire)
        file_format = "csv" if self.output_file.endswith(".csv") else "parquet"
        file_name = self.output_file.rsplit(".", 1)[0]

        helper.generate_parquet_csv_to_temp(
            self.dag.dag_id,
            df,
            file_name,
            file_format,
        )