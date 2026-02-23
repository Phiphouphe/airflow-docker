import pandas as pd
import logging
import pendulum

from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

import app.helper as helper


class TechnicalInfo(PythonOperator):

    def __init__(
        self,
        task_id: str = "add_technical_info",
        input_file: str = None, 
        date_column: str = "date_photo",  
        year_column: str = "annee_photo",
        week_column: str = "semaine_photo",
        instance_column: str = "instance_id",  
        execution_date_column: str = "execution_date",
        output_file: str = None,
        **kwargs,
    ) -> None:
        """Tâche Airflow permettant d'ajouter des informations techniques (date d'exécution et identifiant d'instance) à un fichier Parquet ou CSV.

        Args:
            task_id (str, optional): Identifiant unique pour la tâche dans le DAG. Defaults to "add_technical_info".
            input_file (str, optional): Chemin du fichier cible. Defaults to None.
            date_column (str, optional): Nom de la colonne pour la date d'exécution. Defaults to "date_photo".
            year_column (str, optional): Nom de la colonne pour l'année d'exécution. Defaults to "annee_photo".
            week_column (str, optional): Nom de la colonne pour la semaine d'exécution. Defaults to "semaine_photo".
            instance_column (str, optional): Nom de la colonne pour l'identifiant d'instance. Defaults to "instance_id".
            execution_date_column (str, optional): Nom de la colonne pour la date d'éxecution du DAG. Defaults to "execution_date".
            output_file (str, optional): Chemin du fichier de sortie. Defaults to None.
        """

        self.__input_file = input_file
        self.__date_column = date_column.lower()
        self.__year_column = year_column.lower()
        self.__week_column = week_column.lower()
        self.__instance_column = instance_column
        self.__output_file = output_file
        self.__execution_date_column = execution_date_column

        super().__init__(
            task_id=task_id,
            python_callable=self._run,
            execution_timeout=timedelta(seconds=30),
            **kwargs,
        )

    def _run(self, **context):
        """Ajoute les informations techniques au Parquet et génère le fichier de sortie."""

        df = helper.load_parquet_to_df(self.dag_id, self.__input_file)

        # Récupérer le contexte de l'exécution
        instance_id = context["run_id"]
        logical_date = context["logical_date"]

        # Convertir logical_date en pendulum
        execution_date = pendulum.instance(logical_date)

        # Formater la date pour la colonne "date"
        formatted_date = execution_date.date()  # "YYYY-MM-DD"

        # Année et semaine ISO à partir de pendulum_date
        year_photo = execution_date.year
        week_photo = execution_date.week_of_year  # ISO week correctement gérée

        # Ajouter les colonnes d'information technique
        logging.info("⚙️ Ajout des informations techniques au fichier Parquet...")
        df["dag_id"] = self.dag_id
        df[self.__execution_date_column] = execution_date
        df[self.__date_column] = formatted_date
        df[self.__week_column] = week_photo
        df[self.__year_column] = year_photo
        df[self.__instance_column] = instance_id

        # Informations sur le DataFrame
        num_rows, num_cols = df.shape
        logging.info(f"📊 Nombre de lignes après modification : {num_rows}, Nombre de colonnes : {num_cols}")
        logging.info(f"📊 Liste des colonnes après modification : {df.columns.tolist()}")
        logging.info(f"📊 Aperçu des premières lignes après modification :\n{df.head().to_string()}")

        # Sauvegarder le résultatdans un nouveau fichier Parquet
        helper.generate_parquet_to_temp(self.dag_id, df, self.__output_file)

        return self.__output_file
