import logging
import pandas as pd

import app.helper as helper

from datetime import timedelta

from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator


class TypeConverter(PythonOperator):

    def __init__(
        self,
        input_file: str,
        output_file: str,
        text_columns: list = None,
        int_columns: list = None,
        bool_columns: dict = None,  
        execution_timeout: timedelta = timedelta(seconds=30),
        task_id: str = "TypeConverter",
        **kwargs,
        ):
        """
        Convertit les colonnes d’un DataFrame en fonction des types spécifiés (texte, entier, booléen) et génère un fichier temporaire au format Parquet.

        Arguments :
        - input_file (str) : Nom du fichier Parquet source à charger.
        - output_file (str) : Nom du fichier Parquet de sortie après transformation.
        - text_columns (list[str], optionnel) : Liste des colonnes à convertir en texte (str). Par défaut None.
        - int_columns (list[str], optionnel) : Liste des colonnes à convertir en entier nullable (Int64). Par défaut None.
        - bool_columns (dict[str, dict], optionnel) : Dictionnaire des colonnes booléennes à convertir avec mapping personnalisé. Exemple : {"wifi_enabled": {"Y": True, "N": False}}. Par défaut None.
        - execution_timeout (timedelta, optionnel) : Durée maximale d’exécution de la tâche Airflow. Par défaut 30 secondes.
        - task_id (str, optionnel) : Identifiant de la tâche Airflow. Par défaut "TypeConverter".
        """
  
        self._input_file = input_file
        self._output_file = output_file
        self._text_columns = text_columns or []
        self._int_columns = int_columns or []
        self._bool_columns = bool_columns or {}

        super().__init__(
            task_id=task_id,
            python_callable=self._run,
            execution_timeout=execution_timeout,
            **kwargs,
        )

    def _run(self):
        
        # Charger le DataFrame
        df = helper.load_parquet_to_df(self.dag.dag_id, self._input_file)

        # Conversion des colonnes en fonction du type
        try:
            # --- Colonnes str ---
            for col in self._text_columns:
                if col not in df.columns:
                    logging.warning(f"Colonne '{col}' introuvable")
                    continue
                before = df[col].dtype
                df[col] = df[col].astype(str)
                after = df[col].dtype

                logging.info(f"{col} | dtype avant : {before} | dtype après : {after}")

            # --- Colonnes int ---
            for col in self._int_columns:
                if col not in df.columns:
                    logging.warning(f"Colonne '{col}' introuvable")
                    continue
                # Remplacer les "null" ou valeurs invalides par NaN puis convertir
                before = df[col].dtype
                df[col] = (
                    pd.to_numeric(df[col].replace("null", pd.NA), errors="coerce")
                    .astype("Int64")
                )
                after = df[col].dtype

                logging.info(f"{col} | dtype avant : {before} | dtype après : {after}")

            # --- Colonnes bool ---
            for col, mapping in self._bool_columns.items():
                if col not in df.columns:
                    logging.warning(f"Colonne '{col}' introuvable")
                    continue
                before = df[col].dtype
                df[col] = df[col].map(mapping).astype("boolean")
                after = df[col].dtype

                logging.info(f"{col} | dtype avant : {before} | dtype après : {after}")

        except Exception as e:
            raise AirflowFailException(f"Erreur lors de la conversion des types : {e}")

        # Sauvegarde du DataFrame transformé
        helper.generate_parquet_to_temp(
            self.dag.dag_id,
            df,
            self._output_file,
        )
