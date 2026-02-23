import logging
import time
import pandas as pd

import app.helper as helper

from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException


class ParquetJoin(PythonOperator):

    def __init__(
        self,
        left_file: str,
        right_file: str,
        output_file: str,
        on: dict,
        how: str = "left",
        drop_right_keys: bool = False,
        rename_right: dict = None,
        task_id: str = "ParquetPandasJoin",
        execution_timeout: timedelta = timedelta(minutes=10),
        **kwargs
    ):
        """
        Jointure générique entre 2 fichiers Parquet avec dictionnaire de colonnes.

        Arguments :
        - left_file (str) : chemin du fichier Parquet de gauche
        - right_file (str) : chemin du fichier Parquet de droite
        - on (dict) : dictionnaire de colonnes à joindre {col_left: col_right}
        - how (str) : type de jointure (left / inner / right / outer). Par défaut : left
        - drop_right_keys (bool) : si True, les colonnes de jointure du fichier de droite seront supprimées du résultat. Par défaut : False
        - rename_right (dict) : dictionnaire de renommage des colonnes du fichier de droite après jointure {col_right: new_col_name}. Par défaut : None (pas de renommage)
        - output_file (str) : chemin de sortie du résultat (temporaire)
        - task_id (str) : ID de la tâche Airflow
        - execution_timeout (timedelta) : durée maximale d'exécution de la tâche
        """

        self._left_file = left_file
        self._right_file = right_file
        self._on = on
        self._how = how
        self._output_file = output_file
        self._drop_right_keys = drop_right_keys
        self._rename_right = rename_right or {}

        super().__init__(
            task_id=task_id,
            python_callable=self._run,
            execution_timeout=execution_timeout,
            **kwargs,
        )

    def _run(self, **context):

        self._validate()
        start_time = time.time()

        # Chargement fichiers
        df_left = helper.load_parquet_to_df(
            self.dag.dag_id,
            self._left_file,
            have_file_security=True,
        )
        df_right = helper.load_parquet_to_df(
            self.dag.dag_id,
            self._right_file,
            have_file_security=True,
        )

        if df_left.empty:
            raise AirflowFailException(f"{self._left_file} est vide.")
        if df_right.empty:
            raise AirflowFailException(f"{self._right_file} est vide.")

        logging.info(f"Left shape : {df_left.shape}")
        logging.info(f"Right shape : {df_right.shape}")

        # Renommer les colonnes de droite si nécessaire
        if self._rename_right:
            df_right = df_right.rename(columns=self._rename_right)

        # Préparer les listes de colonnes
        left_cols = list(self._on.keys())
        right_cols = list(self._on.values())

        # Jointure
        df_result = df_left.merge(
            df_right,
            left_on=left_cols,
            right_on=right_cols,
            how=self._how,
        )

        # Supprimer les colonnes clés de droite si demandé
        if self._drop_right_keys:
            df_result.drop(columns=list(self._on.values()), inplace=True)

        logging.info(f"Résultat shape : {df_result.shape}")

        # Sauvegarde
        helper.generate_parquet_to_temp(
            self.dag.dag_id,
            df_result,
            self._output_file,
        )

        total_time = time.time() - start_time
        logging.info(f"Jointure terminée en {total_time:.2f}s")

    def _validate(self):
        if self._how not in ["left", "inner", "right", "outer"]:
            raise ValueError(f"Type de jointure invalide : {self._how}")
        if not self._on or not isinstance(self._on, dict):
            raise ValueError("Paramètre 'on' obligatoire et doit être un dict {left_col: right_col}")