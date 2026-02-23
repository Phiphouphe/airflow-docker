import logging
import pandas as pd

import app.helper as helper

from datetime import timedelta
from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator


class ApplyFunction(PythonOperator):

    def __init__(
        self,
        input_file: str,
        output_file: str,
        columns_functions: dict,
        args: tuple = (),
        kwargs: dict = None,
        null_threshold_percent: float = 50.0,
        execution_timeout: timedelta = timedelta(seconds=30),
        task_id: str = "ApplyFunction",
        **kwargs_op,
        ):
        """
        Applique une ou plusieurs fonctions sur un DataFrame pour créer de nouvelles colonnes et génère un fichier Parquet de sortie.

        Arguments :
        - input_file (str) : Nom du fichier source Parquet à traiter.
        - output_file (str) : Nom du fichier Parquet de sortie après application des fonctions.
        - columns_functions (dict) : Dictionnaire {"nom_colonne": fonction} à appliquer sur le DataFrame.
        - args (tuple, optionnel) : Arguments positionnels à passer aux fonctions.
        - kwargs (dict, optionnel) : Arguments nommés à passer aux fonctions.
        - null_threshold_percent (float, optionnel) : Seuil max autorisé de valeurs NULL pour chaque colonne. Par défaut 50%.
        - execution_timeout (timedelta, optionnel) : Durée maximale d’exécution de la tâche Airflow. Par défaut 30 secondes.
        - task_id (str, optionnel) : Identifiant de la tâche Airflow. Par défaut "ApplyFunction".
        """
        self._input_file = input_file
        self._output_file = output_file
        self._columns_functions = columns_functions
        self._args = args
        self._kwargs = kwargs or {}
        self._threshold_percent = null_threshold_percent

        super().__init__(
            task_id=task_id,
            python_callable=self._run,
            execution_timeout=execution_timeout,
            **kwargs_op,
        )

    def _run(self):
        
        # Charger le DataFrame
        df = helper.load_parquet_to_df(self.dag.dag_id, self._input_file)

        # Aperçu avant transformation
        logging.info(f"Type avant transformation : {type(df)}")
        logging.info(f"Lignes avant transformation :\n{df.head(5)}")

        # Appliquer toutes les fonctions
        for new_col, func in self._columns_functions.items():
            if not callable(func):
                logging.warning(f"❌ {new_col} : valeur associée n'est pas une fonction, ignorée.")
                continue

            try:
                df[new_col] = df.apply(lambda row: func(row, *self._args, **self._kwargs), axis=1)
                logging.info(f"✅ Colonne '{new_col}' créée avec succès.")

                # Contrôle des valeurs nulles pour la nouvelle colonne
                helper.check_nulls(
                    df,
                    columns=[new_col],
                    threshold_percent=self._threshold_percent,
                )

                logging.info(f"Colonnes après ApplyFunction : {df.columns.tolist()}")

            except Exception as e:
                raise AirflowFailException(
                    f"❌ Erreur lors de l'application de la fonction pour '{new_col}' : {e}"
                )

        # Sauvegarde du DataFrame transformé
        helper.generate_parquet_to_temp(
            self.dag.dag_id,
            df,
            self._output_file,
        )
