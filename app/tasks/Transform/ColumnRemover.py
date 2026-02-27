import logging

import app.helper as helper

from datetime import timedelta

from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator

class ColumnRemover(PythonOperator):

    def __init__(
        self,
        input_file: str,
        output_file: str,
        columns_to_drop: list,
        null_threshold_percent: float = 50.0,
        execution_timeout: timedelta = timedelta(seconds=30),
        task_id: str = "ColumnRemover",
        **kwargs,
    ):
        """
        Supprime les colonnes d'un DataFrame et génère un fichier Parquet de sortie.

        Arguments :
        - input_file (str) : Nom du fichier source Parquet à traiter.
        - output_file (str) : Nom du fichier Parquet de sortie après suppression des colonnes.
        - columns (list) : Liste des colonnes à supprimer.
        - null_threshold_percent (float, optionnel) : Pourcentage maximal de valeurs NULL autorisé sur chaque colonne. Par défaut 50%.
        - execution_timeout (timedelta, optionnel) : Durée maximale d’exécution de la tâche Airflow. Par défaut 30 secondes.
        - task_id (str, optionnel) : Identifiant de la tâche Airflow. Par défaut "ColumnRemover".
        """

        self._input_file = input_file
        self._output_file = output_file
        self._columns = columns_to_drop
        self._threshold_percent = null_threshold_percent

        super().__init__(
            task_id=task_id,
            python_callable=self._run,
            execution_timeout=execution_timeout,
            **kwargs,
        )

    def _run(self):

        # Charger les données depuis le fichier source
        df = helper.load_parquet_to_df(self.dag.dag_id, self._input_file)

        # Voir les lignes avant transformation
        print("Type avant transformation", type(df))
        print("Lignes avant transformation", df.head(5))

        # Suppression des colonnes
        try:
            cols_to_drop = [col for col in self._columns if col in df.columns]

            if not cols_to_drop:
                logging.warning("⚠️ Aucune colonne à supprimer trouvée dans le DataFrame.")
            else:
                df = df.drop(columns=cols_to_drop)
                logging.info(f"Suppression des colonnes terminée : {cols_to_drop}")

        except Exception as e:
            raise AirflowFailException(f"Erreur lors de la suppression des colonnes : {e}")

        # ✅ Contrôle des valeurs NULL sur toutes les colonnes
        helper.check_nulls(
            df,
            columns=None,             
            threshold_percent=self._threshold_percent,
        )

        # Sauvegarde du fichier (généralement vers un fichier temporaire)
        helper.generate_parquet_to_temp(
            self.dag.dag_id,
            df,
            self._output_file,
        )