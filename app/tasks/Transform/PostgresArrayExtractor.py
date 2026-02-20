import logging
import pandas as pd

import app.helper as helper

from datetime import timedelta

from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator


class PostgresArrayExtractor(PythonOperator):

    def __init__(
        self,
        input_file: str,
        output_file: str,
        columns: list,
        target_type: str = "int",
        execution_timeout: timedelta = timedelta(seconds=30),
        task_id: str = "PostgresArrayExtractor",
        **kwargs,
    ):
        """
        Extrait le premier élément d'une colonne PostgreSQL stockée sous forme d'array texte (ex: "{55}").

        Arguments :
        - input_file (str) : Fichier Parquet source.
        - output_file (str) : Fichier Parquet de sortie.
        - columns (list) : Colonnes à transformer.
        - target_type (str) : Type cible ("int" ou "str").
        """

        self._input_file = input_file
        self._output_file = output_file
        self._columns = columns
        self._target_type = target_type

        super().__init__(
            task_id=task_id,
            python_callable=self._run,
            execution_timeout=execution_timeout,
            **kwargs,
        )

    def _run(self):

        # Charger le DataFrame depuis le fichier Parquet
        df = helper.load_parquet_to_df(self.dag.dag_id, self._input_file)

        # Appliquer la transformation sur les colonnes spécifiées
        try:
            for col in self._columns:
                if col not in df.columns:
                    logging.warning(f"⚠️ Colonne '{col}' introuvable")
                    continue

                before = df[col].dtype

                cleaned = (
                    df[col]
                    .str.strip("{}")
                    .replace("", pd.NA)
                )

                if self._target_type == "int":
                    df[col] = pd.to_numeric(cleaned, errors="coerce").astype("Int64")
                else:
                    df[col] = cleaned.astype("string")

                after = df[col].dtype

                logging.info(f"📦 {col} | dtype avant : {before} | dtype après : {after}")

        except Exception as e:
            logging.error("❌ Erreur lors de l'extraction array Postgres")
            raise AirflowFailException(e)

        logging.info("💾 Sauvegarde du fichier transformé")

        # Sauvegarder le DataFrame transformé en Parquet dans temp
        helper.generate_parquet_to_temp(
            self.dag.dag_id,
            df,
            self._output_file,
        )

