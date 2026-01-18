import os
import csv
import logging
import pandas as pd

from airflow.exceptions import AirflowFailException

def generate_parquet_csv_to_temp(
    dag_id: str,
    data: pd.DataFrame,
    file_name: str,
    file_format: str = "parquet",
    output_separator: str = ",",
    empty_security: bool = False,
):
    """
    Génère un fichier temporaire à partir d'un DataFrame pandas dans le format spécifié (CSV ou Parquet).

    Arguments :
    - dag_id (str) : Identifiant du DAG pour organiser les fichiers temporaires.
    - data (pd.DataFrame) : Le DataFrame contenant les données à sauvegarder.
    - file_name (str) : Nom du fichier à créer (sans extension).
    - file_format (str, optionnel) : Format de fichier à générer, 'csv' ou 'parquet'. Par défaut 'parquet'.
    - output_separator (str, optionnel) : Séparateur à utiliser pour le CSV. Par défaut ','.
    - empty_security (bool, optionnel) : Si True, lève une exception si le DataFrame est vide. Par défaut False.
    """

    logging.info(f"📝 Génération du fichier {file_format} temporaire pour le DAG {dag_id} avec le nom de fichier {file_name}.")

    # Vérification du type de données
    if not isinstance(data, pd.DataFrame):
        raise ValueError("❌ Le paramètre 'data' doit être un DataFrame pandas.")

    # Définir le chemin du fichier temporaire
    folder_temp = f"./temp/{dag_id}"
    file_path = f"{folder_temp}/{file_name}.{file_format}"

    # Créer le répertoire temporaire si nécessaire
    try:
        os.makedirs(folder_temp, exist_ok=True)
        logging.info(f"📂 Répertoire temporaire créé ou existant : {folder_temp}")
    except Exception as e:
        raise AirflowFailException(f"❌ Erreur lors de la création du répertoire temporaire pour le DAG {dag_id}: {e}")

    # Sauvegarder le DataFrame dans le format spécifié
    if data.empty:
        logging.warning(f"⚠️ Le DataFrame est vide. Le fichier {file_path} a été créé mais ne contient pas de données.")
    else:
        num_rows, num_cols = data.shape
        logging.info(f"📊 Le DataFrame contient {num_rows} lignes et {num_cols} colonnes.")

        try:
            if file_format == "csv":
                data.to_csv(file_path, index=False, sep=output_separator)
                logging.info(f"✅ Fichier CSV sauvegardé : {file_path}")
            elif file_format == "parquet":
                data.to_parquet(file_path, index=False)
                logging.info(f"✅ Fichier Parquet sauvegardé : {file_path}")
            else:
                raise ValueError("❌ Format de fichier non supporté. Utilisez 'csv' ou 'parquet'.")
        except Exception as e:
            raise AirflowFailException(f"❌ Erreur lors de la sauvegarde du fichier {file_path}: {e}")

    # Vérification de la sécurité pour les fichiers vides
    if empty_security and data.empty:
        raise AirflowFailException(f"❌ Le fichier {file_name} est vide.")

    return file_path

