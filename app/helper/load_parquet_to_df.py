import logging
import os
import pandas as pd

from pathlib import Path
from airflow.exceptions import AirflowFailException

def load_parquet_to_df(
    dag_id: str,
    file_name: str,
    empty_security: bool = False,
    have_file_security: bool = False,
) -> pd.DataFrame:
    """
    Charge un fichier temporaire dans un DataFrame pandas à partir du format spécifié (Parquet).

    Arguments :
    - dag_id (str) : Identifiant du DAG pour localiser les fichiers temporaires.
    - file_name (str) : Nom du fichier à charger (sans extension).
    - empty_security (bool, optionnel) : Si True, lève une exception si le fichier est vide. Par défaut False.
    - have_file_security (bool, optionnel) : Si True, lève une exception si le fichier n'existe pas. Par défaut False.
    """

    logging.info(f"📥 Chargement du fichier Parquet temporaire pour le DAG {dag_id} avec le nom de fichier {file_name}.")
    
    # Définir le chemin du fichier temporaire
    file_path = get_file_path(dag_id, file_name, have_file_security)

    # Si le fichier n'existe pas et que la sécurité n'est pas activée, renvoyer un DataFrame vide
    if not file_path.exists():
        logging.warning(f"⚠️ Le fichier {file_path} n'existe pas. Retourne un DataFrame vide.")
        return pd.DataFrame()

    # Charger le fichier dans un DataFrame pandas
    try:
        df = pd.read_parquet(file_path)
        logging.info(f"✅ Fichier Parquet chargé : {file_path} ({len(df)} lignes)")
    except Exception as e:
        raise AirflowFailException(f"❌ Erreur lors du chargement du fichier {file_path} pour le DAG {dag_id}: {e}")

    # Vérification de la sécurité pour les fichiers vides
    if empty_security and df.empty:
        raise AirflowFailException(f"❌ Le fichier {file_name} est vide pour le DAG {dag_id}. Arrêt de l'exécution.")

    # Informations sur le DataFrame chargé
    num_rows, num_cols = df.shape
    logging.info(f"📊 Le DataFrame chargé contient {num_rows} lignes et {num_cols} colonnes.")
    logging.info(f"📋 Colonnes : {df.columns.tolist()}")
    logging.info(f"🔍 Aperçu des données :\n{df.head(5)}")
    logging.info(f"Chargement du fichier terminé : {file_path}.")
    
    return df


def get_file_path(
    dag_id: str,
    file_name: str,
    have_file_security: bool,
) -> Path:
    """
    Retourne le chemin complet du fichier temporaire pour le DAG donné.

    Arguments :
    - dag_id (str) : Identifiant du DAG pour localiser les fichiers temporaires.
    - file_name (str) : Nom du fichier (sans extension).
    - have_file_security (bool) : Si True, lève une exception si le fichier n'existe pas.
    """

    # Définir le chemin du fichier temporaire
    folder_temp = f"./temp/{dag_id}"
    file_path = Path(f"{folder_temp}/{file_name}.parquet")

    # Vérification de la sécurité pour l'existence du fichier
    if have_file_security and not os.path.exists(file_path):
        raise AirflowFailException(f"❌ Le fichier {file_path} n'existe pas pour le DAG {dag_id}.")

    return file_path

