import os
import json
import logging

from airflow.exceptions import AirflowFailException

def load_json_to_dict(
    dag_id: str,
    file_name: str,
) -> dict:
    """
    Charge un fichier JSON déjà existant dans un dictionnaire Python.

    Arguments :
    - dag_id (str) : Identifiant du DAG pour localiser les fichiers temporaires.
    - file_name (str) : Nom du fichier JSON à charger (sans extension).
    """

    logging.info(f"📥 Chargement du fichier JSON temporaire pour le DAG {dag_id} avec le nom de fichier {file_name}.")

    # Définir le chemin du fichier temporaire
    file_path = f"./temp/{dag_id}/{file_name}.json"

    # Vérifier si le fichier existe
    if not os.path.exists(file_path):
        logging.error(f"⚠️ Le fichier JSON {file_path} n'existe pas.")
        return {}

    # Charger le fichier JSON dans un dictionnaire Python
    try:
        with open(file_path, 'r', encoding='utf-8') as json_file:
            data = json.load(json_file)
            logging.info(f"✅ Fichier JSON chargé : {file_path}")
            logging.info(f"📊 Le dictionnaire chargé contient {len(data)} clés.")
    except Exception as e:
        raise AirflowFailException(f"❌ Erreur lors du chargement du fichier {file_path} pour le DAG {dag_id}: {e}")

    return data