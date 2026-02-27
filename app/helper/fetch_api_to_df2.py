import requests
import pandas as pd
import logging

from airflow.exceptions import AirflowFailException

def fetch_api_to_df2(
    url_api: str,
    headers: dict = None,
    params: dict = None,
    transform_function: callable = None,
    api_type: str = "airfrance",  # nouveau param
) -> pd.DataFrame:
    logging.info(f"🌐 Requête vers l'API : {url_api} avec params {params}")

    try:
        response = requests.get(url_api, headers=headers, params=params)
        response.raise_for_status()
    except requests.RequestException as e:
        raise AirflowFailException(f"❌ Erreur lors de la requête API : {e}")

    try:
        data_dict = response.json()
        logging.info(f"✅ JSON récupéré avec {len(data_dict)} éléments")
    except ValueError as e:
        raise AirflowFailException(f"❌ Erreur lors de la conversion JSON : {e}")

    # Traiter selon le type d'API
    try:
        if transform_function:
            if api_type == "openmeteo":
                # on envoie le JSON brut à la fonction
                result = transform_function(data_dict)
            else:
                # Air France ou autres → aplatissement
                df = pd.json_normalize(data_dict)
                result = transform_function(df) if transform_function else df
        else:
            # Pas de transformation
            if api_type == "openmeteo":
                result = data_dict
            else:
                result = pd.json_normalize(data_dict)
    except Exception as e:
        raise AirflowFailException(f"❌ Erreur lors de la création du DataFrame : {e}")

    return result
