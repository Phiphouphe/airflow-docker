import requests
import pandas as pd
import logging
import time
from requests.exceptions import HTTPError
from airflow.exceptions import AirflowFailException

def fetch_api_to_df2(
    url_api: str,
    headers: dict = None,
    params: dict = None,
    transform_function: callable = None,
    api_type: str = "airfrance",
) -> pd.DataFrame:
    logging.info(f"🌐 Requête vers l'API : {url_api} avec params {params}")

    max_retries = 3
    retry_delays = [30, 60]  # secondes entre les tentatives

    for attempt in range(max_retries):
        try:
            response = requests.get(url_api, headers=headers, params=params)
            response.raise_for_status()
            break  # succès, on sort de la boucle
        except HTTPError as e:
            if response.status_code in [502, 503, 504] and attempt < max_retries - 1:
                wait = retry_delays[attempt]
                logging.warning(f"⚠️ {e} — retry dans {wait}s (tentative {attempt + 1}/{max_retries})")
                time.sleep(wait)
            else:
                raise AirflowFailException(f"❌ Erreur lors de la requête API : {e}")
        except requests.RequestException as e:
            raise AirflowFailException(f"❌ Erreur lors de la requête API : {e}")

    # Le reste du code est inchangé
    try:
        data_dict = response.json()
        logging.info(f"✅ JSON récupéré avec {len(data_dict)} éléments")
    except ValueError as e:
        raise AirflowFailException(f"❌ Erreur lors de la conversion JSON : {e}")

    try:
        if transform_function:
            if api_type == "openmeteo":
                result = transform_function(data_dict)
            else:
                df = pd.json_normalize(data_dict)
                result = transform_function(df) if transform_function else df
        else:
            if api_type == "openmeteo":
                result = data_dict
            else:
                result = pd.json_normalize(data_dict)
    except Exception as e:
        raise AirflowFailException(f"❌ Erreur lors de la création du DataFrame : {e}")

    return result