import requests
import pandas as pd
import logging

from airflow.exceptions import AirflowFailException

def fetch_api_to_df(
    url_api: str,
    headers: dict = None,
    params: dict = None,
    transform_function: callable = None,
) -> pd.DataFrame:
    """
    Récupère les données d'une API, les transforme en dict puis en DataFrame.

    Arguments :
    - url_api (str) : URL de l'API à appeler.
    - headers (dict, optionnel) : En-têtes HTTP à envoyer avec la requête.
    - params (dict, optionnel) : Paramètres de la requête.
    - transform_function (callable, optionnel) : Fonction à appliquer sur le DataFrame.
    
    Retourne :
    - df (pd.DataFrame) : Les données transformées en DataFrame pandas.
    """

    logging.info(f"🌐 Requête vers l'API : {url_api} avec params {params}")

    try:
        response = requests.get(url_api, headers=headers, params=params)
        response.raise_for_status()  # Lève une exception si code != 200
    except requests.RequestException as e:
        raise AirflowFailException(f"❌ Erreur lors de la requête API : {e}")

    # Charger le JSON en dictionnaire Python
    try:
        data_dict = response.json()
        logging.info(f"✅ JSON récupéré avec {len(data_dict)} éléments")
    except ValueError as e:
        raise AirflowFailException(f"❌ Erreur lors de la conversion JSON : {e}")

    # Transformer en DataFrame Pandas
    try:
        df = pd.json_normalize(data_dict,)
        print(df.columns)  # quelles colonnes existent réellement ?
        print(df.head())
        if transform_function:
            df = transform_function(df)
        logging.info(f"📊 DataFrame créé avec {df.shape[0]} lignes et {df.shape[1]} colonnes")
    except Exception as e:
        raise AirflowFailException(f"❌ Erreur lors de la création du DataFrame : {e}")

    return df
