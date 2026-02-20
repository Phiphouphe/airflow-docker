import pandas as pd

from airflow.exceptions import AirflowFailException

def extract_daily_weather(json_data, airport_iata):
    """
    Transforme un JSON daily Open-Meteo en deux DataFrames : weather_yesterday et weather_today.
    
    Args:
        json_data (dict): JSON renvoyé par Open-Meteo.
        airport_iata (str): Code IATA de l'aéroport (ex: 'ORY', 'MRS').
        
    Returns:
        weather_yesterday (pd.DataFrame)
        weather_today (pd.DataFrame)
    """

    # Vérifier que "daily" existe
    if "daily" not in json_data:
        raise AirflowFailException("Clé 'daily' absente du JSON Open-Meteo")

    daily = json_data["daily"]

    # Créer le DataFrame complet
    df = pd.DataFrame({
        "date": daily["time"],
        "temp_max": daily["temperature_2m_max"],
        "temp_min": daily["temperature_2m_min"],
        "temp_mean": daily["temperature_2m_mean"],
        "precipitation_sum": daily["precipitation_sum"],
        "rain_sum": daily["rain_sum"],
        "snowfall_sum": daily["snowfall_sum"],
        "precipitation_hours": daily["precipitation_hours"],
        "wind_speed_max": daily["wind_speed_10m_max"],
        "wind_gusts_max": daily["wind_gusts_10m_max"],
        "wind_direction": daily["wind_direction_10m_dominant"],
        "weather_code": daily["weather_code"]
    })

    # Normalement 1 seule ligne
    if df.empty:
        raise AirflowFailException("Aucune donnée météo retournée")

    df["airport_iata"] = airport_iata

    return df

