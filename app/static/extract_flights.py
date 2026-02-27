import pandas as pd
import numpy as np

from airflow.exceptions import AirflowFailException

def extract_flights(df: pd.DataFrame) -> pd.DataFrame:
    if "operationalFlights" not in df.columns:
        raise AirflowFailException("❌ 'operationalFlights' introuvable dans le DataFrame")
    
    flights_series = df["operationalFlights"].explode().dropna()
    extract_flights = []

    for flight in flights_series:
        # prendre le dernier leg
        leg = flight["flightLegs"][-1]

        extract_flights.append({
            "flight_id": flight["id"],
            "flight_number": flight["flightNumber"],
            "airline_code": flight["airline"]["code"],
            "date": flight["flightScheduleDate"],
            "scheduled_departure": leg["departureInformation"]["times"]["scheduled"],
            "actual_departure": leg["departureInformation"]["times"].get("actual"),
            "scheduled_arrival": leg["arrivalInformation"]["times"]["scheduled"],
            "actual_arrival": leg["arrivalInformation"]["times"].get("actual"),
            "origin_airport": leg["departureInformation"]["airport"]["code"],
            "destination_airport": leg["arrivalInformation"]["airport"]["code"],
            "status": flight.get("flightStatusPublic"),
            "delay_minutes": leg.get("irregularity", {}).get("delayMinutes"),
            "delay_code": leg.get("irregularity", {}).get("delayCode"),
            "registration": leg["aircraft"].get("registration"),
            "type_code": leg["aircraft"].get("typeCode"),
            "type_name": leg["aircraft"].get("typeName"),
            "owner_airline": leg["aircraft"].get("ownerAirlineName"),
            "wifi_enabled": leg["aircraft"].get("wifiEnabled"),
        })

    for f in extract_flights:
        dc = f["delay_code"]
        if dc is None:
            f["delay_code"] = '{}'
        elif isinstance(dc, list) or isinstance(dc, pd.Series) or isinstance(dc, pd.Index) or isinstance(dc, np.ndarray):
            f["delay_code"] = '{' + ','.join(map(str, dc)) + '}'
        elif isinstance(dc, str):
            f["delay_code"] = '{' + dc + '}'
        else:
            f["delay_code"] = '{' + str(dc) + '}'

    return pd.DataFrame(extract_flights)
