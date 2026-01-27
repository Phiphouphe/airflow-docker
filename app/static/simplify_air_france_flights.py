import pandas as pd

from airflow.exceptions import AirflowFailException

def simplify_flights(df: pd.DataFrame) -> pd.DataFrame:
    if "operationalFlights" not in df.columns:
        raise AirflowFailException("❌ 'operationalFlights' introuvable dans le DataFrame")
    
    flights_series = df["operationalFlights"].explode().dropna()
    simplified_flights = []

    for flight in flights_series:
        # prendre le dernier leg
        leg = flight["flightLegs"][-1]

        simplified_flights.append({
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

    return pd.DataFrame(simplified_flights)
