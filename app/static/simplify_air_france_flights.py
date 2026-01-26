import pandas as pd
import logging

class AirFranceAPI:

    @staticmethod
    def simplify_flights(df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforme les données brutes de l'API Air France pour ne garder que les colonnes nécessaires.
        """
        simplified_flights = []

        for flight in df.get("operationalFlights", []):
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
                "wifi_enabled": leg["aircraft"].get("wifiEnabled")
            })

        # Formater delay_code pour PostgreSQL TEXT[]
        for f in simplified_flights:
            dc = f["delay_code"]
            if dc is None:
                f["delay_code"] = '{}'
            elif isinstance(dc, list):
                f["delay_code"] = '{' + ','.join(dc) + '}'
            elif isinstance(dc, str):
                f["delay_code"] = '{' + dc + '}'

        return pd.DataFrame(simplified_flights)
