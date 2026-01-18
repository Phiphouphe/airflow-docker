import requests
import psycopg2
import csv
import os
import time
from psycopg2.extras import execute_values

# --- 0. Récupérer les variables d'environnement ---
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = int(os.getenv("DB_PORT", 5432))
API_KEY = os.getenv("AIRFRANCE_API_KEY")

if not all([DB_NAME, DB_USER, DB_PASS]):
    raise ValueError("Variables PostgreSQL manquantes")
if not API_KEY:
    raise ValueError("Clé API Air France manquante")


# --- 1. Attendre que PostgreSQL soit prêt ---
max_retries = 10
retry_delay = 5  # secondes

for i in range(max_retries):
    try:
        conn = psycopg2.connect(
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            port=DB_PORT
        )
        print("Connexion à PostgreSQL réussie !")
        break
    except psycopg2.OperationalError:
        print(f"PostgreSQL non disponible, tentative {i+1}/{max_retries}…")
        time.sleep(retry_delay)
else:
    raise Exception("Impossible de se connecter à PostgreSQL après plusieurs tentatives")

cur = conn.cursor()


# --- 2. Récupérer les données depuis l'API ---
url = "https://api.airfranceklm.com/opendata/flightstatus"
headers = {"API-Key": API_KEY}
params = {
    "endRange": "2025-12-03T23:59:59.000Z",
    "startRange": "2025-12-03T01:00:00.000Z",
    "destination": "CDG",
    "carrierCode": "AF",
    "operatingAirlineCode": "AF",
    "movementType": "A",
    "origin": "NCE",
}

response = requests.get(url, params=params, headers=headers)
response.raise_for_status()
data = response.json()

simplified_flights = []
for flight in data.get("operationalFlights", []):
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

# Formater delay_code au même format pour PostgreSQL TEXT[]
for f in simplified_flights:
    dc = f["delay_code"]
    if dc is None:
        f["delay_code"] = '{}'
    elif isinstance(dc, list):
        f["delay_code"] = '{' + ','.join(dc) + '}'
    elif isinstance(dc, str):
        f["delay_code"] = '{' + dc + '}'


# --- 3. Sauvegarder dans un CSV dans le conteneur (optionnel) ---
csv_file = "/app/data/flights.csv"
fieldnames = simplified_flights[0].keys() if simplified_flights else []

with open(csv_file, mode='w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(simplified_flights)

print(f"CSV créé dans le conteneur : {csv_file}")


# --- 4. Créer la table PostgreSQL si nécessaire ---
create_table_query = """
CREATE TABLE IF NOT EXISTS flights (
    flight_id TEXT PRIMARY KEY,
    flight_number INT,
    airline_code TEXT,
    date DATE,
    scheduled_departure TIMESTAMP,
    actual_departure TIMESTAMP,
    scheduled_arrival TIMESTAMP,
    actual_arrival TIMESTAMP,
    origin_airport TEXT,
    destination_airport TEXT,
    status TEXT,
    delay_minutes INT,
    delay_code TEXT[],
    registration TEXT,
    type_code TEXT,
    type_name TEXT,
    owner_airline TEXT,
    wifi_enabled TEXT
)
"""

cur.execute(create_table_query)
conn.commit()
print("Table PostgreSQL créée (ou existante)")


# --- 5. Insérer les données dans PostgreSQL ---
insert_query = """
INSERT INTO flights (
    flight_id, flight_number, airline_code, date,
    scheduled_departure, actual_departure, scheduled_arrival, actual_arrival,
    origin_airport, destination_airport, status, delay_minutes, delay_code,
    registration, type_code, type_name, owner_airline, wifi_enabled
) VALUES %s
ON CONFLICT (flight_id) DO UPDATE SET
    flight_number = EXCLUDED.flight_number,
    airline_code = EXCLUDED.airline_code,
    date = EXCLUDED.date,
    scheduled_departure = EXCLUDED.scheduled_departure,
    actual_departure = EXCLUDED.actual_departure,
    scheduled_arrival = EXCLUDED.scheduled_arrival,
    actual_arrival = EXCLUDED.actual_arrival,
    origin_airport = EXCLUDED.origin_airport,
    destination_airport = EXCLUDED.destination_airport,
    status = EXCLUDED.status,
    delay_minutes = EXCLUDED.delay_minutes,
    delay_code = EXCLUDED.delay_code,
    registration = EXCLUDED.registration,
    type_code = EXCLUDED.type_code,
    type_name = EXCLUDED.type_name,
    owner_airline = EXCLUDED.owner_airline,
    wifi_enabled = EXCLUDED.wifi_enabled
"""

values = [
    (
        f["flight_id"], f["flight_number"], f["airline_code"], f["date"],
        f["scheduled_departure"], f["actual_departure"], f["scheduled_arrival"], f["actual_arrival"],
        f["origin_airport"], f["destination_airport"], f["status"], f["delay_minutes"], f["delay_code"],
        f["registration"], f["type_code"], f["type_name"], f["owner_airline"], f["wifi_enabled"]
    )
    for f in simplified_flights
]

execute_values(cur, insert_query, values)
conn.commit()
print(f"{len(values)} lignes insérées dans PostgreSQL")

cur.close()
conn.close()