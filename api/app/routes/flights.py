from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import date
from typing import List

from app.db import get_db
from app.auth import require_user
from app.models.schemas import AirportResponse, FlightPredictionResponse, FlightNotFoundResponse

router = APIRouter()

AIRPORTS = {
    "NCE": "Nice",
    "TLS": "Toulouse",
    "LYS": "Lyon",
    "MRS": "Marseille",
    "BOD": "Bordeaux",
    "CDG": "Paris Charles de Gaulle",
}

AIRPORTS_ORIGIN = {k: v for k, v in AIRPORTS.items() if k != "CDG"}


@router.get("/airports", response_model=List[AirportResponse])
def get_airports(current_user: dict = Depends(require_user)):
    return [{"code": code, "city": city} for code, city in AIRPORTS_ORIGIN.items()]


@router.get("/flights/destinations", response_model=List[AirportResponse])
def get_destinations(
    origin_airport: str,
    db: Session = Depends(get_db),
    current_user: dict = Depends(require_user),
):
    if origin_airport.upper() not in AIRPORTS:
        raise HTTPException(status_code=400, detail=f"Aéroport inconnu : {origin_airport}")

    result = db.execute(
        text("SELECT DISTINCT destination_airport FROM ml.flight_predictions WHERE origin_airport = :origin ORDER BY destination_airport"),
        {"origin": origin_airport.upper()}
    ).fetchall()

    return [{"code": row.destination_airport, "city": AIRPORTS.get(row.destination_airport, row.destination_airport)} for row in result]


@router.get("/flights/hours")
def get_hours(
    origin_airport: str,
    destination_airport: str,
    flight_date: date,
    db: Session = Depends(get_db),
    current_user: dict = Depends(require_user),
):
    if origin_airport.upper() not in AIRPORTS:
        raise HTTPException(status_code=400, detail=f"Aéroport inconnu : {origin_airport}")
    if destination_airport.upper() not in AIRPORTS:
        raise HTTPException(status_code=400, detail=f"Aéroport inconnu : {destination_airport}")

    result = db.execute(
        text("""
            SELECT DISTINCT dep_hour
            FROM ml.flight_predictions
            WHERE origin_airport = :origin
            AND destination_airport = :destination
            AND flight_date = :flight_date
            ORDER BY dep_hour
        """),
        {
            "origin": origin_airport.upper(),
            "destination": destination_airport.upper(),
            "flight_date": flight_date,
        }
    ).fetchall()

    return [row.dep_hour for row in result]


@router.get("/flights/prediction")
def get_flight_prediction(
    flight_date: date,
    dep_hour: int,
    origin_airport: str,
    destination_airport: str,
    db: Session = Depends(get_db),
    current_user: dict = Depends(require_user),
):
    if origin_airport.upper() not in AIRPORTS:
        raise HTTPException(status_code=400, detail=f"Aéroport d'origine inconnu : {origin_airport}")
    if destination_airport.upper() not in AIRPORTS:
        raise HTTPException(status_code=400, detail=f"Aéroport de destination inconnu : {destination_airport}")
    if dep_hour < 0 or dep_hour > 23:
        raise HTTPException(status_code=400, detail="L'heure doit être entre 0 et 23.")
    if origin_airport.upper() == destination_airport.upper():
        raise HTTPException(status_code=400, detail="Origine et destination identiques.")

    result = db.execute(
        text("""
            SELECT flight_date, dep_hour, origin_airport, destination_airport, is_delayed
            FROM ml.flight_predictions
            WHERE flight_date = :flight_date
            AND dep_hour = :dep_hour
            AND origin_airport = :origin_airport
            AND destination_airport = :destination_airport
        """),
        {"flight_date": flight_date, "dep_hour": dep_hour, "origin_airport": origin_airport.upper(), "destination_airport": destination_airport.upper()}
    ).fetchone()

    if not result:
        return {"message": "Aucune prédiction trouvée pour ce vol.", "flight_date": flight_date, "dep_hour": dep_hour, "origin_airport": origin_airport.upper(), "destination_airport": destination_airport.upper()}

    return {"flight_date": result.flight_date, "dep_hour": result.dep_hour, "origin_airport": result.origin_airport, "destination_airport": result.destination_airport, "is_delayed": result.is_delayed}