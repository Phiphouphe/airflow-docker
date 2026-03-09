from pydantic import BaseModel
from datetime import date, datetime
from typing import Optional


# ── Auth ──────────────────────────────────────────────────────────────────────

class LoginRequest(BaseModel):
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


# ── Airports ──────────────────────────────────────────────────────────────────

class AirportResponse(BaseModel):
    code: str
    city: str


# ── Flights ───────────────────────────────────────────────────────────────────

class FlightPredictionResponse(BaseModel):
    flight_date: date
    dep_hour: int
    origin_airport: str
    destination_airport: str
    is_delayed: bool
    model_name: str
    model_version: str
    prediction_date: datetime


class FlightNotFoundResponse(BaseModel):
    message: str
    flight_date: date
    dep_hour: int
    origin_airport: str
    destination_airport: str