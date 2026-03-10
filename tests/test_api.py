import pytest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient

"""
Tests unitaires pour l'API FastAPI.
On utilise TestClient de FastAPI pour simuler les requêtes HTTP
sans démarrer un vrai serveur ni se connecter à la base de données.
"""

# ── Mock de la base de données ────────────────────────────────────────────────

def override_get_db():
    """Mock de la session DB — ne se connecte pas à PostgreSQL."""
    db = MagicMock()
    yield db


# ── Import de l'app avec mock DB ──────────────────────────────────────────────

@pytest.fixture(scope="module")
def client():
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'api'))

    from app.main import app
    from app.db import get_db
    app.dependency_overrides[get_db] = override_get_db

    with TestClient(app) as c:
        yield c

    app.dependency_overrides.clear()


@pytest.fixture(scope="module")
def auth_token(client):
    """Obtenir un token JWT valide pour les tests authentifiés."""
    response = client.post(
        "/auth/login",
        data={"username": "admin", "password": "admin"},
    )
    assert response.status_code == 200
    return response.json()["access_token"]


@pytest.fixture(scope="module")
def auth_headers(auth_token):
    return {"Authorization": f"Bearer {auth_token}"}


# ── Tests Health ──────────────────────────────────────────────────────────────

class TestHealth:

    def test_health_returns_200(self, client):
        """GET /health retourne 200."""
        response = client.get("/health")
        assert response.status_code == 200

    def test_health_returns_ok(self, client):
        """GET /health retourne status ok."""
        response = client.get("/health")
        assert response.json() == {"status": "ok"}


# ── Tests Auth ────────────────────────────────────────────────────────────────

class TestAuth:

    def test_login_valid_credentials(self, client):
        """POST /auth/login avec credentials valides retourne un token."""
        response = client.post(
            "/auth/login",
            data={"username": "admin", "password": "admin"},
        )
        assert response.status_code == 200
        assert "access_token" in response.json()
        assert response.json()["token_type"] == "bearer"

    def test_login_invalid_password(self, client):
        """POST /auth/login avec mauvais mot de passe retourne 401."""
        response = client.post(
            "/auth/login",
            data={"username": "admin", "password": "wrong_password"},
        )
        assert response.status_code == 401

    def test_login_unknown_user(self, client):
        """POST /auth/login avec utilisateur inconnu retourne 401."""
        response = client.post(
            "/auth/login",
            data={"username": "unknown", "password": "password"},
        )
        assert response.status_code == 401

    def test_login_user_credentials(self, client):
        """POST /auth/login avec credentials user valides retourne un token."""
        response = client.post(
            "/auth/login",
            data={"username": "user", "password": "user"},
        )
        assert response.status_code == 200
        assert "access_token" in response.json()


# ── Tests Airports ────────────────────────────────────────────────────────────

class TestAirports:

    def test_get_airports_without_token_returns_401(self, client):
        """GET /airports sans token retourne 401."""
        response = client.get("/airports")
        assert response.status_code == 401

    def test_get_airports_with_token_returns_200(self, client, auth_headers):
        """GET /airports avec token retourne 200."""
        response = client.get("/airports", headers=auth_headers)
        assert response.status_code == 200

    def test_get_airports_excludes_cdg(self, client, auth_headers):
        """GET /airports ne retourne pas CDG comme origine."""
        response = client.get("/airports", headers=auth_headers)
        codes = [a["code"] for a in response.json()]
        assert "CDG" not in codes

    def test_get_airports_returns_5_airports(self, client, auth_headers):
        """GET /airports retourne bien 5 aéroports (sans CDG)."""
        response = client.get("/airports", headers=auth_headers)
        assert len(response.json()) == 5

    def test_get_airports_contains_nce(self, client, auth_headers):
        """GET /airports contient NCE."""
        response = client.get("/airports", headers=auth_headers)
        codes = [a["code"] for a in response.json()]
        assert "NCE" in codes

    def test_get_airports_response_format(self, client, auth_headers):
        """GET /airports retourne bien code et city."""
        response = client.get("/airports", headers=auth_headers)
        for airport in response.json():
            assert "code" in airport
            assert "city" in airport


# ── Tests Destinations ────────────────────────────────────────────────────────

class TestDestinations:

    def test_unknown_airport_returns_400(self, client, auth_headers):
        """GET /flights/destinations avec aéroport inconnu retourne 400."""
        response = client.get(
            "/flights/destinations",
            params={"origin_airport": "XXX"},
            headers=auth_headers,
        )
        assert response.status_code == 400

    def test_valid_airport_returns_200(self, client, auth_headers):
        """GET /flights/destinations avec aéroport valide retourne 200."""
        with patch("app.routes.flights.Session") as mock_session:
            mock_db = MagicMock()
            mock_db.execute.return_value.fetchall.return_value = [
                MagicMock(destination_airport="CDG")
            ]
            response = client.get(
                "/flights/destinations",
                params={"origin_airport": "NCE"},
                headers=auth_headers,
            )
        assert response.status_code == 200

    def test_without_token_returns_401(self, client):
        """GET /flights/destinations sans token retourne 401."""
        response = client.get(
            "/flights/destinations",
            params={"origin_airport": "NCE"},
        )
        assert response.status_code == 401


# ── Tests Prediction ──────────────────────────────────────────────────────────

class TestPrediction:

    def test_same_origin_destination_returns_400(self, client, auth_headers):
        """Origine = destination retourne 400."""
        response = client.get(
            "/flights/prediction",
            params={
                "flight_date": "2026-03-08",
                "dep_hour": 10,
                "origin_airport": "NCE",
                "destination_airport": "NCE",
            },
            headers=auth_headers,
        )
        assert response.status_code == 400

    def test_invalid_dep_hour_returns_400(self, client, auth_headers):
        """Heure invalide (> 23) retourne 400."""
        response = client.get(
            "/flights/prediction",
            params={
                "flight_date": "2026-03-08",
                "dep_hour": 25,
                "origin_airport": "NCE",
                "destination_airport": "CDG",
            },
            headers=auth_headers,
        )
        assert response.status_code == 400

    def test_unknown_origin_returns_400(self, client, auth_headers):
        """Aéroport d'origine inconnu retourne 400."""
        response = client.get(
            "/flights/prediction",
            params={
                "flight_date": "2026-03-08",
                "dep_hour": 10,
                "origin_airport": "XXX",
                "destination_airport": "CDG",
            },
            headers=auth_headers,
        )
        assert response.status_code == 400

    def test_unknown_destination_returns_400(self, client, auth_headers):
        """Aéroport de destination inconnu retourne 400."""
        response = client.get(
            "/flights/prediction",
            params={
                "flight_date": "2026-03-08",
                "dep_hour": 10,
                "origin_airport": "NCE",
                "destination_airport": "ZZZ",
            },
            headers=auth_headers,
        )
        assert response.status_code == 400

    def test_without_token_returns_401(self, client):
        """GET /flights/prediction sans token retourne 401."""
        response = client.get(
            "/flights/prediction",
            params={
                "flight_date": "2026-03-08",
                "dep_hour": 10,
                "origin_airport": "NCE",
                "destination_airport": "CDG",
            },
        )
        assert response.status_code == 401

    def test_negative_dep_hour_returns_400(self, client, auth_headers):
        """Heure négative retourne 400."""
        response = client.get(
            "/flights/prediction",
            params={
                "flight_date": "2026-03-08",
                "dep_hour": -1,
                "origin_airport": "NCE",
                "destination_airport": "CDG",
            },
            headers=auth_headers,
        )
        assert response.status_code == 400