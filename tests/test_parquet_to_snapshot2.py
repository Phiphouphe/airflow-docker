"""
Tests unitaires pour la logique de Parquet_to_snapshot2.
On teste la construction des requêtes DELETE selon le mode et l'api_type,
sans connexion à la base de données ni à Airflow.
"""
import pytest
import pandas as pd


# ── Helper qui reproduit la logique de DELETE de Parquet_to_snapshot2 ─────────

def build_delete_query(mode: str, api_type: str, schema: str, table_name: str) -> str:
    """Reproduit la logique de construction des requêtes DELETE."""
    if api_type == "openmeteo":
        if mode == "scheduled":
            return f'DELETE FROM "{schema}"."{table_name}" WHERE "date_photo" <= :date_photo AND "airport_iata" = :airport_iata'
        else:
            return f'DELETE FROM "{schema}"."{table_name}" WHERE "date_photo" = :date_photo AND "airport_iata" = :airport_iata'
    else:  # airfrance
        if mode == "scheduled":
            return f'DELETE FROM "{schema}"."{table_name}" WHERE "date_photo" <= :date_photo AND "origin_airport" = :origin_airport'
        else:
            return f'DELETE FROM "{schema}"."{table_name}" WHERE "date_photo" = :date_photo AND "origin_airport" = :origin_airport'


def validate_parquet(df: pd.DataFrame) -> str:
    """Reproduit la validation du Parquet (date_photo unique)."""
    if 'date_photo' not in df.columns:
        raise ValueError("La colonne 'date_photo' est requise dans le Parquet")
    unique_dates = df['date_photo'].unique()
    if len(unique_dates) > 1:
        raise ValueError("Le Parquet doit contenir une seule valeur unique pour DATE_PHOTO")
    return unique_dates[0]


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def df_airfrance():
    return pd.DataFrame({
        "flight_id":      ["AF001", "AF002"],
        "origin_airport": ["NCE", "NCE"],
        "date_photo":     ["2026-03-07", "2026-03-07"],
    })


@pytest.fixture
def df_openmeteo():
    return pd.DataFrame({
        "airport_iata": ["NCE", "NCE"],
        "date":         ["2026-03-07", "2026-03-07"],
        "date_photo":   ["2026-03-07", "2026-03-07"],
    })


@pytest.fixture
def df_multiple_date_photos():
    return pd.DataFrame({
        "flight_id":  ["AF001", "AF002"],
        "date_photo": ["2026-03-06", "2026-03-07"],
    })


@pytest.fixture
def df_no_date_photo():
    return pd.DataFrame({
        "flight_id": ["AF001"],
    })


# ── Tests requêtes DELETE ─────────────────────────────────────────────────────

class TestBuildDeleteQuery:

    def test_airfrance_raw_uses_equal(self):
        """Mode raw airfrance utilise = pour date_photo."""
        query = build_delete_query("raw", "airfrance", "raw", "raw_flights")
        assert '"date_photo" = :date_photo' in query
        assert '"origin_airport" = :origin_airport' in query

    def test_airfrance_scheduled_uses_lte(self):
        """Mode scheduled airfrance utilise <= pour date_photo."""
        query = build_delete_query("scheduled", "airfrance", "raw", "scheduled_flights")
        assert '"date_photo" <= :date_photo' in query
        assert '"origin_airport" = :origin_airport' in query

    def test_openmeteo_raw_uses_equal(self):
        """Mode raw openmeteo utilise = pour date_photo."""
        query = build_delete_query("raw", "openmeteo", "raw", "raw_weather")
        assert '"date_photo" = :date_photo' in query
        assert '"airport_iata" = :airport_iata' in query

    def test_openmeteo_scheduled_uses_lte(self):
        """Mode scheduled openmeteo utilise <= pour date_photo."""
        query = build_delete_query("scheduled", "openmeteo", "raw", "scheduled_weather")
        assert '"date_photo" <= :date_photo' in query
        assert '"airport_iata" = :airport_iata' in query

    def test_airfrance_no_airport_iata(self):
        """airfrance n'utilise pas airport_iata."""
        query = build_delete_query("raw", "airfrance", "raw", "raw_flights")
        assert "airport_iata" not in query

    def test_openmeteo_no_origin_airport(self):
        """openmeteo n'utilise pas origin_airport."""
        query = build_delete_query("raw", "openmeteo", "raw", "raw_weather")
        assert "origin_airport" not in query

    def test_schema_and_table_in_query(self):
        """Le schéma et la table sont bien dans la requête."""
        query = build_delete_query("raw", "airfrance", "staging", "raw_flights")
        assert '"staging"."raw_flights"' in query

    def test_staging_schema(self):
        """Fonctionne avec le schéma staging."""
        query = build_delete_query("scheduled", "airfrance", "staging", "scheduled_flights")
        assert '"staging"."scheduled_flights"' in query

    def test_analytics_schema(self):
        """Fonctionne avec le schéma analytics."""
        query = build_delete_query("raw", "airfrance", "analytics", "raw_flights")
        assert '"analytics"."raw_flights"' in query


# ── Tests validation Parquet ──────────────────────────────────────────────────

class TestValidateParquet:

    def test_valid_parquet_returns_date(self, df_airfrance):
        """Un Parquet valide retourne la date_photo."""
        date_photo = validate_parquet(df_airfrance)
        assert date_photo == "2026-03-07"

    def test_missing_date_photo_raises(self, df_no_date_photo):
        """Pas de colonne date_photo → ValueError."""
        with pytest.raises(ValueError, match="date_photo"):
            validate_parquet(df_no_date_photo)

    def test_multiple_date_photos_raises(self, df_multiple_date_photos):
        """Plusieurs date_photo différentes → ValueError."""
        with pytest.raises(ValueError, match="DATE_PHOTO"):
            validate_parquet(df_multiple_date_photos)

    def test_empty_dataframe_raises(self):
        """DataFrame vide avec colonne date_photo → pas d'erreur sur unicité."""
        df = pd.DataFrame({"date_photo": []})
        with pytest.raises((ValueError, IndexError)):
            validate_parquet(df)

