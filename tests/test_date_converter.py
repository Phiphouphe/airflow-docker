import pytest
import pandas as pd
import numpy as np
from datetime import date

"""
Tests unitaires pour la logique de DateConverter.
On teste la conversion des colonnes date/timestamp sur des DataFrames pandas,
sans dépendances Airflow ni fichiers Parquet.
"""

# ── Helper qui reproduit la logique de DateConverter._run() ──────────────────

def convert_dates(df: pd.DataFrame, timestamp_columns: list = None, date_columns: list = None) -> pd.DataFrame:
    """Reproduit la logique de conversion de DateConverter."""
    df = df.copy()
    timestamp_columns = timestamp_columns or []
    date_columns = date_columns or []

    for col in timestamp_columns:
        if col not in df.columns:
            continue
        df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

    for col in date_columns:
        if col not in df.columns:
            continue
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    return df


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def df_raw():
    return pd.DataFrame({
        "flight_id":           ["AF001", "AF002", "AF003"],
        "date":                ["2026-03-07", "2026-03-07", "2026-03-07"],
        "scheduled_departure": ["2026-03-07T06:00:00+01:00", "2026-03-07T08:30:00+01:00", None],
        "actual_departure":    ["2026-03-07T06:05:00+01:00", None, None],
    })


# ── Tests timestamp ───────────────────────────────────────────────────────────

class TestTimestampConversion:

    def test_timestamp_converted_to_datetime(self, df_raw):
        """Les colonnes timestamp sont converties en datetime."""
        result = convert_dates(df_raw, timestamp_columns=["scheduled_departure", "actual_departure"])
        assert pd.api.types.is_datetime64_any_dtype(result["scheduled_departure"])

    def test_timestamp_is_utc(self, df_raw):
        """Les timestamps sont normalisés en UTC."""
        result = convert_dates(df_raw, timestamp_columns=["scheduled_departure"])
        dtype_str = str(result["scheduled_departure"].dtype)
        assert "UTC" in dtype_str, f"Le timestamp doit être en UTC, reçu : {dtype_str}"

    def test_null_timestamp_becomes_nat(self, df_raw):
        """Les valeurs None deviennent NaT."""
        result = convert_dates(df_raw, timestamp_columns=["scheduled_departure"])
        assert pd.isna(result["scheduled_departure"].iloc[2])

    def test_invalid_timestamp_becomes_nat(self):
        """Les valeurs invalides deviennent NaT (errors='coerce')."""
        df = pd.DataFrame({"scheduled_departure": ["not_a_date", "2026-03-07T06:00:00+01:00"]})
        result = convert_dates(df, timestamp_columns=["scheduled_departure"])
        assert pd.isna(result["scheduled_departure"].iloc[0])

    def test_missing_column_ignored(self, df_raw):
        """Une colonne inexistante est ignorée sans erreur."""
        result = convert_dates(df_raw, timestamp_columns=["colonne_inexistante"])
        assert "colonne_inexistante" not in result.columns


# ── Tests date calendrier ─────────────────────────────────────────────────────

class TestDateConversion:

    def test_date_converted_to_date_type(self, df_raw):
        """Les colonnes date sont converties en type date."""
        result = convert_dates(df_raw, date_columns=["date"])
        assert result["date"].iloc[0] == date(2026, 3, 7)

    def test_date_column_type(self, df_raw):
        """Le type de la colonne date est bien object (date Python)."""
        result = convert_dates(df_raw, date_columns=["date"])
        assert isinstance(result["date"].iloc[0], date)

    def test_null_date_becomes_nat(self):
        """Les valeurs None dans une colonne date deviennent NaT."""
        df = pd.DataFrame({"date": [None, "2026-03-07"]})
        result = convert_dates(df, date_columns=["date"])
        assert pd.isna(result["date"].iloc[0])

    def test_date_missing_column_ignored(self, df_raw):
        """Une colonne date inexistante est ignorée sans erreur."""
        result = convert_dates(df_raw, date_columns=["colonne_inexistante"])
        assert "colonne_inexistante" not in result.columns


# ── Tests combinés ────────────────────────────────────────────────────────────

class TestCombinedConversion:

    def test_timestamp_and_date_together(self, df_raw):
        """Conversion simultanée de timestamp et date."""
        result = convert_dates(
            df_raw,
            timestamp_columns=["scheduled_departure"],
            date_columns=["date"]
        )
        assert pd.api.types.is_datetime64_any_dtype(result["scheduled_departure"])
        assert isinstance(result["date"].iloc[0], date)

    def test_original_df_not_modified(self, df_raw):
        """Le DataFrame original n'est pas modifié."""
        original_dtype = df_raw["date"].dtype
        convert_dates(df_raw, date_columns=["date"])
        assert df_raw["date"].dtype == original_dtype

    def test_empty_dataframe(self):
        """DataFrame vide traité sans erreur."""
        df = pd.DataFrame(columns=["date", "scheduled_departure"])
        result = convert_dates(df, timestamp_columns=["scheduled_departure"], date_columns=["date"])
        assert result.empty