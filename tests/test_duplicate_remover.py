import pytest
import pandas as pd
import numpy as np

"""
Tests unitaires pour la logique de DuplicateRemover.
On teste la logique de déduplication sur des DataFrames pandas,
sans dépendances Airflow ni fichiers Parquet.
"""

# ── Helper qui reproduit la logique de DuplicateRemover._run() ────────────────

def remove_duplicates(df: pd.DataFrame, key_columns: list, keep: str = "last") -> pd.DataFrame:
    """Reproduit la logique de déduplication de DuplicateRemover."""
    return df.drop_duplicates(subset=key_columns, keep=keep)


def check_nulls(df: pd.DataFrame, threshold_percent: float = 50.0) -> list:
    """Retourne les colonnes qui dépassent le seuil de nulls."""
    flagged = []
    for col in df.columns:
        pct = df[col].isna().mean() * 100
        if pct >= threshold_percent:
            flagged.append(col)
    return flagged


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def df_with_duplicates():
    return pd.DataFrame({
        "flight_id": ["AF001", "AF001", "AF002", "AF003", "AF003"],
        "date":      ["2026-03-07", "2026-03-07", "2026-03-07", "2026-03-07", "2026-03-07"],
        "status":    ["ON_TIME", "DELAYED", "ON_TIME", "CANCELLED", "ON_TIME"],
        "origin_airport": ["NCE", "NCE", "TLS", "LYS", "LYS"],
    })


@pytest.fixture
def df_no_duplicates():
    return pd.DataFrame({
        "flight_id": ["AF001", "AF002", "AF003"],
        "date":      ["2026-03-07", "2026-03-07", "2026-03-07"],
        "status":    ["ON_TIME", "DELAYED", "CANCELLED"],
    })


@pytest.fixture
def df_with_nulls():
    return pd.DataFrame({
        "flight_id":      ["AF001", "AF002", "AF003", "AF004"],
        "origin_airport": [None, None, None, "NCE"],       # 75% nulls → dépasse seuil 50%
        "status":         ["ON_TIME", None, "DELAYED", "CANCELLED"],  # 25% nulls → ok
    })


# ── Tests déduplication ───────────────────────────────────────────────────────

class TestRemoveDuplicates:

    def test_duplicates_removed(self, df_with_duplicates):
        """Les doublons sont bien supprimés."""
        result = remove_duplicates(df_with_duplicates, key_columns=["flight_id", "date"])
        assert len(result) == 3

    def test_keep_last(self, df_with_duplicates):
        """keep='last' conserve la dernière occurrence."""
        result = remove_duplicates(df_with_duplicates, key_columns=["flight_id", "date"], keep="last")
        af001 = result[result["flight_id"] == "AF001"]
        assert af001.iloc[0]["status"] == "DELAYED"

    def test_keep_first(self, df_with_duplicates):
        """keep='first' conserve la première occurrence."""
        result = remove_duplicates(df_with_duplicates, key_columns=["flight_id", "date"], keep="first")
        af001 = result[result["flight_id"] == "AF001"]
        assert af001.iloc[0]["status"] == "ON_TIME"

    def test_no_duplicates_unchanged(self, df_no_duplicates):
        """Un DataFrame sans doublons n'est pas modifié."""
        result = remove_duplicates(df_no_duplicates, key_columns=["flight_id", "date"])
        assert len(result) == len(df_no_duplicates)

    def test_multiple_key_columns(self, df_with_duplicates):
        """Déduplication sur plusieurs colonnes clés."""
        result = remove_duplicates(
            df_with_duplicates,
            key_columns=["flight_id", "date", "origin_airport"]
        )
        assert len(result) == 3

    def test_empty_dataframe(self):
        """DataFrame vide retourné tel quel."""
        df = pd.DataFrame(columns=["flight_id", "date"])
        result = remove_duplicates(df, key_columns=["flight_id", "date"])
        assert result.empty

    def test_row_count_decreases(self, df_with_duplicates):
        """Le nombre de lignes diminue après déduplication."""
        result = remove_duplicates(df_with_duplicates, key_columns=["flight_id", "date"])
        assert len(result) < len(df_with_duplicates)


# ── Tests contrôle qualité nulls ──────────────────────────────────────────────

class TestCheckNulls:

    def test_column_exceeds_threshold(self, df_with_nulls):
        """Colonnes avec trop de nulls détectées."""
        flagged = check_nulls(df_with_nulls, threshold_percent=50.0)
        assert "origin_airport" in flagged

    def test_column_below_threshold_not_flagged(self, df_with_nulls):
        """Colonnes sous le seuil non signalées."""
        flagged = check_nulls(df_with_nulls, threshold_percent=50.0)
        assert "status" not in flagged

    def test_no_nulls_no_flags(self, df_no_duplicates):
        """Aucune colonne signalée si pas de nulls."""
        flagged = check_nulls(df_no_duplicates, threshold_percent=50.0)
        assert flagged == []

    def test_threshold_100_flags_nothing(self, df_with_nulls):
        """Seuil à 100% ne signale rien."""
        flagged = check_nulls(df_with_nulls, threshold_percent=100.0)
        assert flagged == []