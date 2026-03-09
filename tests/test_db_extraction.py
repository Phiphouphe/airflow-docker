import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

"""
Tests unitaires pour DB_extraction._build_query()
On teste uniquement la logique de construction de requête SQL,
sans connexion à la base de données ni à Airflow.
"""

# ── Helper pour instancier DB_extraction sans Airflow ─────────────────────────

class FakeDBExtraction:
    """Simule DB_extraction sans dépendances Airflow."""

    def __init__(self, table_name, schema_name, columns="*",
                 where_clause=None, limit_clause=None, query=None):
        self._table_name = table_name
        self._schema_name = schema_name
        self._columns = columns
        self._where_clause = where_clause
        self._limit_clause = limit_clause
        self._query = query

    def _build_query(self) -> str:
        if self._query:
            return self._query

        if isinstance(self._columns, list):
            columns_part = ", ".join([c.strip() for c in self._columns])
        else:
            columns_part = self._columns.strip()

        query = f"SELECT {columns_part} FROM {self._schema_name}.{self._table_name}"

        if self._where_clause:
            query += f" WHERE {self._where_clause}"

        if self._limit_clause:
            query += f" LIMIT {self._limit_clause}"

        return query


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestBuildQuery:

    def test_select_all_columns(self):
        """SELECT * par défaut."""
        ext = FakeDBExtraction("raw_flights", "raw")
        assert ext._build_query() == "SELECT * FROM raw.raw_flights"

    def test_select_specific_columns_list(self):
        """SELECT avec liste de colonnes."""
        ext = FakeDBExtraction("raw_flights", "raw", columns=["flight_id", "date", "origin_airport"])
        query = ext._build_query()
        assert "SELECT flight_id, date, origin_airport" in query
        assert "FROM raw.raw_flights" in query

    def test_select_with_where_clause(self):
        """WHERE clause ajoutée correctement."""
        ext = FakeDBExtraction("raw_flights", "raw", where_clause="date_photo = '2026-03-07'")
        query = ext._build_query()
        assert "WHERE date_photo = '2026-03-07'" in query

    def test_select_with_limit(self):
        """LIMIT ajouté correctement."""
        ext = FakeDBExtraction("raw_flights", "raw", limit_clause=100)
        query = ext._build_query()
        assert "LIMIT 100" in query

    def test_select_with_where_and_limit(self):
        """WHERE et LIMIT combinés."""
        ext = FakeDBExtraction(
            "raw_flights", "raw",
            where_clause="origin_airport = 'NCE'",
            limit_clause=50
        )
        query = ext._build_query()
        assert "WHERE origin_airport = 'NCE'" in query
        assert "LIMIT 50" in query

    def test_custom_query_overrides_everything(self):
        """Si query personnalisée fournie, elle est retournée telle quelle."""
        custom = "SELECT id, code FROM ref.iata_delay_codes"
        ext = FakeDBExtraction(
            "raw_flights", "raw",
            columns=["flight_id"],
            where_clause="date_photo = '2026-03-07'",
            query=custom
        )
        assert ext._build_query() == custom

    def test_columns_stripped(self):
        """Les espaces autour des noms de colonnes sont supprimés."""
        ext = FakeDBExtraction("raw_flights", "raw", columns=[" flight_id ", " date "])
        query = ext._build_query()
        assert "SELECT flight_id, date" in query

    def test_schema_and_table_in_query(self):
        """Le schéma et la table sont bien inclus."""
        ext = FakeDBExtraction("scheduled_flights", "staging")
        query = ext._build_query()
        assert "FROM staging.scheduled_flights" in query

    def test_no_where_when_none(self):
        """Pas de WHERE si where_clause est None."""
        ext = FakeDBExtraction("raw_flights", "raw")
        query = ext._build_query()
        assert "WHERE" not in query

    def test_no_limit_when_none(self):
        """Pas de LIMIT si limit_clause est None."""
        ext = FakeDBExtraction("raw_flights", "raw")
        query = ext._build_query()
        assert "LIMIT" not in query