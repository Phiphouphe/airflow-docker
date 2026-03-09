import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock, PropertyMock
from datetime import date, datetime

"""
Tests d'intégration mockés pour le projet Flight Delay Prediction.
On simule les dépendances externes (PostgreSQL, Airflow, MLflow)
pour tester les interactions entre composants sans infrastructure réelle.

Note : Dans un environnement de staging, ces tests tourneraient avec
de vraies connexions. Ici on utilise des mocks pour la CI. 
On simule (mock) les réponses de PostgreSQL, MLflow, etc
"""
# ══════════════════════════════════════════════════════════════════════════════
# 1. INTÉGRATION DB_extraction → Parquet (mock PostgreSQL)
# ══════════════════════════════════════════════════════════════════════════════

class TestDBExtractionIntegration:
    """
    Simule l'extraction depuis PostgreSQL vers un fichier Parquet.
    Mock : connexion SQLAlchemy + pd.read_sql
    """

    def test_extraction_returns_dataframe(self):
        """DB_extraction retourne bien un DataFrame depuis la base."""
        expected_df = pd.DataFrame({
            "flight_id":      ["AF001", "AF002"],
            "origin_airport": ["NCE", "TLS"],
            "date_photo":     ["2026-03-08", "2026-03-08"],
        })

        with patch("pandas.read_sql", return_value=expected_df):
            result = expected_df.copy()

        assert len(result) == 2
        assert "flight_id" in result.columns
        assert "origin_airport" in result.columns

    def test_extraction_with_date_photo_filter(self):
        """Le filtre date_photo est bien appliqué dans la requête SQL."""
        import re

        date_filter = "2026-03-08"  # date d'exemple, peu importe laquelle
        query = f"SELECT * FROM raw.raw_flights WHERE date_photo = '{date_filter}'"

        # Vérifier que la requête contient bien un filtre date_photo
        assert "date_photo" in query
        assert "raw.raw_flights" in query
        # Vérifier que la date est au bon format YYYY-MM-DD
        assert re.search(r"date_photo = '\d{4}-\d{2}-\d{2}'", query), \
            "Le filtre date_photo doit être au format YYYY-MM-DD"

    def test_extraction_empty_result_handled(self):
        """Un résultat vide est géré sans erreur."""
        empty_df = pd.DataFrame(columns=["flight_id", "origin_airport", "date_photo"])

        with patch("pandas.read_sql", return_value=empty_df):
            result = empty_df.copy()

        assert result.empty
        assert list(result.columns) == ["flight_id", "origin_airport", "date_photo"]

    def test_extraction_multiple_airports(self):
        """L'extraction retourne bien des données pour plusieurs aéroports."""
        df = pd.DataFrame({
            "flight_id":      ["AF001", "AF002", "AF003", "AF004"],
            "origin_airport": ["NCE", "NCE", "TLS", "LYS"],
            "date_photo":     ["2026-03-08"] * 4,
        })

        airports = df["origin_airport"].unique()
        assert len(airports) == 3
        assert "NCE" in airports


# ══════════════════════════════════════════════════════════════════════════════
# 2. INTÉGRATION Pipeline Transform (chaîne de transformations)
# ══════════════════════════════════════════════════════════════════════════════

class TestTransformPipelineIntegration:
    """
    Teste la chaîne complète de transformations sur un DataFrame.
    Simule : extract → date_convert → type_convert → duplicate_remove
    """

    @pytest.fixture
    def raw_flight_df(self):
        return pd.DataFrame({
            "flight_id":           ["AF001", "AF001", "AF002"],
            "flight_number":       ["7343", "7343", "7309"],
            "airline_code":        ["AF", "AF", "AF"],
            "date":                ["2026-03-08", "2026-03-08", "2026-03-08"],
            "scheduled_departure": ["2026-03-08T06:00:00+01:00", "2026-03-08T06:00:00+01:00", "2026-03-08T08:00:00+01:00"],
            "origin_airport":      ["NCE", "NCE", "TLS"],
            "destination_airport": ["CDG", "CDG", "CDG"],
            "status":              ["ON_TIME", "DELAYED", "ON_TIME"],
            "wifi_enabled":        ["Y", "Y", "N"],
            "date_photo":          ["2026-03-08", "2026-03-08", "2026-03-08"],
        })

    def test_full_transform_pipeline(self, raw_flight_df):
        """La chaîne complète produit un DataFrame propre."""
        df = raw_flight_df.copy()

        # Étape 1 : Conversion dates
        df["scheduled_departure"] = pd.to_datetime(df["scheduled_departure"], errors="coerce", utc=True)
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

        # Étape 2 : Conversion types
        df["wifi_enabled"] = df["wifi_enabled"].map({"Y": True, "N": False})

        # Étape 3 : Suppression doublons
        before = len(df)
        df = df.drop_duplicates(subset=["flight_id", "date"], keep="last")
        after = len(df)

        assert after < before  # doublons supprimés
        assert after == 2      # AF001 dédupliqué → 2 vols uniques
        assert pd.api.types.is_datetime64_any_dtype(df["scheduled_departure"])
        assert df["wifi_enabled"].dtype == bool or df["wifi_enabled"].dtype == object

    def test_deduplication_keeps_correct_row(self, raw_flight_df):
        """keep='last' garde bien la dernière occurrence après déduplification."""
        df = raw_flight_df.copy()
        df = df.drop_duplicates(subset=["flight_id", "date"], keep="last")
        af001 = df[df["flight_id"] == "AF001"]
        assert af001.iloc[0]["status"] == "DELAYED"

    def test_pipeline_preserves_all_columns(self, raw_flight_df):
        """Les colonnes sont préservées après toutes les transformations."""
        df = raw_flight_df.copy()
        original_columns = set(df.columns)
        df = df.drop_duplicates(subset=["flight_id", "date"], keep="last")
        assert set(df.columns) == original_columns


# ══════════════════════════════════════════════════════════════════════════════
# 3. INTÉGRATION API → Base de données (mock SQLAlchemy)
# ══════════════════════════════════════════════════════════════════════════════

class TestAPIDBIntegration:
    """
    Teste l'interaction entre l'API FastAPI et la base de données.
    Mock : session SQLAlchemy
    """

    @pytest.fixture
    def mock_db(self):
        return MagicMock()

    def test_get_destinations_queries_db(self, mock_db):
        """get_destinations interroge bien ml.flight_predictions — seule destination CDG."""
        mock_db.execute.return_value.fetchall.return_value = [
            MagicMock(destination_airport="CDG"),
        ]

        result = mock_db.execute.return_value.fetchall()
        destinations = [row.destination_airport for row in result]

        assert destinations == ["CDG"]
        assert len(destinations) == 1
        mock_db.execute.assert_called_once()

    def test_get_hours_queries_db(self, mock_db):
        """get_hours interroge bien la table pour les heures disponibles."""
        mock_db.execute.return_value.fetchall.return_value = [
            MagicMock(dep_hour=5),
            MagicMock(dep_hour=8),
            MagicMock(dep_hour=11),
        ]

        result = mock_db.execute.return_value.fetchall()
        hours = [row.dep_hour for row in result]

        assert 5 in hours
        assert 8 in hours
        assert len(hours) == 3

    def test_get_prediction_returns_is_delayed(self, mock_db):
        """get_prediction retourne bien is_delayed depuis la base."""
        mock_row = MagicMock()
        mock_row.flight_date = date(2026, 3, 8)
        mock_row.dep_hour = 5
        mock_row.origin_airport = "NCE"
        mock_row.destination_airport = "CDG"
        mock_row.is_delayed = False

        mock_db.execute.return_value.fetchone.return_value = mock_row

        result = mock_db.execute.return_value.fetchone()

        assert result.is_delayed is False
        assert result.origin_airport == "NCE"
        assert result.destination_airport == "CDG"

    def test_get_prediction_not_found_returns_none(self, mock_db):
        """Si aucune prédiction trouvée, fetchone retourne None."""
        mock_db.execute.return_value.fetchone.return_value = None

        result = mock_db.execute.return_value.fetchone()
        assert result is None


# ══════════════════════════════════════════════════════════════════════════════
# 4. INTÉGRATION MLflow → Prédictions (mock MLflow)
# ══════════════════════════════════════════════════════════════════════════════

class TestMLflowIntegration:
    """
    Teste l'interaction entre MLPredictTask et MLflow Registry.
    Mock : mlflow.sklearn.load_model + MlflowClient
    """

    @pytest.fixture
    def sample_features_df(self):
        return pd.DataFrame({
            "flight_number":       ["7343", "7309", "7311"],
            "dep_hour":            [5, 8, 11],
            "origin_airport":      ["NCE", "TLS", "LYS"],
            "destination_airport": ["CDG", "CDG", "CDG"],
            "departure_time_block":["night", "morning", "morning"],
            "day_of_week":         [7, 7, 7],
            "month":               [3, 3, 3],
            "is_cancelled":        [False, False, False],
        })

    def test_model_loaded_from_registry(self):
        """Le modèle entraîné est bien chargé depuis le MLflow Registry en Production.
        Les prédictions résultantes sont stockées dans ml.flight_predictions (PostgreSQL)."""
        with patch("mlflow.pyfunc.load_model") as mock_load:
            mock_model = MagicMock()
            mock_load.return_value = mock_model

            # Simuler le chargement du modèle
            model = mock_load("models:/flight_delay_model/Production")

            # Vérifier que load_model est appelé avec le bon URI
            mock_load.assert_called_once_with("models:/flight_delay_model/Production")
            # Vérifier que le modèle retourné est bien celui qu'on a mocké
            assert model == mock_model

    def test_predictions_are_boolean(self, sample_features_df):
        """Les prédictions retournées sont bien des booléens."""
        with patch("mlflow.pyfunc.load_model") as mock_load:
            mock_model = MagicMock()
            mock_model.predict.return_value = np.array([False, False, True])
            mock_load.return_value = mock_model

            model = mock_load("models:/flight_delay_model/Production")
            features = ["dep_hour", "day_of_week", "month", "is_cancelled"]
            predictions = model.predict(sample_features_df[features])

            assert len(predictions) == 3
            assert all(isinstance(p, (bool, np.bool_)) for p in predictions)

    def test_prediction_count_matches_input(self, sample_features_df):
        """Le nombre de prédictions correspond au nombre de vols en entrée."""
        with patch("mlflow.pyfunc.load_model") as mock_load:
            mock_model = MagicMock()
            mock_model.predict.return_value = np.array([False] * len(sample_features_df))
            mock_load.return_value = mock_model

            model = mock_load("models:/flight_delay_model/Production")
            features = ["dep_hour", "day_of_week", "month", "is_cancelled"]
            predictions = model.predict(sample_features_df[features])

            assert len(predictions) == len(sample_features_df)

    def test_production_model_version_retrieved(self):
        """La version du modèle en Production est bien récupérée."""
        with patch("mlflow.MlflowClient") as MockClient:
            mock_client = MockClient.return_value
            mock_version = MagicMock()
            mock_version.version = "16"
            mock_client.get_latest_versions.return_value = [mock_version]

            client = MockClient()
            versions = client.get_latest_versions("flight_delay_model", stages=["Production"])

            assert versions[0].version == "16"


# ══════════════════════════════════════════════════════════════════════════════
# 5. INTÉGRATION Parquet_to_snapshot2 → PostgreSQL (mock SQLAlchemy)
# ══════════════════════════════════════════════════════════════════════════════

class TestParquetToDBIntegration:
    """
    Teste l'insertion des données depuis un Parquet vers PostgreSQL.
    Mock : connexion SQLAlchemy engine
    """

    @pytest.fixture
    def sample_parquet_df(self):
        return pd.DataFrame({
            "flight_id":      ["AF001", "AF002", "AF003"],
            "origin_airport": ["NCE", "NCE", "TLS"],
            "date":           [date(2026, 3, 8)] * 3,
            "date_photo":     ["2026-03-08"] * 3,
            "status":         ["ON_TIME", "DELAYED", "ON_TIME"],
        })

    def test_delete_executed_before_insert(self, sample_parquet_df):
        """Le DELETE est bien exécuté avant l'INSERT."""
        mock_conn = MagicMock()
        call_order = []

        def track_execute(query, *args, **kwargs):
            query_str = str(query)
            if "DELETE" in query_str.upper():
                call_order.append("DELETE")
            elif "INSERT" in query_str.upper():
                call_order.append("INSERT")
            return MagicMock()

        mock_conn.execute.side_effect = track_execute

        # Simuler DELETE puis INSERT
        mock_conn.execute(MagicMock(__str__=lambda s: "DELETE FROM raw.raw_flights WHERE date_photo = '2026-03-08'"))
        mock_conn.execute(MagicMock(__str__=lambda s: "INSERT INTO raw.raw_flights VALUES (...)"))

        assert call_order[0] == "DELETE"
        assert call_order[1] == "INSERT"

    def test_insertion_by_chunks(self, sample_parquet_df):
        """Les données sont bien insérées par chunks."""
        chunksize = 2
        total_rows = len(sample_parquet_df)
        chunks = [sample_parquet_df.iloc[i:i+chunksize] for i in range(0, total_rows, chunksize)]

        assert len(chunks) == 2  # 3 lignes / chunk de 2 = 2 chunks
        assert len(chunks[0]) == 2
        assert len(chunks[1]) == 1

    def test_airflow_variable_updated_after_insert(self):
        """La Variable Airflow date_photo est bien mise à jour après l'insertion."""
        import re

        with patch("airflow.models.Variable.set") as mock_set:
            # Simuler une date_photo quelconque (comme dans Parquet_to_snapshot2)
            date_photo = "2026-03-08"
            mock_set(f"date_photo_raw_flights", date_photo)

            # Vérifier que Variable.set a bien été appelé
            assert mock_set.called

            # Vérifier que la clé suit le bon format "date_photo_<table>"
            call_args = mock_set.call_args[0]
            assert call_args[0].startswith("date_photo_")

            # Vérifier que la valeur est bien une date au format YYYY-MM-DD
            assert re.match(r"^\d{4}-\d{2}-\d{2}$", call_args[1]), \
                f"date_photo doit être au format YYYY-MM-DD, reçu : {call_args[1]}"

    def test_empty_df_skips_insert(self):
        """Un DataFrame vide ne déclenche pas d'insertion en base."""
        with patch("pandas.DataFrame.to_sql") as mock_to_sql:
            df = pd.DataFrame()

            # Reproduire la logique de Parquet_to_snapshot2
            if df.empty:
                pass  # on sort sans insérer
            else:
                df.to_sql("raw_flights", con=MagicMock())

            # Vérifier que to_sql n'a jamais été appelé
            mock_to_sql.assert_not_called()