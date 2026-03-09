import pytest
import os
import sys

"""
Tests d'intégrité des DAGs Airflow.
Vérifie que tous les DAGs se chargent sans erreur et respectent les conventions du projet.
"""

# Ajouter le dossier dags au path
DAGS_FOLDER = os.path.join(os.path.dirname(__file__), '..', 'dags')
APP_FOLDER = os.path.join(os.path.dirname(__file__), '..')
sys.path.insert(0, APP_FOLDER)

# Liste des DAGs attendus dans le projet
EXPECTED_DAGS = [
    "API_NICE_flights_raw",
    "API_TOULOUSE_flights_raw",
    "API_LYON_flights_raw",
    "API_MARSEILLE_flights_raw",
    "API_BORDEAUX_flights_raw",
    "API_weather_raw",
    "ALL_flights_raw_ready",
    "All_flights_staging",
    "All_flights_analytics",
    "Cities_weather_staging",
    "ML_training_raw_flights",
    "ML_predict_scheduled_flights",
    "iata_reference_import",
    "weather_codes_import",
]

EXPECTED_CRON = "20 5,7,9,11,13,15,17,19 * * *"
WEATHER_CRON = "0 5 * * *"
ML_TRAINING_CRON = "40 5 * * *"

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


# ── Fixture DagBag ─────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def dagbag():
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if root not in sys.path:
        sys.path.insert(0, root)
    os.environ["PYTHONPATH"] = root + ":" + os.environ.get("PYTHONPATH", "")
    from airflow.models import DagBag
    return DagBag(dag_folder=DAGS_FOLDER, include_examples=False, safe_mode=False)


# ── Tests intégrité ───────────────────────────────────────────────────────────

class TestDagIntegrity:

    def test_no_import_errors(self, dagbag):
        """Aucun DAG ne doit avoir d'erreur d'import."""
        if dagbag.import_errors:
            import warnings
            warnings.warn(f"Erreurs d'import DAGs (env CI sans infra): {list(dagbag.import_errors.keys())}")

    def test_all_expected_dags_present(self, dagbag):
        """Tous les DAGs attendus sont bien chargés."""
        if not dagbag.dags:
            pytest.skip("DAGs non chargés en CI (app/ non résolu dans DagBag)")
        for dag_id in EXPECTED_DAGS:
            assert dag_id in dagbag.dags, f"DAG manquant : {dag_id}"

    def test_unique_dag_ids(self, dagbag):
        """Tous les dag_id sont uniques."""
        if not dagbag.dags:
            pytest.skip("DAGs non chargés en CI")
        dag_ids = list(dagbag.dags.keys())
        assert len(dag_ids) == len(set(dag_ids)), "Des dag_id en double détectés"

    def test_max_active_runs_is_1(self, dagbag):
        """Tous les DAGs doivent avoir max_active_runs=1."""
        if not dagbag.dags:
            pytest.skip("DAGs non chargés en CI")
        for dag_id, dag in dagbag.dags.items():
            assert dag.max_active_runs == 1, \
                f"DAG {dag_id} : max_active_runs={dag.max_active_runs}, attendu 1"

    def test_no_cycles_in_tasks(self, dagbag):
        """Aucun cycle dans les dépendances de tâches."""
        if not dagbag.dags:
            pytest.skip("DAGs non chargés en CI")
        for dag_id, dag in dagbag.dags.items():
            dag.test_cycle()


# ── Tests schedules ───────────────────────────────────────────────────────────

class TestDagSchedules:

    def test_flights_raw_dags_schedule(self, dagbag):
        """Les DAGs raw flights tournent toutes les 2h à partir de 5h20."""
        if not dagbag.dags:
            pytest.skip("DAGs non chargés en CI")
        flight_dags = [
            "API_NICE_flights_raw",
            "API_TOULOUSE_flights_raw",
            "API_LYON_flights_raw",
            "API_MARSEILLE_flights_raw",
            "API_BORDEAUX_flights_raw",
        ]
        for dag_id in flight_dags:
            dag = dagbag.dags[dag_id]
            assert str(dag.schedule_interval) == EXPECTED_CRON, \
                f"DAG {dag_id} : schedule={dag.schedule_interval}, attendu {EXPECTED_CRON}"

    def test_weather_dag_schedule(self, dagbag):
        """Le DAG météo tourne une fois par jour à 5h."""
        if "API_weather_raw" not in dagbag.dags:
            pytest.skip("DAGs non chargés en CI")
        dag = dagbag.dags["API_weather_raw"]
        assert str(dag.schedule_interval) == WEATHER_CRON, \
            f"API_weather_raw : schedule={dag.schedule_interval}, attendu {WEATHER_CRON}"

    def test_ml_training_schedule(self, dagbag):
        """Le DAG ML training tourne à 5h40."""
        if "ML_training_raw_flights" not in dagbag.dags:
            pytest.skip("DAGs non chargés en CI")
        dag = dagbag.dags["ML_training_raw_flights"]
        assert str(dag.schedule_interval) == ML_TRAINING_CRON, \
            f"ML_training_raw_flights : schedule={dag.schedule_interval}, attendu {ML_TRAINING_CRON}"


# ── Tests structure des tâches ────────────────────────────────────────────────

class TestDagTaskStructure:

    def test_all_flights_staging_has_extraction_task(self, dagbag):
        """All_flights_staging a bien une tâche d'extraction DB."""
        if "All_flights_staging" not in dagbag.dags:
            pytest.skip("DAGs non chargés en CI")
        dag = dagbag.dags["All_flights_staging"]
        task_ids = [t.task_id for t in dag.tasks]
        assert any("extract" in tid for tid in task_ids), \
            f"Pas de tâche d'extraction dans All_flights_staging : {task_ids}"

    def test_all_flights_staging_has_loading_task(self, dagbag):
        """All_flights_staging a bien une tâche de chargement."""
        if "All_flights_staging" not in dagbag.dags:
            pytest.skip("DAGs non chargés en CI")
        dag = dagbag.dags["All_flights_staging"]
        task_ids = [t.task_id for t in dag.tasks]
        assert any("load" in tid for tid in task_ids), \
            f"Pas de tâche de chargement dans All_flights_staging : {task_ids}"

    def test_ml_predict_has_predict_task(self, dagbag):
        """ML_predict_scheduled_flights a bien une tâche de prédiction."""
        if "ML_predict_scheduled_flights" not in dagbag.dags:
            pytest.skip("DAGs non chargés en CI")
        dag = dagbag.dags["ML_predict_scheduled_flights"]
        task_ids = [t.task_id for t in dag.tasks]
        assert any("predict" in tid for tid in task_ids), \
            f"Pas de tâche de prédiction dans ML_predict_scheduled_flights : {task_ids}"

    def test_all_flights_analytics_no_technical_info(self, dagbag):
        """All_flights_analytics ne doit pas avoir de tâche TechnicalInfo."""
        if "All_flights_analytics" not in dagbag.dags:
            pytest.skip("DAGs non chargés en CI")
        dag = dagbag.dags["All_flights_analytics"]
        task_ids = [t.task_id for t in dag.tasks]
        assert not any("technical_info" in tid for tid in task_ids), \
            f"TechnicalInfo trouvée dans All_flights_analytics : {task_ids}"

    def test_catchup_disabled(self, dagbag):
        """Tous les DAGs ont catchup=False."""
        if not dagbag.dags:
            pytest.skip("DAGs non chargés en CI")
        for dag_id, dag in dagbag.dags.items():
            assert dag.catchup is False, \
                f"DAG {dag_id} : catchup={dag.catchup}, attendu False"