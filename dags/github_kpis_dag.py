from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import requests
from datetime import timezone

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def collect_github_kpis():
    GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
    GITHUB_REPO = os.getenv("GITHUB_REPO")
    PUSHGATEWAY_URL = "http://pushgateway:9091"

    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }

    since = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
    url = f"https://api.github.com/repos/{GITHUB_REPO}/actions/runs?created=>{since}&per_page=100"
    response = requests.get(url, headers=headers)
    runs = response.json().get("workflow_runs", [])

    if not runs:
        return

    total = len(runs)
    success = sum(1 for r in runs if r["conclusion"] == "success")
    failed = sum(1 for r in runs if r["conclusion"] == "failure")
    deployment_frequency = total / 7
    success_rate = (success / total * 100) if total > 0 else 0

    durations = []
    for r in runs:
        if r["created_at"] and r["updated_at"]:
            start = datetime.fromisoformat(r["created_at"].replace("Z", "+00:00"))
            end = datetime.fromisoformat(r["updated_at"].replace("Z", "+00:00"))
            durations.append((end - start).total_seconds())
    avg_cycle_time = sum(durations) / len(durations) if durations else 0

    metrics = ""
    for name, value in {
        "deployment_frequency": deployment_frequency,
        "deployment_success_rate": success_rate,
        "deployment_cycle_time_seconds": avg_cycle_time,
        "deployment_total": total,
        "deployment_failed": failed
    }.items():
        metrics += f"# TYPE {name} gauge\n{name} {value}\n"

    requests.post(
        f"{PUSHGATEWAY_URL}/metrics/job/github_actions",
        data=metrics,
        headers={"Content-Type": "text/plain"}
    )

with DAG(
    dag_id="github_kpis_collector",
    default_args=default_args,
    description="Collecte les KPIs DevOps depuis GitHub Actions",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["devops", "kpis"],
) as dag:

    collect_kpis = PythonOperator(
        task_id="collect_github_kpis",
        python_callable=collect_github_kpis,
    )
