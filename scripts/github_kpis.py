import os
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_REPO = os.getenv("GITHUB_REPO")
PUSHGATEWAY_URL = "http://localhost:9091"

headers = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

def get_workflow_runs():
    since = (datetime.utcnow() - timedelta(days=7)).isoformat() + "Z"
    url = f"https://api.github.com/repos/{GITHUB_REPO}/actions/runs?created=>{since}&per_page=100"
    response = requests.get(url, headers=headers)
    return response.json().get("workflow_runs", [])

def compute_kpis(runs):
    if not runs:
        return {}

    total = len(runs)
    success = sum(1 for r in runs if r["conclusion"] == "success")
    failed = sum(1 for r in runs if r["conclusion"] == "failure")

    # Deployment Frequency (déploiements par jour sur 7 jours)
    deployment_frequency = total / 7

    # Deployment Success Rate
    success_rate = (success / total * 100) if total > 0 else 0

    # Deployment Cycle Time (temps moyen en secondes)
    durations = []
    for r in runs:
        if r["created_at"] and r["updated_at"]:
            start = datetime.fromisoformat(r["created_at"].replace("Z", "+00:00"))
            end = datetime.fromisoformat(r["updated_at"].replace("Z", "+00:00"))
            durations.append((end - start).total_seconds())
    avg_cycle_time = sum(durations) / len(durations) if durations else 0

    return {
        "deployment_frequency": deployment_frequency,
        "deployment_success_rate": success_rate,
        "deployment_cycle_time_seconds": avg_cycle_time,
        "deployment_total": total,
        "deployment_failed": failed
    }

def push_to_gateway(kpis):
    metrics = ""
    for name, value in kpis.items():
        metrics += f"# TYPE {name} gauge\n"
        metrics += f"{name} {value}\n"

    response = requests.post(
        f"{PUSHGATEWAY_URL}/metrics/job/github_actions",
        data=metrics,
        headers={"Content-Type": "text/plain"}
    )
    print(f"Pushed to gateway: {response.status_code}")

if __name__ == "__main__":
    runs = get_workflow_runs()
    kpis = compute_kpis(runs)
    print(kpis)
    push_to_gateway(kpis)
