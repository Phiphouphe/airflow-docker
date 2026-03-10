from fastapi import FastAPI, Request
import subprocess
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

SERVICE_MAP = {
    "FastAPIDown": "flight_api",
    "AirflowDown": "airflow-docker-airflow-apiserver-1",
    "DAGFailed": None
}

@app.post("/webhook")
async def handle_alert(request: Request):
    data = await request.json()
    alerts = data.get("alerts", [])
    
    for alert in alerts:
        alert_name = alert.get("labels", {}).get("alertname")
        status = alert.get("status")
        
        logger.info(f"Received alert: {alert_name} - {status}")
        
        if status == "firing" and alert_name in SERVICE_MAP:
            service = SERVICE_MAP.get(alert_name)
            if service:
                logger.info(f"Restarting service: {service}")
                subprocess.run(["docker", "restart", service])
    
    return {"status": "ok"}