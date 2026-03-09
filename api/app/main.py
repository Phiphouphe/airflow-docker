import logging
import time

from fastapi import FastAPI, Depends, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordRequestForm

from app.auth import authenticate_user, create_access_token, require_user
from app.models.schemas import TokenResponse
from app.routes.flights import router as flights_router

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Application ───────────────────────────────────────────────────────────────

app = FastAPI(
    title="Flight Delay Prediction API",
    description="API de prédiction de retards de vols basée sur les modèles MLflow.",
    version="1.0.0",
)

# ── CORS ──────────────────────────────────────────────────────────────────────

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Middleware de logs ────────────────────────────────────────────────────────

@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    duration = round(time.time() - start_time, 4)
    logger.info(
        f"{request.method} {request.url.path} | "
        f"IP: {request.client.host} | "
        f"Status: {response.status_code} | "
        f"{duration}s"
    )
    return response

# ── Routes ────────────────────────────────────────────────────────────────────

@app.post(
    "/auth/login",
    response_model=TokenResponse,
    summary="Connexion",
    description="Retourne un token JWT valable 60 minutes.",
    tags=["Auth"],
)
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    user = authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Nom d'utilisateur ou mot de passe incorrect",
            headers={"WWW-Authenticate": "Bearer"},
        )
    token = create_access_token(data={"sub": user["username"]})
    return {"access_token": token, "token_type": "bearer"}


@app.get(
    "/health",
    summary="Health check",
    description="Vérifie que l'API est en ligne.",
    tags=["System"],
)
def health():
    return {"status": "ok"}


app.include_router(flights_router, tags=["Flights"])