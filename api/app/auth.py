import os
from datetime import datetime, timedelta
from typing import Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from passlib.context import CryptContext
from dotenv import load_dotenv

load_dotenv()

# ── Configuration ─────────────────────────────────────────────────────────────

SECRET_KEY = os.getenv("API_SECRET_KEY", "changeme_in_production")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

# ── Utilisateurs ──────────────────────────────────────────────────────────────

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

USERS_DB = {
    "admin": {
        "username": "admin",
        "role": "admin",
        "hashed_password": pwd_context.hash(os.getenv("API_ADMIN_PASSWORD", "admin")),
    },
    "user": {
        "username": "user",
        "role": "user",
        "hashed_password": pwd_context.hash(os.getenv("API_USER_PASSWORD", "user")),
    }
}

# ── OAuth2 ────────────────────────────────────────────────────────────────────

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")


# ── Fonctions utilitaires ─────────────────────────────────────────────────────

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)


def authenticate_user(username: str, password: str) -> Optional[dict]:
    user = USERS_DB.get(username)
    if not user:
        return None
    if not verify_password(password, user["hashed_password"]):
        return None
    return user


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def get_current_user(token: str = Depends(oauth2_scheme)) -> dict:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Token invalide ou expiré",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception

    user = USERS_DB.get(username)
    if user is None:
        raise credentials_exception
    return user


def require_role(role: str):
    """Vérifie que l'utilisateur a le rôle requis."""
    def role_checker(current_user: dict = Depends(get_current_user)):
        if current_user["role"] != role and current_user["role"] != "admin":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Accès refusé. Rôle requis : {role}",
            )
        return current_user
    return role_checker


# ── Dépendances prêtes à l'emploi ─────────────────────────────────────────────

require_user = require_role("user")
require_admin = require_role("admin")