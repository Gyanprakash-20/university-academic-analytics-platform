"""
API Security: JWT Authentication + Role-based authorization
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Annotated

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel

from config.settings import settings

# ── Setup ──────────────────────────────────────────────────────────────────────
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/token")

# ── Demo user store (replace with DB in production) ───────────────────────────
def get_fake_users_db() -> dict[str, dict]:
    return {
        "admin": {
            "username": "admin",
            "hashed_password": pwd_context.hash("admin123"),
            "role": "UNIVERSITY_ADMIN",
            "department_id": None,
        },
        "analyst": {
            "username": "analyst",
            "hashed_password": pwd_context.hash("analyst123"),
            "role": "DATA_ANALYST",
            "department_id": None,
        },
        "dept_cs": {
            "username": "dept_cs",
            "hashed_password": pwd_context.hash("cs2024"),
            "role": "DEPARTMENT_ADMIN",
            "department_id": "CS001",
        },
    }
"""FAKE_USERS_DB: dict[str, dict] = {
    "admin": {
        "username": "admin",
        "hashed_password": pwd_context.hash("admin123"),
        "role": "UNIVERSITY_ADMIN",
        "department_id": None,
    },
    "analyst": {
        "username": "analyst",
        "hashed_password": pwd_context.hash("analyst123"),
        "role": "DATA_ANALYST",
        "department_id": None,
    },
    "dept_cs": {
        "username": "dept_cs",
        "hashed_password": pwd_context.hash("cs2024"),
        "role": "DEPARTMENT_ADMIN",
        "department_id": "CS001",
    },
}"""


# ── Pydantic models ────────────────────────────────────────────────────────────
class Token(BaseModel):
    access_token: str
    token_type:   str = "bearer"


class TokenData(BaseModel):
    username:      str
    role:          str
    department_id: str | None = None


class User(BaseModel):
    username:      str
    role:          str
    department_id: str | None = None


# ── Helpers ────────────────────────────────────────────────────────────────────
def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)

"""
def authenticate_user(username: str, password: str) -> User | None:
    user = FAKE_USERS_DB.get(username)     """
def authenticate_user(username: str, password: str) -> User | None:
    users = get_fake_users_db()
    user = users.get(username)    
    if not user or not verify_password(password, user["hashed_password"]):
        return None
    return User(**{k: v for k, v in user.items() if k != "hashed_password"})


def create_access_token(data: dict) -> str:
    payload = data.copy()
    expire  = datetime.utcnow() + timedelta(minutes=settings.jwt_expire_minutes)
    payload.update({"exp": expire})
    return jwt.encode(payload, settings.jwt_secret_key, algorithm=settings.jwt_algorithm)


async def get_current_user(token: Annotated[str, Depends(oauth2_scheme)]) -> User:
    credentials_exc = HTTPException(
        status_code = status.HTTP_401_UNAUTHORIZED,
        detail      = "Could not validate credentials",
        headers     = {"WWW-Authenticate": "Bearer"},
    )
    try:
        payload  = jwt.decode(token, settings.jwt_secret_key, algorithms=[settings.jwt_algorithm])
        username = payload.get("sub")
        role     = payload.get("role")
        dept     = payload.get("department_id")
        if username is None:
            raise credentials_exc
        return User(username=username, role=role, department_id=dept)
    except JWTError:
        raise credentials_exc


# ── Role guards ────────────────────────────────────────────────────────────────
def require_roles(*allowed_roles: str):
    """Dependency factory that restricts endpoint to specific roles."""
    async def guard(current_user: Annotated[User, Depends(get_current_user)]) -> User:
        if current_user.role not in allowed_roles:
            raise HTTPException(
                status_code = status.HTTP_403_FORBIDDEN,
                detail      = f"Role '{current_user.role}' is not authorized for this endpoint",
            )
        return current_user
    return guard


AdminOnly   = Depends(require_roles("UNIVERSITY_ADMIN"))
AnalystPlus = Depends(require_roles("UNIVERSITY_ADMIN", "DATA_ANALYST", "DEPARTMENT_ADMIN"))