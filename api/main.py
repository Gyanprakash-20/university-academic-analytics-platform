"""
FastAPI Application Entry Point
================================
University Academic Analytics REST API
"""
from __future__ import annotations

from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordRequestForm
from typing import Annotated

from api.auth.security import authenticate_user, create_access_token, Token, get_current_user, User
from api.routes import students, departments, analytics
from config.logging_config import logger

# ── App ────────────────────────────────────────────────────────────────────────
app = FastAPI(
    title       = "University Academic Analytics API",
    description = "REST API for querying academic performance, GPA, attendance, and risk analytics.",
    version     = "1.0.0",
    docs_url    = "/docs",
    redoc_url   = "/redoc",
)

# ── CORS ───────────────────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins     = ["http://localhost:8050", "http://dashboard:8050"],
    allow_credentials = True,
    allow_methods     = ["*"],
    allow_headers     = ["*"],
)

# ── Routers ────────────────────────────────────────────────────────────────────
app.include_router(students.router,    prefix="/students",    tags=["Students"])
app.include_router(departments.router, prefix="/departments", tags=["Departments"])
app.include_router(analytics.router,   prefix="/analytics",   tags=["Analytics"])


# ── Auth endpoints ─────────────────────────────────────────────────────────────
@app.post("/auth/token", response_model=Token, tags=["Auth"])
async def login(form_data: Annotated[OAuth2PasswordRequestForm, Depends()]):
    user = authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    token = create_access_token({
        "sub":           user.username,
        "role":          user.role,
        "department_id": user.department_id,
    })
    logger.info(f"Login: {user.username} ({user.role})")
    return Token(access_token=token)


@app.get("/auth/me", response_model=User, tags=["Auth"])
async def get_me(current_user: Annotated[User, Depends(get_current_user)]):
    return current_user


# ── Health ─────────────────────────────────────────────────────────────────────
@app.get("/health", tags=["System"])
async def health():
    return {"status": "ok", "service": "university-analytics-api"}


if __name__ == "__main__":
    import uvicorn
    from config.settings import settings
    uvicorn.run("api.main:app", host=settings.api_host, port=settings.api_port, reload=True)