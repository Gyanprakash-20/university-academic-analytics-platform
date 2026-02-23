"""
Analytics Router
================
Endpoints for university-wide academic analytics and KPIs.
"""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Query
from pydantic import BaseModel

from api.auth.security import User, AnalystPlus

router = APIRouter()


# ── Response Models ────────────────────────────────────────────────────────────
class RiskSummary(BaseModel):
    total_students: int
    at_risk_count: int
    at_risk_pct: float
    low_gpa_count: int
    low_attendance_count: int


class GpaTrend(BaseModel):
    semester: int
    avg_gpa: float
    department_id: Optional[str] = None


class AttendanceSummary(BaseModel):
    department_id: str
    department_name: str
    avg_attendance_pct: float
    below_75_pct_count: int


# ── Sample analytics data (replace with Snowflake queries) ───────────────────
_RISK_SUMMARY: dict = {
    "total_students": 1010,
    "at_risk_count": 143,
    "at_risk_pct": 14.16,
    "low_gpa_count": 98,
    "low_attendance_count": 121,
}

_GPA_TRENDS: list[dict] = [
    {"semester": 1, "avg_gpa": 7.9},
    {"semester": 2, "avg_gpa": 7.6},
    {"semester": 3, "avg_gpa": 7.4},
    {"semester": 4, "avg_gpa": 7.2},
    {"semester": 5, "avg_gpa": 7.5},
    {"semester": 6, "avg_gpa": 7.8},
    {"semester": 7, "avg_gpa": 8.0},
    {"semester": 8, "avg_gpa": 8.2},
]

_ATTENDANCE_SUMMARY: list[dict] = [
    {"department_id": "CS001", "department_name": "Computer Science",       "avg_attendance_pct": 84.3, "below_75_pct_count": 38},
    {"department_id": "EE002", "department_name": "Electrical Engineering", "avg_attendance_pct": 88.1, "below_75_pct_count": 21},
    {"department_id": "ME003", "department_name": "Mechanical Engineering", "avg_attendance_pct": 86.5, "below_75_pct_count": 29},
]


# ── Endpoints ──────────────────────────────────────────────────────────────────
@router.get(
    "/risk-summary",
    response_model=RiskSummary,
    summary="University-wide risk summary",
)
async def get_risk_summary(
    current_user: User = AnalystPlus,
) -> RiskSummary:
    """Return aggregate at-risk student counts for the whole university."""
    return RiskSummary(**_RISK_SUMMARY)


@router.get(
    "/gpa-trends",
    response_model=list[GpaTrend],
    summary="GPA trend across semesters",
)
async def get_gpa_trends(
    current_user: User = AnalystPlus,
    department_id: Optional[str] = Query(None, description="Filter by department"),
) -> list[GpaTrend]:
    """Return average GPA per semester, optionally filtered by department."""
    return [GpaTrend(semester=row["semester"], avg_gpa=row["avg_gpa"], department_id=department_id) for row in _GPA_TRENDS]


@router.get(
    "/attendance",
    response_model=list[AttendanceSummary],
    summary="Attendance summary by department",
)
async def get_attendance_summary(
    current_user: User = AnalystPlus,
    department_id: Optional[str] = Query(None, description="Filter by department"),
) -> list[AttendanceSummary]:
    """Return attendance statistics grouped by department."""
    data = list(_ATTENDANCE_SUMMARY)
    if current_user.role == "DEPARTMENT_ADMIN" and current_user.department_id:
        data = [d for d in data if d["department_id"] == current_user.department_id]
    elif department_id:
        data = [d for d in data if d["department_id"] == department_id]
    return [AttendanceSummary(**d) for d in data]


@router.get(
    "/kpis",
    summary="University KPI dashboard",
)
async def get_kpis(
    current_user: User = AnalystPlus,
) -> dict:
    """Return high-level KPIs for the university analytics dashboard."""
    gpa_values = [t["avg_gpa"] for t in _GPA_TRENDS]
    att_values = [d["avg_attendance_pct"] for d in _ATTENDANCE_SUMMARY]
    return {
        "total_students": _RISK_SUMMARY["total_students"],
        "at_risk_pct": _RISK_SUMMARY["at_risk_pct"],
        "overall_avg_gpa": round(sum(gpa_values) / len(gpa_values), 2),
        "overall_avg_attendance_pct": round(sum(att_values) / len(att_values), 2),
        "departments_count": len(_ATTENDANCE_SUMMARY),
    }
