"""
Departments Router
==================
Endpoints for querying department-level data.
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

from api.auth.security import User, AnalystPlus

router = APIRouter()


# ── Response Models ────────────────────────────────────────────────────────────
class DepartmentSummary(BaseModel):
    department_id: str
    name: str
    faculty_count: int
    student_count: int
    avg_gpa: float
    avg_attendance_pct: float


class DepartmentDetail(DepartmentSummary):
    head_of_department: str
    established_year: int
    programs: list[str]


# ── Sample data (replace with real DB queries) ────────────────────────────────
_SAMPLE_DEPARTMENTS: list[dict] = [
    {
        "department_id": "CS001",
        "name": "Computer Science",
        "faculty_count": 24,
        "student_count": 420,
        "avg_gpa": 7.8,
        "avg_attendance_pct": 84.3,
        "head_of_department": "Prof. D. Nair",
        "established_year": 1985,
        "programs": ["B.Tech Computer Science", "M.Tech AI & ML", "PhD Computer Science"],
    },
    {
        "department_id": "EE002",
        "name": "Electrical Engineering",
        "faculty_count": 18,
        "student_count": 310,
        "avg_gpa": 7.4,
        "avg_attendance_pct": 88.1,
        "head_of_department": "Prof. S. Rao",
        "established_year": 1972,
        "programs": ["B.Tech Electrical Engineering", "M.Tech Power Systems"],
    },
    {
        "department_id": "ME003",
        "name": "Mechanical Engineering",
        "faculty_count": 20,
        "student_count": 280,
        "avg_gpa": 7.1,
        "avg_attendance_pct": 86.5,
        "head_of_department": "Prof. A. Verma",
        "established_year": 1968,
        "programs": ["B.Tech Mechanical Engineering", "M.Tech Robotics"],
    },
]


def _get_department(department_id: str) -> dict | None:
    for d in _SAMPLE_DEPARTMENTS:
        if d["department_id"] == department_id:
            return d
    return None


# ── Endpoints ──────────────────────────────────────────────────────────────────
@router.get(
    "/",
    response_model=list[DepartmentSummary],
    summary="List all departments",
)
async def list_departments(
    current_user: User = AnalystPlus,
) -> list[DepartmentSummary]:
    """Return summary metrics for all departments."""
    # DEPARTMENT_ADMIN only sees their own department
    if current_user.role == "DEPARTMENT_ADMIN" and current_user.department_id:
        dept = _get_department(current_user.department_id)
        return [DepartmentSummary(**dept)] if dept else []
    return [DepartmentSummary(**d) for d in _SAMPLE_DEPARTMENTS]


@router.get(
    "/{department_id}",
    response_model=DepartmentDetail,
    summary="Get department details",
)
async def get_department(
    department_id: str,
    current_user: User = AnalystPlus,
) -> DepartmentDetail:
    """Return full details for a single department."""
    dept = _get_department(department_id)
    if not dept:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Department not found")

    if (
        current_user.role == "DEPARTMENT_ADMIN"
        and current_user.department_id
        and department_id != current_user.department_id
    ):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access denied")

    return DepartmentDetail(**dept)


@router.get(
    "/{department_id}/stats",
    summary="Department performance statistics",
)
async def get_department_stats(
    department_id: str,
    current_user: User = AnalystPlus,
) -> dict:
    """Return aggregated performance statistics for a department."""
    dept = _get_department(department_id)
    if not dept:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Department not found")

    return {
        "department_id": department_id,
        "name": dept["name"],
        "avg_gpa": dept["avg_gpa"],
        "avg_attendance_pct": dept["avg_attendance_pct"],
        "student_count": dept["student_count"],
        "faculty_count": dept["faculty_count"],
        "student_faculty_ratio": round(dept["student_count"] / dept["faculty_count"], 2),
    }
