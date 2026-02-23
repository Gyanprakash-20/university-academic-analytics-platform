"""
Students Router
===============
Endpoints for querying student academic data.
"""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel

from api.auth.security import User, AnalystPlus

router = APIRouter()


# ── Response Models ────────────────────────────────────────────────────────────
class StudentSummary(BaseModel):
    student_id: str
    full_name: str
    department_id: str
    program: str
    semester: int
    gpa: float
    attendance_pct: float
    risk_flag: bool


class StudentDetail(StudentSummary):
    email: str
    enrollment_year: int
    credits_completed: int
    credits_required: int


# ── Sample data (replace with real DB queries against Snowflake / Postgres) ────
_SAMPLE_STUDENTS: list[dict] = [
    {
        "student_id": "STU001",
        "full_name": "Alice Kumar",
        "department_id": "CS001",
        "program": "B.Tech Computer Science",
        "semester": 6,
        "gpa": 8.7,
        "attendance_pct": 92.5,
        "risk_flag": False,
        "email": "alice.kumar@university.edu",
        "enrollment_year": 2021,
        "credits_completed": 120,
        "credits_required": 160,
    },
    {
        "student_id": "STU002",
        "full_name": "Ravi Sharma",
        "department_id": "CS001",
        "program": "B.Tech Computer Science",
        "semester": 4,
        "gpa": 5.2,
        "attendance_pct": 61.0,
        "risk_flag": True,
        "email": "ravi.sharma@university.edu",
        "enrollment_year": 2022,
        "credits_completed": 70,
        "credits_required": 160,
    },
    {
        "student_id": "STU003",
        "full_name": "Priya Mehta",
        "department_id": "EE002",
        "program": "B.Tech Electrical Engineering",
        "semester": 8,
        "gpa": 9.1,
        "attendance_pct": 97.0,
        "risk_flag": False,
        "email": "priya.mehta@university.edu",
        "enrollment_year": 2020,
        "credits_completed": 155,
        "credits_required": 160,
    },
]


def _get_student(student_id: str) -> dict | None:
    for s in _SAMPLE_STUDENTS:
        if s["student_id"] == student_id:
            return s
    return None


# ── Endpoints ──────────────────────────────────────────────────────────────────
@router.get(
    "/",
    response_model=list[StudentSummary],
    summary="List all students",
)
async def list_students(
    current_user: User = AnalystPlus,
    department_id: Optional[str] = Query(None, description="Filter by department"),
    at_risk_only: bool = Query(False, description="Return only at-risk students"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> list[StudentSummary]:
    """Return a paginated list of students with optional filters."""
    data = list(_SAMPLE_STUDENTS)

    # DEPARTMENT_ADMIN can only see their own department
    if current_user.role == "DEPARTMENT_ADMIN" and current_user.department_id:
        data = [s for s in data if s["department_id"] == current_user.department_id]
    elif department_id:
        data = [s for s in data if s["department_id"] == department_id]

    if at_risk_only:
        data = [s for s in data if s["risk_flag"]]

    return [StudentSummary(**s) for s in data[offset : offset + limit]]


@router.get(
    "/{student_id}",
    response_model=StudentDetail,
    summary="Get student details",
)
async def get_student(
    student_id: str,
    current_user: User = AnalystPlus,
) -> StudentDetail:
    """Return full details for a single student."""
    student = _get_student(student_id)
    if not student:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Student not found")

    # DEPARTMENT_ADMIN cannot view students from other departments
    if (
        current_user.role == "DEPARTMENT_ADMIN"
        and current_user.department_id
        and student["department_id"] != current_user.department_id
    ):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access denied")

    return StudentDetail(**student)


@router.get(
    "/{student_id}/risk",
    summary="Get student risk assessment",
)
async def get_student_risk(
    student_id: str,
    current_user: User = AnalystPlus,
) -> dict:
    """Return risk indicators for a student."""
    student = _get_student(student_id)
    if not student:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Student not found")

    risk_factors: list[str] = []
    if student["gpa"] < 6.0:
        risk_factors.append("Low GPA (below 6.0)")
    if student["attendance_pct"] < 75.0:
        risk_factors.append("Low attendance (below 75%)")

    return {
        "student_id": student_id,
        "risk_flag": student["risk_flag"],
        "risk_factors": risk_factors,
        "gpa": student["gpa"],
        "attendance_pct": student["attendance_pct"],
    }