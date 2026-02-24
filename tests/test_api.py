"""
Integration Tests: FastAPI Endpoints
Uses TestClient with generated synthetic data.
Snowflake is NOT required — reads from local CSV files.
"""
from __future__ import annotations

import os
import pytest
import pandas as pd
from pathlib import Path

# Ensure test runs against local data
os.environ.setdefault("ENV",            "development")
os.environ.setdefault("JWT_SECRET_KEY", "test-secret-key-for-unit-tests-only")
os.environ.setdefault("SNOWFLAKE_ACCOUNT",  "test.account")
os.environ.setdefault("SNOWFLAKE_USER",     "testuser")
os.environ.setdefault("SNOWFLAKE_PASSWORD", "testpass")

from fastapi.testclient import TestClient
from api.main import app

RAW_DIR = Path("data/raw")


# ── Helpers ────────────────────────────────────────────────────────────────────
def _ensure_test_data():
    """Generate minimal CSV data if not present."""
    if not (RAW_DIR / "students.csv").exists():
        from etl.extract.extract_sis import extract_sis
        from etl.extract.extract_lms import extract_lms
        extract_sis(output_dir=RAW_DIR)
        lms = extract_lms(output_dir=RAW_DIR)


@pytest.fixture(scope="session", autouse=True)
def setup_test_data():
    _ensure_test_data()


@pytest.fixture(scope="module")
def client():
    with TestClient(app) as c:
        yield c


@pytest.fixture(scope="module")
def admin_token(client):
    """Obtain a JWT token for the admin user."""
    resp = client.post("/auth/token", data={"username": "admin", "password": "admin123"})
    assert resp.status_code == 200
    return resp.json()["access_token"]


@pytest.fixture(scope="module")
def analyst_token(client):
    resp = client.post("/auth/token", data={"username": "analyst", "password": "analyst123"})
    assert resp.status_code == 200
    return resp.json()["access_token"]


@pytest.fixture(scope="module")
def dept_token(client):
    resp = client.post("/auth/token", data={"username": "dept_cs", "password": "cs2024"})
    assert resp.status_code == 200
    return resp.json()["access_token"]


def auth_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


# ── Auth Tests ─────────────────────────────────────────────────────────────────
class TestAuth:
    def test_health_no_auth(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_login_success(self, client):
        resp = client.post("/auth/token", data={"username": "admin", "password": "admin123"})
        assert resp.status_code == 200
        data = resp.json()
        assert "access_token" in data
        assert data["token_type"] == "bearer"

    def test_login_wrong_password(self, client):
        resp = client.post("/auth/token", data={"username": "admin", "password": "wrongpass"})
        assert resp.status_code == 401

    def test_login_unknown_user(self, client):
        resp = client.post("/auth/token", data={"username": "ghost", "password": "x"})
        assert resp.status_code == 401

    def test_get_me(self, client, admin_token):
        resp = client.get("/auth/me", headers=auth_headers(admin_token))
        assert resp.status_code == 200
        assert resp.json()["username"] == "admin"
        assert resp.json()["role"]     == "UNIVERSITY_ADMIN"

    def test_protected_route_no_token(self, client):
        resp = client.get("/students")
        assert resp.status_code == 401

    def test_protected_route_bad_token(self, client):
        resp = client.get("/students", headers={"Authorization": "Bearer invalid.token.here"})
        assert resp.status_code == 401


# ── Student Endpoint Tests ─────────────────────────────────────────────────────
class TestStudentEndpoints:
    def test_list_students_authenticated(self, client, admin_token):
        resp = client.get("/students", headers=auth_headers(admin_token))
        assert resp.status_code == 200
        data = resp.json()
        assert "total" in data
        assert "data"  in data
        assert data["total"] > 0

    def test_list_students_pagination(self, client, admin_token):
        resp = client.get("/students?page=1&page_size=10", headers=auth_headers(admin_token))
        assert resp.status_code == 200
        assert len(resp.json()["data"]) <= 10

    def test_list_students_filter_by_dept(self, client, admin_token):
        resp = client.get("/students?department_id=CS001", headers=auth_headers(admin_token))
        assert resp.status_code == 200
        for s in resp.json()["data"]:
            assert s["department_id"] == "CS001"

    def test_list_students_filter_by_type(self, client, admin_token):
        resp = client.get("/students?student_type=Full-Time", headers=auth_headers(admin_token))
        assert resp.status_code == 200
        for s in resp.json()["data"]:
            assert s["student_type"] == "Full-Time"

    def test_get_student_by_id(self, client, admin_token):
        # Get first student_id from list
        list_resp = client.get("/students?page_size=1", headers=auth_headers(admin_token))
        assert list_resp.status_code == 200
        student_id = list_resp.json()["data"][0]["student_id"]

        resp = client.get(f"/students/{student_id}", headers=auth_headers(admin_token))
        assert resp.status_code == 200
        assert resp.json()["student_id"] == student_id

    def test_get_student_not_found(self, client, admin_token):
        resp = client.get("/students/NONEXISTENT999", headers=auth_headers(admin_token))
        assert resp.status_code == 404

    def test_pii_masking_for_analyst(self, client, analyst_token):
        """Analyst should see masked email."""
        list_resp = client.get("/students?page_size=1", headers=auth_headers(analyst_token))
        student_id = list_resp.json()["data"][0]["student_id"]

        resp = client.get(f"/students/{student_id}", headers=auth_headers(analyst_token))
        assert resp.status_code == 200
        # Email should be masked (contains ***)
        email = resp.json().get("email", "")
        assert "***" in email

    def test_dept_admin_scoped_access(self, client, dept_token):
        """Department admin should only see CS001 students."""
        resp = client.get("/students", headers=auth_headers(dept_token))
        assert resp.status_code == 200
        for s in resp.json()["data"]:
            assert s["department_id"] == "CS001"

    def test_student_enrollments(self, client, admin_token):
        list_resp = client.get("/students?page_size=1", headers=auth_headers(admin_token))
        student_id = list_resp.json()["data"][0]["student_id"]

        resp = client.get(f"/students/{student_id}/enrollments", headers=auth_headers(admin_token))
        assert resp.status_code in (200, 404)  # 404 if student has no enrollments
        if resp.status_code == 200:
            assert isinstance(resp.json(), list)


# ── Department Endpoint Tests ──────────────────────────────────────────────────
class TestDepartmentEndpoints:
    def test_list_departments(self, client, analyst_token):
        resp = client.get("/departments", headers=auth_headers(analyst_token))
        assert resp.status_code == 200
        depts = resp.json()
        assert len(depts) == 8
        ids = [d["department_id"] for d in depts]
        assert "CS001" in ids
        assert "BA001" in ids

    def test_get_department_detail(self, client, analyst_token):
        resp = client.get("/departments/CS001", headers=auth_headers(analyst_token))
        assert resp.status_code == 200
        data = resp.json()
        assert data["department_id"]   == "CS001"
        assert data["name"]            == "Computer Science"
        assert "student_count"         in data
        assert "course_count"          in data

    def test_department_not_found(self, client, analyst_token):
        resp = client.get("/departments/ZZZZZ", headers=auth_headers(analyst_token))
        assert resp.status_code == 404

    def test_department_courses(self, client, analyst_token):
        resp = client.get("/departments/CS001/courses", headers=auth_headers(analyst_token))
        assert resp.status_code == 200
        courses = resp.json()
        assert len(courses) > 0
        for c in courses:
            assert c["department_id"] == "CS001"


# ── Analytics Endpoint Tests ───────────────────────────────────────────────────
class TestAnalyticsEndpoints:
    def test_overview_kpis(self, client, analyst_token):
        resp = client.get(
            "/analytics/overview-kpis?academic_year=2024-2025",
            headers=auth_headers(analyst_token)
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "total_students"  in data
        assert "avg_gpa"         in data
        assert "pass_rate"       in data
        assert "at_risk_count"   in data
        assert data["total_students"] > 0
        assert 0.0 <= data["avg_gpa"] <= 4.0
        assert 0.0 <= data["pass_rate"] <= 100.0

    def test_top_students(self, client, analyst_token):
        resp = client.get(
            "/analytics/top-students?academic_year=2024-2025&semester=Fall 2024&top_n=5",
            headers=auth_headers(analyst_token)
        )
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) <= 5
        # Should be sorted descending by GPA
        gpas = [row["semester_gpa"] for row in data]
        assert gpas == sorted(gpas, reverse=True)

    def test_at_risk_students(self, client, analyst_token):
        resp = client.get(
            "/analytics/at-risk?academic_year=2024-2025&semester=Fall 2024",
            headers=auth_headers(analyst_token)
        )
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        for row in data:
            assert (row["semester_gpa"] <= 2.0) or (row["avg_attendance_pct"] <= 75.0)

    def test_department_summary(self, client, analyst_token):
        resp = client.get(
            "/analytics/department-summary?academic_year=2024-2025&semester=Fall 2024",
            headers=auth_headers(analyst_token)
        )
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) > 0
        for row in data:
            assert "department_id" in row
            assert "avg_gpa"       in row
            assert "student_count" in row

    def test_gpa_trend(self, client, analyst_token):
        list_resp = client.get("/students?page_size=1", headers=auth_headers(analyst_token))
        student_id = list_resp.json()["data"][0]["student_id"]

        resp = client.get(
            f"/analytics/gpa-trend/{student_id}",
            headers=auth_headers(analyst_token)
        )
        assert resp.status_code in (200, 404)
        if resp.status_code == 200:
            data = resp.json()
            assert isinstance(data, list)
            for row in data:
                assert "semester_gpa" in row
                assert "student_id"   in row

    def test_gpa_trend_invalid_student(self, client, analyst_token):
        resp = client.get(
            "/analytics/gpa-trend/BADSTUDENT999",
            headers=auth_headers(analyst_token)
        )
        assert resp.status_code == 404