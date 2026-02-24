"""
Dashboard Page: Overview
=========================
Platform-wide KPIs + distribution charts.
"""
from __future__ import annotations

from pathlib import Path
from functools import lru_cache

import dash
import pandas as pd
from dash import html, dcc, callback, Input, Output
import dash_bootstrap_components as dbc

from dashboard.components.kpi_cards import kpi_row
from dashboard.components.charts import (
    gpa_distribution_histogram,
    department_avg_gpa_bar,
    risk_scatter,
    grade_distribution_pie,
)
from etl.transform.calculate_gpa import full_gpa_pipeline

dash.register_page(__name__, path="/", name="Overview", title="Overview | University Analytics")

RAW_DIR = Path("data/raw")

SEMESTER_OPTIONS = [
    {"label": "Fall 2024",   "value": "Fall 2024"},
    {"label": "Spring 2024", "value": "Spring 2024"},
    {"label": "Fall 2023",   "value": "Fall 2023"},
    {"label": "Spring 2023", "value": "Spring 2023"},
]
YEAR_OPTIONS = [
    {"label": "2024-2025", "value": "2024-2025"},
    {"label": "2023-2024", "value": "2023-2024"},
    {"label": "2022-2023", "value": "2022-2023"},
]


@lru_cache(maxsize=1)
def _load():
    students    = pd.read_csv(RAW_DIR / "students.csv")
    enrollments = pd.read_csv(RAW_DIR / "enrollments.csv")
    gpa         = full_gpa_pipeline(enrollments)
    return students, enrollments, gpa


layout = dbc.Container([
    # ── Header ────────────────────────────────────────────────────────────────
    dbc.Row([
        dbc.Col([
            html.H2("📊 Platform Overview", className="fw-bold mb-1"),
            html.P("University-wide academic performance metrics", className="text-muted"),
        ], width=8),
        dbc.Col([
            dbc.Row([
                dbc.Col(dcc.Dropdown(
                    id="overview-year", options=YEAR_OPTIONS,
                    value="2024-2025", clearable=False,
                )),
                dbc.Col(dcc.Dropdown(
                    id="overview-semester", options=SEMESTER_OPTIONS,
                    value="Fall 2024", clearable=False,
                )),
            ]),
        ], width=4),
    ], className="mb-4 align-items-center"),

    # ── KPI Row ───────────────────────────────────────────────────────────────
    html.Div(id="overview-kpi-row"),

    # ── Charts ────────────────────────────────────────────────────────────────
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody(dcc.Graph(id="overview-gpa-hist"))
            ], className="shadow-sm border-0"),
        ], md=6, className="mb-4"),
        dbc.Col([
            dbc.Card([
                dbc.CardBody(dcc.Graph(id="overview-dept-bar"))
            ], className="shadow-sm border-0"),
        ], md=6, className="mb-4"),
    ]),

    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody(dcc.Graph(id="overview-risk-scatter"))
            ], className="shadow-sm border-0"),
        ], md=8, className="mb-4"),
        dbc.Col([
            dbc.Card([
                dbc.CardBody(dcc.Graph(id="overview-grade-pie"))
            ], className="shadow-sm border-0"),
        ], md=4, className="mb-4"),
    ]),
], fluid=True)


@callback(
    Output("overview-kpi-row", "children"),
    Output("overview-gpa-hist", "figure"),
    Output("overview-dept-bar", "figure"),
    Output("overview-risk-scatter", "figure"),
    Output("overview-grade-pie", "figure"),
    Input("overview-year", "value"),
    Input("overview-semester", "value"),
)
def update_overview(academic_year: str, semester: str):
    students, enrollments, gpa = _load()

    # Filter by semester
    sem_gpa = gpa[
        (gpa["academic_year"] == academic_year) &
        (gpa["semester"]      == semester)
    ]
    sem_enr = enrollments[
        (enrollments["academic_year"] == academic_year) &
        (enrollments["semester"]      == semester)
    ]

    # KPIs
    total_students  = int(gpa["student_id"].nunique())
    avg_gpa         = round(float(sem_gpa["semester_gpa"].mean()),  2) if not sem_gpa.empty else 0
    avg_attendance  = round(float(sem_gpa["avg_attendance_pct"].mean()), 1) if not sem_gpa.empty else 0
    at_risk         = int((sem_gpa["semester_gpa"] < 2.0).sum())
    honors          = int((sem_gpa["semester_gpa"] >= 3.5).sum())
    pass_rate       = round(
        sem_gpa["courses_passed"].sum() / max(sem_gpa["courses_taken"].sum(), 1) * 100, 1
    ) if not sem_gpa.empty else 0

    kpis = [
        {"title": "Total Students",    "value": f"{total_students:,}", "icon": "bi-people-fill",     "color": "primary"},
        {"title": "Average GPA",       "value": avg_gpa,               "icon": "bi-bar-chart-fill",  "color": "success"},
        {"title": "Avg. Attendance",   "value": f"{avg_attendance}%",  "icon": "bi-calendar-check",  "color": "info"},
        {"title": "At-Risk Students",  "value": f"{at_risk:,}",        "icon": "bi-exclamation-triangle-fill", "color": "danger"},
        {"title": "Honors Students",   "value": f"{honors:,}",         "icon": "bi-award-fill",       "color": "warning"},
        {"title": "Pass Rate",         "value": f"{pass_rate}%",       "icon": "bi-check-circle-fill","color": "success"},
    ]

    # Department aggregation for bar chart
    if not sem_gpa.empty:
        dept_agg = (
            sem_gpa
            .merge(students[["student_id", "department_id"]], on="student_id", how="left")
            .groupby("department_id")
            .agg(avg_gpa=("semester_gpa", "mean"))
            .reset_index()
        )
        dept_agg["avg_gpa"] = dept_agg["avg_gpa"].round(2)
    else:
        dept_agg = pd.DataFrame(columns=["department_id", "avg_gpa"])

    return (
        kpi_row(kpis),
        gpa_distribution_histogram(sem_gpa if not sem_gpa.empty else gpa),
        department_avg_gpa_bar(dept_agg),
        risk_scatter(sem_gpa if not sem_gpa.empty else gpa, students),
        grade_distribution_pie(sem_enr if not sem_enr.empty else enrollments),
    )