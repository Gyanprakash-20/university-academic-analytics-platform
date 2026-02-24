"""
Dashboard Page: Department Analysis
"""
from __future__ import annotations

from pathlib import Path
from functools import lru_cache

import dash
import pandas as pd
from dash import html, dcc, callback, Input, Output, dash_table
import dash_bootstrap_components as dbc
import plotly.express as px

from etl.transform.calculate_gpa import full_gpa_pipeline
from dashboard.components.charts import gpa_trend_line, grade_distribution_pie

dash.register_page(__name__, path="/departments", name="Departments")

RAW_DIR = Path("data/raw")

DEPT_NAMES = {
    "CS001": "Computer Science",       "EE001": "Electrical Engineering",
    "BA001": "Business Administration","MA001": "Mathematics",
    "PH001": "Physics",                "EN001": "English Literature",
    "PS001": "Psychology",             "ME001": "Mechanical Engineering",
}

DEPT_OPTIONS = [{"label": v, "value": k} for k, v in DEPT_NAMES.items()]
SEMESTER_OPTIONS = [
    {"label": s, "value": s}
    for s in ["Fall 2024", "Spring 2024", "Fall 2023", "Spring 2023",
              "Fall 2022", "Spring 2022"]
]


@lru_cache(maxsize=1)
def _load():
    students    = pd.read_csv(RAW_DIR / "students.csv")
    enrollments = pd.read_csv(RAW_DIR / "enrollments.csv")
    courses     = pd.read_csv(RAW_DIR / "courses.csv")
    gpa         = full_gpa_pipeline(enrollments)
    return students, enrollments, courses, gpa


layout = dbc.Container([
    dbc.Row([
        dbc.Col(html.H2("🏛️ Department Analysis", className="fw-bold"), width=8),
        dbc.Col([
            dcc.Dropdown(
                id="dept-selector", options=DEPT_OPTIONS,
                value="CS001", clearable=False,
                placeholder="Select Department",
            )
        ], width=4),
    ], className="mb-4 align-items-center"),

    # KPI row
    html.Div(id="dept-kpis"),

    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("📈 GPA Trend Over Semesters"),
                dbc.CardBody(dcc.Graph(id="dept-gpa-trend")),
            ], className="shadow-sm border-0"),
        ], md=8, className="mb-4"),
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("📊 Grade Distribution"),
                dbc.CardBody(dcc.Graph(id="dept-grade-pie")),
            ], className="shadow-sm border-0"),
        ], md=4, className="mb-4"),
    ]),

    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("🏆 Top 10 Students This Semester"),
                dbc.CardBody(html.Div(id="dept-top-table")),
            ], className="shadow-sm border-0"),
        ], md=6, className="mb-4"),
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("⚠️ At-Risk Students"),
                dbc.CardBody(html.Div(id="dept-risk-table")),
            ], className="shadow-sm border-0"),
        ], md=6, className="mb-4"),
    ]),
], fluid=True)


@callback(
    Output("dept-kpis",       "children"),
    Output("dept-gpa-trend",  "figure"),
    Output("dept-grade-pie",  "figure"),
    Output("dept-top-table",  "children"),
    Output("dept-risk-table", "children"),
    Input("dept-selector",    "value"),
)
def update_department(dept_id: str):
    from dashboard.components.kpi_cards import kpi_row
    students, enrollments, courses, gpa = _load()

    dept_students = students[students["department_id"] == dept_id]["student_id"]
    dept_gpa      = gpa[gpa["student_id"].isin(dept_students)]
    dept_enr      = enrollments[enrollments["student_id"].isin(dept_students)]

    # KPIs
    latest = dept_gpa[dept_gpa["academic_year"] == "2024-2025"]
    latest_sem = latest[latest["semester"] == "Fall 2024"]

    kpis = [
        {"title": "Students",      "value": len(dept_students),
         "icon": "bi-people-fill", "color": "primary"},
        {"title": "Avg GPA (Fall '24)", "value": round(float(latest_sem["semester_gpa"].mean()), 2) if not latest_sem.empty else "N/A",
         "icon": "bi-bar-chart", "color": "success"},
        {"title": "Avg Attendance", "value": f"{round(float(latest_sem['avg_attendance_pct'].mean()), 1) if not latest_sem.empty else 0}%",
         "icon": "bi-calendar-check", "color": "info"},
        {"title": "Courses Offered", "value": len(courses[courses["department_id"] == dept_id]),
         "icon": "bi-book-fill", "color": "warning"},
    ]

    # GPA trend (average per semester)
    trend = (
        dept_gpa
        .groupby(["academic_year", "semester"])
        .agg(semester_gpa=("semester_gpa", "mean"), cumulative_gpa=("cumulative_gpa", "mean"))
        .reset_index()
        .sort_values(["academic_year", "semester"])
    )

    gpa_fig = gpa_trend_line(trend, DEPT_NAMES.get(dept_id, dept_id))
    grade_fig = grade_distribution_pie(dept_enr)

    # Top 10 students
    top10 = (
        latest_sem
        .sort_values("semester_gpa", ascending=False)
        .head(10)
        .merge(students[["student_id", "first_name", "last_name"]], on="student_id", how="left")
    )
    top_table = dash_table.DataTable(
        data=top10[["student_id", "first_name", "last_name", "semester_gpa", "avg_attendance_pct"]].to_dict("records"),
        columns=[
            {"name": "ID",          "id": "student_id"},
            {"name": "First Name",  "id": "first_name"},
            {"name": "Last Name",   "id": "last_name"},
            {"name": "GPA",         "id": "semester_gpa"},
            {"name": "Attendance%", "id": "avg_attendance_pct"},
        ],
        style_cell={"fontSize": "13px", "padding": "6px"},
        style_header={"fontWeight": "bold", "backgroundColor": "#f8f9fa"},
        style_data_conditional=[
            {"if": {"filter_query": "{semester_gpa} >= 3.5"},
             "backgroundColor": "#d4edda", "color": "black"},
        ],
        page_size=10,
    )

    # At-risk students
    risk = (
        latest_sem[
            (latest_sem["semester_gpa"] < 2.5) |
            (latest_sem["avg_attendance_pct"] < 75)
        ]
        .sort_values("semester_gpa")
        .head(10)
        .merge(students[["student_id", "first_name", "last_name"]], on="student_id", how="left")
    )
    risk_table = dash_table.DataTable(
        data=risk[["student_id", "first_name", "last_name", "semester_gpa", "avg_attendance_pct"]].to_dict("records"),
        columns=[
            {"name": "ID",          "id": "student_id"},
            {"name": "First Name",  "id": "first_name"},
            {"name": "GPA",         "id": "semester_gpa"},
            {"name": "Attendance%", "id": "avg_attendance_pct"},
        ],
        style_cell={"fontSize": "13px", "padding": "6px"},
        style_header={"fontWeight": "bold", "backgroundColor": "#f8f9fa"},
        style_data_conditional=[
            {"if": {"filter_query": "{semester_gpa} < 2.0"},
             "backgroundColor": "#f8d7da", "color": "black"},
            {"if": {"filter_query": "{semester_gpa} >= 2.0 && {semester_gpa} < 2.5"},
             "backgroundColor": "#fff3cd", "color": "black"},
        ],
        page_size=10,
    )

    return kpi_row(kpis), gpa_fig, grade_fig, top_table, risk_table