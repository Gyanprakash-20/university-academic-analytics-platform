"""
Dashboard Page: Student Details
"""
from __future__ import annotations

from pathlib import Path
from functools import lru_cache

import dash
import pandas as pd
from dash import html, dcc, callback, Input, Output, State, dash_table
import dash_bootstrap_components as dbc
import plotly.express as px

from etl.transform.calculate_gpa import full_gpa_pipeline
from dashboard.components.charts import gpa_trend_line

dash.register_page(__name__, path="/students", name="Students")

RAW_DIR = Path("data/raw")

DEPT_NAMES = {
    "CS001": "Computer Science",       "EE001": "Electrical Engineering",
    "BA001": "Business Administration","MA001": "Mathematics",
    "PH001": "Physics",                "EN001": "English Literature",
    "PS001": "Psychology",             "ME001": "Mechanical Engineering",
}


@lru_cache(maxsize=1)
def _load():
    students    = pd.read_csv(RAW_DIR / "students.csv")
    enrollments = pd.read_csv(RAW_DIR / "enrollments.csv")
    gpa         = full_gpa_pipeline(enrollments)
    return students, enrollments, gpa


layout = dbc.Container([
    dbc.Row([
        dbc.Col(html.H2("🎓 Student Details", className="fw-bold"), width=6),
        dbc.Col([
            dbc.InputGroup([
                dbc.Input(id="student-search", placeholder="Search by Student ID (e.g. STU000042)", type="text"),
                dbc.Button("Search", id="student-search-btn", color="primary"),
            ])
        ], width=6),
    ], className="mb-4 align-items-center"),

    html.Div(id="student-profile"),
], fluid=True)


@callback(
    Output("student-profile", "children"),
    Input("student-search-btn", "n_clicks"),
    State("student-search", "value"),
    prevent_initial_call=True,
)
def load_student(n_clicks, student_id: str):
    if not student_id:
        return dbc.Alert("Please enter a Student ID", color="warning")

    students, enrollments, gpa = _load()
    student_id = student_id.strip().upper()

    student = students[students["student_id"] == student_id]
    if student.empty:
        return dbc.Alert(f"Student '{student_id}' not found", color="danger")

    s          = student.iloc[0]
    dept_name  = DEPT_NAMES.get(s["department_id"], s["department_id"])
    student_gpa = gpa[gpa["student_id"] == student_id].sort_values(["academic_year", "semester"])
    student_enr = enrollments[enrollments["student_id"] == student_id]

    # Current semester summary
    latest = student_gpa.iloc[-1] if not student_gpa.empty else None

    # Profile card
    profile_card = dbc.Card([
        dbc.CardBody([
            dbc.Row([
                dbc.Col([
                    html.Div([
                        html.I(className="bi bi-person-circle", style={"fontSize": "4rem", "color": "#4c85de"}),
                    ], className="text-center mb-2"),
                    html.H4(f"{s['first_name']} {s['last_name']}", className="text-center fw-bold"),
                    html.P(s["student_id"], className="text-center text-muted small"),
                ], md=3),
                dbc.Col([
                    dbc.Row([
                        dbc.Col([
                            html.P("Program",    className="text-muted small mb-1"),
                            html.P(s["program"],  className="fw-semibold"),
                        ], md=6),
                        dbc.Col([
                            html.P("Department", className="text-muted small mb-1"),
                            html.P(dept_name,    className="fw-semibold"),
                        ], md=6),
                        dbc.Col([
                            html.P("Type",       className="text-muted small mb-1"),
                            html.P(s["student_type"], className="fw-semibold"),
                        ], md=6),
                        dbc.Col([
                            html.P("Enrolled",   className="text-muted small mb-1"),
                            html.P(str(s["enrollment_date"]), className="fw-semibold"),
                        ], md=6),
                    ]),
                ], md=5),
                dbc.Col([
                    dbc.Row([
                        dbc.Col([
                            html.Div([
                                html.H3(
                                    f"{latest['semester_gpa']:.2f}" if latest is not None else "N/A",
                                    className="fw-bold text-success mb-0"
                                ),
                                html.Small("Current GPA", className="text-muted"),
                            ], className="text-center p-3 bg-light rounded"),
                        ], md=6),
                        dbc.Col([
                            html.Div([
                                html.H3(
                                    f"{latest['avg_attendance_pct']:.0f}%" if latest is not None else "N/A",
                                    className="fw-bold text-info mb-0"
                                ),
                                html.Small("Attendance", className="text-muted"),
                            ], className="text-center p-3 bg-light rounded"),
                        ], md=6),
                    ]),
                ], md=4),
            ]),
        ])
    ], className="shadow-sm border-0 mb-4")

    # GPA trend chart
    if not student_gpa.empty:
        trend_fig = gpa_trend_line(student_gpa, f"{s['first_name']} {s['last_name']}")
        trend_section = dbc.Card([
            dbc.CardBody(dcc.Graph(figure=trend_fig))
        ], className="shadow-sm border-0 mb-4")
    else:
        trend_section = None

    # Enrollment history table
    enr_table = dash_table.DataTable(
        data=student_enr[["semester", "academic_year", "course_id", "grade",
                           "attendance_pct", "midterm_score", "final_score"]].to_dict("records"),
        columns=[
            {"name": "Semester",   "id": "semester"},
            {"name": "Year",       "id": "academic_year"},
            {"name": "Course",     "id": "course_id"},
            {"name": "Grade",      "id": "grade"},
            {"name": "Attendance%","id": "attendance_pct"},
            {"name": "Midterm",    "id": "midterm_score"},
            {"name": "Final",      "id": "final_score"},
        ],
        style_cell={"fontSize": "13px", "padding": "6px"},
        style_header={"fontWeight": "bold", "backgroundColor": "#f8f9fa"},
        style_data_conditional=[
            {"if": {"filter_query": "{grade} = 'A' || {grade} = 'A+'"},
             "backgroundColor": "#d4edda"},
            {"if": {"filter_query": "{grade} = 'F' || {grade} = 'W'"},
             "backgroundColor": "#f8d7da"},
        ],
        sort_action="native",
        page_size=15,
    )   

    enr_section = dbc.Card([
        dbc.CardHeader("📚 Enrollment & Grade History"),
        dbc.CardBody(enr_table),
    ], className="shadow-sm border-0")

    return [profile_card, trend_section, enr_section]