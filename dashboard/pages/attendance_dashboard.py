"""
Dashboard Page: Attendance Analytics
"""
from __future__ import annotations

from pathlib import Path
from functools import lru_cache

import dash
import pandas as pd
from dash import html, dcc, callback, Input, Output
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go

dash.register_page(__name__, path="/attendance", name="Attendance")

RAW_DIR = Path("data/raw")

DEPT_OPTIONS = [
    {"label": "All Departments", "value": "ALL"},
    {"label": "Computer Science",       "value": "CS001"},
    {"label": "Business Administration","value": "BA001"},
    {"label": "Electrical Engineering", "value": "EE001"},
    {"label": "Mathematics",            "value": "MA001"},
]


@lru_cache(maxsize=1)
def _load():
    students   = pd.read_csv(RAW_DIR / "students.csv")
    attendance = pd.read_csv(RAW_DIR / "attendance.csv")
    return students, attendance


layout = dbc.Container([
    dbc.Row([
        dbc.Col(html.H2("📅 Attendance Analytics", className="fw-bold"), width=8),
        dbc.Col([
            dcc.Dropdown(
                id="att-dept", options=DEPT_OPTIONS,
                value="ALL", clearable=False,
            )
        ], width=4),
    ], className="mb-4 align-items-center"),

    # KPI row
    html.Div(id="att-kpis"),

    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("📊 Attendance Status Breakdown"),
                dbc.CardBody(dcc.Graph(id="att-status-pie")),
            ], className="shadow-sm border-0"),
        ], md=4, className="mb-4"),
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("📈 Daily Attendance Trend"),
                dbc.CardBody(dcc.Graph(id="att-daily-line")),
            ], className="shadow-sm border-0"),
        ], md=8, className="mb-4"),
    ]),

    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("🏫 Attendance by Weekday"),
                dbc.CardBody(dcc.Graph(id="att-weekday-bar")),
            ], className="shadow-sm border-0"),
        ], md=6, className="mb-4"),
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("⚠️ High Absence Rate Courses"),
                dbc.CardBody(dcc.Graph(id="att-course-bar")),
            ], className="shadow-sm border-0"),
        ], md=6, className="mb-4"),
    ]),
], fluid=True)


@callback(
    Output("att-kpis",       "children"),
    Output("att-status-pie", "figure"),
    Output("att-daily-line", "figure"),
    Output("att-weekday-bar","figure"),
    Output("att-course-bar", "figure"),
    Input("att-dept", "value"),
)
def update_attendance(dept_id: str):
    from dashboard.components.kpi_cards import kpi_row
    students, attendance = _load()

    att = attendance.copy()

    if dept_id != "ALL":
        dept_students = students[students["department_id"] == dept_id]["student_id"]
        att = att[att["student_id"].isin(dept_students)]

    total    = len(att)
    present  = int((att["status"].isin(["PRESENT", "LATE"])).sum())
    absent   = int((att["status"] == "ABSENT").sum())
    excused  = int((att["status"] == "EXCUSED").sum())
    att_rate = round(present / max(total, 1) * 100, 1)

    kpis = [
        {"title": "Total Records",  "value": f"{total:,}",   "icon": "bi-calendar3",        "color": "primary"},
        {"title": "Present",        "value": f"{present:,}", "icon": "bi-check-circle-fill", "color": "success"},
        {"title": "Absent",         "value": f"{absent:,}",  "icon": "bi-x-circle-fill",     "color": "danger"},
        {"title": "Attendance Rate","value": f"{att_rate}%", "icon": "bi-percent",           "color": "info"},
    ]

    # Status pie
    status_counts = att["status"].value_counts().reset_index()
    status_counts.columns = ["status", "count"]
    status_fig = px.pie(
        status_counts, names="status", values="count",
        title="Status Distribution",
        color_discrete_sequence=px.colors.qualitative.Set2,
        hole=0.4, template="plotly_white",
    )

    # Daily trend (sample last 60 days)
    att["attendance_date"] = pd.to_datetime(att["attendance_date"])
    daily = (
        att
        .groupby(["attendance_date", "status"])
        .size().reset_index(name="count")
        .sort_values("attendance_date")
    )
    daily_fig = px.area(
        daily, x="attendance_date", y="count", color="status",
        title="Daily Attendance Events",
        labels={"attendance_date": "Date", "count": "Count"},
        color_discrete_sequence=px.colors.qualitative.Set1,
        template="plotly_white",
    )

    # Weekday bar
    att["weekday"] = att["attendance_date"].dt.day_name()
    day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
    weekday = (
        att[att["weekday"].isin(day_order)]
        .groupby("weekday")
        .apply(lambda x: (x["status"].isin(["PRESENT", "LATE"])).mean() * 100)
        .reset_index()
    )
    weekday.columns = ["weekday", "attendance_rate"]
    weekday["weekday"] = pd.Categorical(weekday["weekday"], categories=day_order, ordered=True)
    weekday = weekday.sort_values("weekday")
    weekday_fig = px.bar(
        weekday, x="weekday", y="attendance_rate",
        title="Attendance Rate by Weekday",
        labels={"weekday": "Day", "attendance_rate": "Attendance %"},
        color="attendance_rate", color_continuous_scale="RdYlGn",
        template="plotly_white", text_auto=".1f",
    )
    weekday_fig.update_layout(coloraxis_showscale=False)

    # High absence courses
    course_att = (
        att[att["status"] == "ABSENT"]
        .groupby("course_id")
        .size().reset_index(name="absence_count")
        .sort_values("absence_count", ascending=False)
        .head(10)
    )
    course_fig = px.bar(
        course_att, x="absence_count", y="course_id",
        orientation="h",
        title="Top 10 Courses by Absence Count",
        labels={"absence_count": "Absences", "course_id": "Course"},
        color="absence_count", color_continuous_scale="Reds",
        template="plotly_white",
    )
    course_fig.update_layout(coloraxis_showscale=False, yaxis_title="")

    return kpi_row(kpis), status_fig, daily_fig, weekday_fig, course_fig