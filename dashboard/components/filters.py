"""
Dashboard: Reusable Filter Components
=======================================
Shared dropdown, date-range, and multi-select filter widgets
used across all dashboard pages.
"""
from __future__ import annotations

from dash import dcc, html
import dash_bootstrap_components as dbc

# ── Data constants ─────────────────────────────────────────────────────────────
DEPARTMENT_OPTIONS = [
    {"label": "All Departments",           "value": "ALL"},
    {"label": "Computer Science",          "value": "CS001"},
    {"label": "Electrical Engineering",    "value": "EE001"},
    {"label": "Business Administration",   "value": "BA001"},
    {"label": "Mathematics",               "value": "MA001"},
    {"label": "Physics",                   "value": "PH001"},
    {"label": "English Literature",        "value": "EN001"},
    {"label": "Psychology",                "value": "PS001"},
    {"label": "Mechanical Engineering",    "value": "ME001"},
]

SEMESTER_OPTIONS = [
    {"label": "Fall 2024",   "value": "Fall 2024"},
    {"label": "Spring 2024", "value": "Spring 2024"},
    {"label": "Fall 2023",   "value": "Fall 2023"},
    {"label": "Spring 2023", "value": "Spring 2023"},
    {"label": "Fall 2022",   "value": "Fall 2022"},
    {"label": "Spring 2022", "value": "Spring 2022"},
]

ACADEMIC_YEAR_OPTIONS = [
    {"label": "2024–2025", "value": "2024-2025"},
    {"label": "2023–2024", "value": "2023-2024"},
    {"label": "2022–2023", "value": "2022-2023"},
]

STUDENT_TYPE_OPTIONS = [
    {"label": "All Types",   "value": "ALL"},
    {"label": "Full-Time",   "value": "Full-Time"},
    {"label": "Part-Time",   "value": "Part-Time"},
]

GPA_TIER_OPTIONS = [
    {"label": "All Students",  "value": "ALL"},
    {"label": "Honors (≥3.5)", "value": "honors"},
    {"label": "Good (3.0–3.5)","value": "good"},
    {"label": "Average (2.5–3.0)","value": "average"},
    {"label": "At-Risk (<2.5)","value": "at_risk"},
]


# ── Filter component builders ──────────────────────────────────────────────────
def department_filter(
    component_id: str = "dept-filter",
    multi: bool = False,
    value: str = "ALL",
    label: str = "Department",
) -> html.Div:
    return html.Div([
        html.Label(label, className="form-label small fw-semibold text-muted text-uppercase"),
        dcc.Dropdown(
            id=component_id,
            options=DEPARTMENT_OPTIONS,
            value=value if not multi else [value],
            multi=multi,
            clearable=not multi,
            placeholder="Select department…",
            className="shadow-sm",
        ),
    ])


def semester_filter(
    component_id: str = "semester-filter",
    value: str = "Fall 2024",
    label: str = "Semester",
) -> html.Div:
    return html.Div([
        html.Label(label, className="form-label small fw-semibold text-muted text-uppercase"),
        dcc.Dropdown(
            id=component_id,
            options=SEMESTER_OPTIONS,
            value=value,
            clearable=False,
            className="shadow-sm",
        ),
    ])


def academic_year_filter(
    component_id: str = "year-filter",
    value: str = "2024-2025",
    label: str = "Academic Year",
) -> html.Div:
    return html.Div([
        html.Label(label, className="form-label small fw-semibold text-muted text-uppercase"),
        dcc.Dropdown(
            id=component_id,
            options=ACADEMIC_YEAR_OPTIONS,
            value=value,
            clearable=False,
            className="shadow-sm",
        ),
    ])


def gpa_range_slider(
    component_id: str = "gpa-slider",
    label: str = "GPA Range",
) -> html.Div:
    return html.Div([
        html.Label(label, className="form-label small fw-semibold text-muted text-uppercase"),
        dcc.RangeSlider(
            id=component_id,
            min=0.0, max=4.0, step=0.1,
            value=[0.0, 4.0],
            marks={0: "0.0", 1: "1.0", 2: "2.0", 3: "3.0", 4: "4.0"},
            tooltip={"placement": "bottom", "always_visible": True},
            className="mt-2",
        ),
    ])


def attendance_range_slider(
    component_id: str = "att-slider",
    label: str = "Attendance % Range",
) -> html.Div:
    return html.Div([
        html.Label(label, className="form-label small fw-semibold text-muted text-uppercase"),
        dcc.RangeSlider(
            id=component_id,
            min=0, max=100, step=5,
            value=[0, 100],
            marks={0: "0%", 25: "25%", 50: "50%", 75: "75%", 100: "100%"},
            tooltip={"placement": "bottom", "always_visible": True},
            className="mt-2",
        ),
    ])


def student_type_filter(
    component_id: str = "student-type-filter",
    value: str = "ALL",
) -> html.Div:
    return html.Div([
        html.Label("Student Type", className="form-label small fw-semibold text-muted text-uppercase"),
        dbc.RadioItems(
            id=component_id,
            options=STUDENT_TYPE_OPTIONS,
            value=value,
            inline=True,
            className="mt-1",
        ),
    ])


def filter_panel(filters: list[html.Div], title: str = "Filters") -> dbc.Card:
    """
    Wrap a list of filter components in a collapsible card panel.
    """
    return dbc.Card([
        dbc.CardHeader([
            html.I(className="bi bi-funnel-fill me-2"),
            title,
        ], className="fw-semibold"),
        dbc.CardBody([
            dbc.Row([
                dbc.Col(f, md=True, className="mb-3")
                for f in filters
            ])
        ]),
    ], className="shadow-sm border-0 mb-4")


def top_n_selector(
    component_id: str = "top-n",
    default: int = 10,
    label: str = "Show Top N",
) -> html.Div:
    return html.Div([
        html.Label(label, className="form-label small fw-semibold text-muted text-uppercase"),
        dbc.Select(
            id=component_id,
            options=[
                {"label": "Top 5",   "value": 5},
                {"label": "Top 10",  "value": 10},
                {"label": "Top 20",  "value": 20},
                {"label": "Top 50",  "value": 50},
                {"label": "Top 100", "value": 100},
            ],
            value=default,
        ),
    ])