"""Dashboard: Reusable Plotly chart builders"""
from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

PALETTE = px.colors.qualitative.Set2
THEME   = "plotly_white"


def gpa_distribution_histogram(df: pd.DataFrame, department_id: str | None = None) -> go.Figure:
    """GPA distribution histogram, optionally filtered by department."""
    title = f"GPA Distribution — {department_id}" if department_id else "GPA Distribution (All Departments)"
    fig = px.histogram(
        df, x="semester_gpa", nbins=40,
        title=title,
        labels={"semester_gpa": "Semester GPA"},
        color_discrete_sequence=[PALETTE[0]],
        template=THEME,
    )
    fig.add_vline(x=2.0, line_dash="dash", line_color="red",
                  annotation_text="Academic Probation (2.0)")
    fig.add_vline(x=3.5, line_dash="dash", line_color="green",
                  annotation_text="Dean's List (3.5)")
    fig.update_layout(bargap=0.05, showlegend=False)
    return fig


def department_avg_gpa_bar(dept_agg: pd.DataFrame) -> go.Figure:
    """Horizontal bar chart: average GPA per department."""
    sorted_df = dept_agg.sort_values("avg_gpa", ascending=True)
    fig = px.bar(
        sorted_df, x="avg_gpa", y="department_id",
        orientation="h",
        title="Average GPA by Department",
        labels={"avg_gpa": "Average GPA", "department_id": "Department"},
        color="avg_gpa",
        color_continuous_scale="RdYlGn",
        range_color=[2.0, 4.0],
        template=THEME,
        text="avg_gpa",
    )
    fig.update_traces(texttemplate="%{text:.2f}", textposition="outside")
    fig.update_layout(coloraxis_showscale=False, yaxis_title="")
    return fig


def gpa_trend_line(trend_df: pd.DataFrame, student_name: str = "") -> go.Figure:
    """Line chart: semester GPA vs cumulative GPA trend for a student."""
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=trend_df["semester"], y=trend_df["semester_gpa"],
        mode="lines+markers", name="Semester GPA",
        line=dict(color=PALETTE[0], width=2),
        marker=dict(size=8),
    ))
    fig.add_trace(go.Scatter(
        x=trend_df["semester"], y=trend_df["cumulative_gpa"],
        mode="lines+markers", name="Cumulative GPA",
        line=dict(color=PALETTE[1], width=2, dash="dot"),
        marker=dict(size=6),
    ))
    fig.update_layout(
        title=f"GPA Trend — {student_name}" if student_name else "GPA Trend",
        xaxis_title="Semester",
        yaxis_title="GPA",
        yaxis_range=[0, 4.2],
        template=THEME,
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    fig.add_hrect(y0=0, y1=2.0, fillcolor="red",   opacity=0.05, line_width=0)
    fig.add_hrect(y0=3.5, y1=4.2, fillcolor="green", opacity=0.05, line_width=0)
    return fig


def attendance_heatmap(att_df: pd.DataFrame) -> go.Figure:
    """Heatmap: attendance rate by department × semester."""
    pivot = att_df.pivot_table(
        index="department_id", columns="semester",
        values="avg_attendance_pct", aggfunc="mean"
    ).round(1)

    fig = px.imshow(
        pivot,
        title="Attendance Rate (%) — Dept × Semester",
        labels=dict(x="Semester", y="Department", color="Attendance %"),
        color_continuous_scale="RdYlGn",
        zmin=60, zmax=100,
        text_auto=True,
        template=THEME,
    )
    fig.update_layout(coloraxis_showscale=True)
    return fig


def risk_scatter(gpa_df: pd.DataFrame, students_df: pd.DataFrame) -> go.Figure:
    """Scatter: GPA vs attendance, color-coded by risk level."""
    merged = gpa_df.merge(
        students_df[["student_id", "department_id", "program"]],
        on="student_id", how="left"
    )

    def risk_level(row):
        if row["semester_gpa"] < 2.0 or row["avg_attendance_pct"] < 60:
            return "Critical"
        elif row["semester_gpa"] < 2.5 or row["avg_attendance_pct"] < 75:
            return "At Risk"
        elif row["semester_gpa"] >= 3.5:
            return "Honors"
        return "Normal"

    merged["risk"] = merged.apply(risk_level, axis=1)

    color_map = {"Critical": "red", "At Risk": "orange", "Normal": "steelblue", "Honors": "green"}

    fig = px.scatter(
        merged, x="avg_attendance_pct", y="semester_gpa",
        color="risk", color_discrete_map=color_map,
        hover_data=["student_id", "department_id", "program"],
        title="Student Risk Profile: GPA vs Attendance",
        labels={"avg_attendance_pct": "Attendance (%)", "semester_gpa": "Semester GPA"},
        opacity=0.6,
        template=THEME,
    )
    fig.add_hline(y=2.0, line_dash="dash", line_color="red",   annotation_text="GPA 2.0")
    fig.add_vline(x=75,  line_dash="dash", line_color="orange", annotation_text="75% Attendance")
    return fig


def grade_distribution_pie(enrollments_df: pd.DataFrame, department_id: str | None = None) -> go.Figure:
    """Pie chart: distribution of letter grades."""
    df = enrollments_df.copy()
    grade_counts = df["grade"].value_counts().reset_index()
    grade_counts.columns = ["grade", "count"]

    # Sort by grade hierarchy
    ORDER = ["A+","A","A-","B+","B","B-","C+","C","C-","D","F","W","I"]
    grade_counts["order"] = grade_counts["grade"].map(
        {g: i for i, g in enumerate(ORDER)}
    ).fillna(99)
    grade_counts = grade_counts.sort_values("order")

    fig = px.pie(
        grade_counts, names="grade", values="count",
        title="Grade Distribution",
        color_discrete_sequence=px.colors.qualitative.Set3,
        hole=0.4,
        template=THEME,
    )
    fig.update_traces(textposition="inside", textinfo="percent+label")
    return fig