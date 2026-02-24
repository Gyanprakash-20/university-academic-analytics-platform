"""Dashboard: KPI Card Components"""
from __future__ import annotations

import dash_bootstrap_components as dbc
from dash import html


def kpi_card(
    title: str,
    value: str | int | float,
    icon:  str,
    color: str = "primary",
    subtitle: str = "",
    delta: str | None = None,
    delta_positive: bool = True,
) -> dbc.Card:
    """
    Create a KPI metric card.

    Parameters
    ----------
    title    : Metric name
    value    : Main display value
    icon     : Bootstrap icon class (e.g. 'bi-people-fill')
    color    : Bootstrap color (primary, success, warning, danger, info)
    subtitle : Small text below value
    delta    : Change indicator string (e.g. '+2.3%')
    """
    delta_color = "text-success" if delta_positive else "text-danger"
    delta_icon  = "bi-arrow-up-short" if delta_positive else "bi-arrow-down-short"

    return dbc.Card([
        dbc.CardBody([
            dbc.Row([
                dbc.Col([
                    html.P(title, className="text-muted small mb-1 fw-semibold text-uppercase"),
                    html.H3(str(value), className=f"fw-bold text-{color} mb-0"),
                    html.Small(subtitle, className="text-muted") if subtitle else None,
                    html.Div([
                        html.I(className=f"bi {delta_icon} {delta_color}"),
                        html.Small(delta, className=f"{delta_color} fw-semibold ms-1"),
                    ], className="mt-1") if delta else None,
                ], width=9),
                dbc.Col([
                    html.Div([
                        html.I(
                            className=f"bi {icon}",
                            style={"fontSize": "2.5rem", "opacity": "0.7"}
                        )
                    ], className=f"text-{color} text-end"),
                ], width=3),
            ]),
        ])
    ], className="shadow-sm h-100 border-0")


def kpi_row(kpis: list[dict]) -> dbc.Row:
    """
    Render a row of KPI cards from a list of dicts.
    Each dict: {title, value, icon, color?, subtitle?, delta?, delta_positive?}
    """
    cols = [
        dbc.Col(
            kpi_card(**kpi),
            xs=12, sm=6, lg=3,
            className="mb-4"
        )
        for kpi in kpis
    ]
    return dbc.Row(cols)