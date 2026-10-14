"""Archery Lanes scheduled pipeline: migrate legacy JSON sessions, date helpers, alter_computed_column scoring. Tables and @pxt.query live here; UDFs in udfs.py.
"""
from __future__ import annotations


import pixeltable as pxt
import pixeltable.functions as pxtf
from pixeltable.functions import date as pxtd

from udfs import archer_tag, boto3_core_probe, score_band

TableModel = pxt.model_base()


class Sessions(TableModel, name='sessions', has_default_idxs=False):
    """One archery-range scoring session migrated from legacy JSON."""

    session_id = pxt.Column(value=pxtf.uuid.uuid7(), primary_key=True)
    session_code: pxt.String
    archer: pxt.String
    lane: pxt.Int
    session_date: pxt.Date
    arrows: pxt.Int
    hits: pxt.Int
    x_rings: pxt.Int
    notes: pxt.String

    # --- fn.date secondary ---
    session_year = pxtd.year(session_date)
    session_month = pxtd.month(session_date)
    session_day = pxtd.day(session_date)
    session_weekday = pxtd.weekday(session_date)  # Mon=0 .. Sun=6
    session_iso_weekday = pxtd.isoweekday(session_date)
    session_iso = pxtd.isoformat(session_date)
    next_practice = pxtd.add_days(session_date, 7)
    calendar_label = pxtd.strftime(session_date, '%a %b %d, %Y')

    # --- scoring (v1: hit%; later altered to include X-ring bonus) ---
    hit_pct = (hits * 100.0) / arrows
    # Club formula (v2): hit% + 2 per X-ring. pipeline.py alters from v1 hit_pct -> this.
    score = (hits * 100.0) / arrows + (x_rings * 2.0)
    band = score_band(hit_pct)
    tag = archer_tag(archer, lane)
    boto3_ok = boto3_core_probe()

    __indexes__ = [
        pxt.BtreeIndex(archer),
        pxt.BtreeIndex(lane),
        pxt.BtreeIndex(session_date),
        pxt.BtreeIndex(band),
        pxt.BtreeIndex(session_code),
    ]


@pxt.query
def by_archer(archer: str):
    """Sessions for one archer, newest first."""
    return (
        Sessions.where(Sessions.archer == archer)
        .select(
            Sessions.session_code,
            Sessions.session_date,
            Sessions.lane,
            Sessions.hit_pct,
            Sessions.score,
            Sessions.band,
            Sessions.calendar_label,
            Sessions.next_practice,
        )
        .order_by(Sessions.session_date, asc=False)
    )


@pxt.query
def weekend_club(min_pct: float = 70.0):
    """Weekend sessions (Sat/Sun) at or above a hit percentage."""
    return (
        Sessions.where(
            (Sessions.session_weekday >= 5) & (Sessions.hit_pct >= min_pct)
        )
        .select(
            Sessions.session_code,
            Sessions.archer,
            Sessions.calendar_label,
            Sessions.hit_pct,
            Sessions.score,
            Sessions.band,
            Sessions.tag,
        )
        .order_by(Sessions.hit_pct, asc=False)
    )


@pxt.query
def lane_day(lane: int, day_iso: str):
    """Sessions on one lane for a given ISO date string (YYYY-MM-DD)."""
    return (
        Sessions.where((Sessions.lane == lane) & (Sessions.session_iso == day_iso))
        .select(
            Sessions.session_code,
            Sessions.archer,
            Sessions.arrows,
            Sessions.hits,
            Sessions.score,
            Sessions.boto3_ok,
        )
        .order_by(Sessions.session_code)
    )
