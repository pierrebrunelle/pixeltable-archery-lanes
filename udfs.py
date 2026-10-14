"""Pixeltable UDFs for the archery-lanes scheduled pipeline (module path e.g. `udfs.score_band`)."""

from __future__ import annotations

import pixeltable as pxt


@pxt.udf
def score_band(pct: float) -> str:
    """Map hit percentage into a lane band label."""
    if pct is None:
        return 'unknown'
    p = float(pct)
    if p < 50:
        return 'novice'
    if p < 70:
        return 'developing'
    if p < 85:
        return 'club'
    return 'elite'


@pxt.udf
def archer_tag(archer: str, lane: int) -> str:
    """Compact tag like 'maya-chen@L1'."""
    name = (archer or 'unknown').strip().lower().replace(' ', '-')
    return f'{name}@L{int(lane)}'


@pxt.udf
def boto3_core_probe() -> str:
    """Prove boto3 imports as a Pixeltable core dependency (PR #1710 (boto3 as a core dependency)).

    Returns the installed boto3 version string. No AWS calls — 443-only box
    and we intentionally avoid needing AWS credentials.
    """
    import boto3

    return f'boto3/{boto3.__version__}'
