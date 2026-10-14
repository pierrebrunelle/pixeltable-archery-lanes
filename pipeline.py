#!/usr/bin/env python3
"""Archery Lanes scheduled-pipeline driver (no HTTP service).

Workflow: data migration + incremental ticks
1. pxt schema update app.py archery
2. Prove boto3 imports as a core Pixeltable dependency (#1710)
3. import_json staging tables from legacy range-software JSON batches
4. Idempotent insert into modeled sessions (skip session_codes already present)
5. Exercise pixeltable.functions.date columns + @pxt.query
6. alter_computed_column: upgrade score from hit% to hit% + 2*X-rings
7. Persist watermark in state.json so a second tick only loads batch 2

Usage:
    python pipeline.py              # run next pending batch(es)
    python pipeline.py --reset      # clear watermark (does not drop catalog)
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import date, datetime
from pathlib import Path

import boto3
import pixeltable as pxt
from pixeltable.functions import date as pxtd
from pixeltable.io import import_json

import udfs  # noqa: F401 — register UDF module path

ROOT = Path(__file__).resolve().parent
LOG = ROOT / 'logs' / 'run.jsonl'
STATE = ROOT / 'state.json'
VENV_PXT = Path(__file__).resolve().parents[2] / '.venv' / 'bin' / 'pxt'
CATALOG = 'archery'
BATCHES = [
    ROOT / 'data' / 'legacy_sessions_batch1.json',
    ROOT / 'data' / 'legacy_sessions_batch2.json',
]


def log(event: str, **payload) -> None:
    LOG.parent.mkdir(parents=True, exist_ok=True)
    with LOG.open('a') as f:
        f.write(json.dumps({'event': event, 'ts': datetime.now().isoformat(timespec='seconds'), **payload}, default=str) + '\n')
    print(event, json.dumps(payload, default=str)[:400], flush=True)


def load_state() -> dict:
    if STATE.exists():
        return json.loads(STATE.read_text())
    return {'next_batch': 0, 'altered_score': False, 'runs': 0}


def save_state(st: dict) -> None:
    STATE.write_text(json.dumps(st, indent=2, default=str))


def run_schema() -> None:
    cmd = [str(VENV_PXT), 'schema', 'update', 'app.py', CATALOG, '-f']
    log('schema_update_start', catalog=CATALOG)
    t0 = time.time()
    r = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True)
    dt = round(time.time() - t0, 2)
    log(
        'schema_update_done',
        exit=r.returncode,
        seconds=dt,
        stdout=(r.stdout or '')[-600:],
        stderr=(r.stderr or '')[-600:],
    )
    if r.returncode != 0:
        raise SystemExit(f'schema update failed: {r.returncode}')


def prove_boto3_core() -> str:
    ver = boto3.__version__
    # Session construction must not need credentials for this probe.
    session = boto3.session.Session()
    region = session.region_name or 'unset'
    log('boto3_core_ok', version=ver, region=region, note='PR #1710 core dependency')
    return ver


def existing_codes(sessions) -> set[str]:
    rows = sessions.select(sessions.session_code).collect()
    return {r['session_code'] for r in rows}


def ingest_batch(batch_path: Path, batch_idx: int) -> int:
    staging = f'{CATALOG}/legacy_batch_{batch_idx}'
    try:
        pxt.drop_table(staging, force=True)
    except Exception:
        pass
    log('import_json_start', path=str(batch_path.name), staging=staging)
    t0 = time.time()
    src = import_json(
        staging,
        str(batch_path),
        schema_overrides={
            'session_code': pxt.String,
            'archer': pxt.String,
            'lane': pxt.Int,
            'session_date': pxt.String,  # ISO string in JSON; parse below
            'arrows': pxt.Int,
            'hits': pxt.Int,
            'x_rings': pxt.Int,
            'notes': pxt.String,
        },
        primary_key='session_code',
    )
    log('import_json_done', seconds=round(time.time() - t0, 2), rows=src.count())

    sessions = pxt.get_table(f'{CATALOG}/sessions')
    have = existing_codes(sessions)
    raw = json.loads(batch_path.read_text())
    to_insert = []
    for r in raw:
        if r['session_code'] in have:
            continue
        to_insert.append(
            {
                'session_code': r['session_code'],
                'archer': r['archer'],
                'lane': int(r['lane']),
                'session_date': date.fromisoformat(r['session_date']),
                'arrows': int(r['arrows']),
                'hits': int(r['hits']),
                'x_rings': int(r['x_rings']),
                'notes': r.get('notes') or '',
            }
        )
    if not to_insert:
        log('insert_skip', reason='all session_codes already present', batch=batch_idx)
        return 0
    t0 = time.time()
    status = sessions.insert(to_insert)
    log(
        'insert_done',
        batch=batch_idx,
        rows=len(to_insert),
        seconds=round(time.time() - t0, 2),
        status=str(status),
    )
    return len(to_insert)


def alter_score_formula() -> None:
    sessions = pxt.get_table(f'{CATALOG}/sessions')
    # Club formula: hit percentage + 2 points per X-ring (still / arrows scale).
    new_score = (sessions.hits * 100.0) / sessions.arrows + (sessions.x_rings * 2.0)
    log('alter_computed_start', column='score')
    t0 = time.time()
    status = sessions.alter_computed_column(score=new_score, recompute=True, cascade=True)
    log('alter_computed_done', seconds=round(time.time() - t0, 2), status=str(status))


def exercise_queries() -> None:
    # Mirror @pxt.query helpers via table filters (query templates are for schema/serving)
    sessions = pxt.get_table(f'{CATALOG}/sessions')
    maya = list(
        sessions.where(sessions.archer == 'Maya Chen')
        .select(
            sessions.session_code,
            sessions.session_date,
            sessions.lane,
            sessions.hit_pct,
            sessions.score,
            sessions.band,
            sessions.calendar_label,
            sessions.next_practice,
        )
        .order_by(sessions.session_date, asc=False)
        .collect()
    )
    wknd = list(
        sessions.where((sessions.session_weekday >= 5) & (sessions.hit_pct >= 70.0))
        .select(
            sessions.session_code,
            sessions.archer,
            sessions.calendar_label,
            sessions.hit_pct,
            sessions.score,
            sessions.band,
            sessions.tag,
        )
        .order_by(sessions.hit_pct, asc=False)
        .collect()
    )
    lane2 = list(
        sessions.where((sessions.lane == 2) & (sessions.session_iso == '2026-10-04'))
        .select(
            sessions.session_code,
            sessions.archer,
            sessions.arrows,
            sessions.hits,
            sessions.score,
            sessions.boto3_ok,
        )
        .order_by(sessions.session_code)
        .collect()
    )
    sample = list(
        sessions.select(
            sessions.session_code,
            sessions.calendar_label,
            sessions.session_weekday,
            sessions.next_practice,
            sessions.hit_pct,
            sessions.score,
            sessions.band,
            sessions.boto3_ok,
        )
        .order_by(sessions.session_date)
        .limit(5)
        .collect()
    )
    log(
        'queries_ok',
        maya_n=len(maya),
        weekend_club_n=len(wknd),
        lane2_n=len(lane2),
        maya=maya[:3],
        weekend=wknd[:3],
        lane2=lane2,
        sample=sample,
        total=sessions.count(),
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--reset', action='store_true', help='reset watermark only')
    ap.add_argument('--all-batches', action='store_true', help='drain every pending batch this tick')
    args = ap.parse_args()

    if args.reset:
        save_state({'next_batch': 0, 'altered_score': False, 'runs': 0})
        log('watermark_reset')
        return 0

    prove_boto3_core()
    run_schema()

    st = load_state()
    inserted_total = 0
    while st['next_batch'] < len(BATCHES):
        idx = st['next_batch']
        inserted_total += ingest_batch(BATCHES[idx], idx)
        st['next_batch'] = idx + 1
        st['runs'] = st.get('runs', 0) + 1
        save_state(st)
        if not args.all_batches:
            break

    if not st.get('altered_score') and st['next_batch'] > 0:
        alter_score_formula()
        st['altered_score'] = True
        save_state(st)

    exercise_queries()
    log('pipeline_complete', next_batch=st['next_batch'], inserted_total=inserted_total, altered=st.get('altered_score'))
    return 0


if __name__ == '__main__':
    sys.exit(main())
