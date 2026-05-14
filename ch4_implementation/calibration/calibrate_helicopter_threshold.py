"""
Calibrate the Stage 3 helicopter score threshold via sweep against the
Master Flight Report (MFR), with two methodological fixes:

1. SORTIE-LEVEL matching — group MFR flights by (date, helicopter) into
   sorties; match an event to the whole sortie window rather than to one
   flight leg. A multi-leg sortie (airport → SOV → SOV → airport) is one
   ground-truth unit, so en-route turbine overflights aren't double-counted
   as separate false positives.

2. SOV-AWARE precision — an unmatched turbine event is NOT a false positive
   if the same helicopter has a Stage 4 SOV interaction in an overlapping
   window. The MFR records the SOV destination but not the en-route turbine
   visits; cross-referencing Stage 4 captures them.

Usage:
    python calibrate_helicopter_threshold.py --year 2024
"""

import argparse
import datetime as dt
from datetime import timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
from collections import defaultdict

import openpyxl
import psycopg2

DB = dict(host='localhost', port=5432, dbname='windfarm',
         user='thesis', password='thesis2026')
FLIGHT_REPORT = '/mnt/e/data_lake/helicopters/validation/Master Flight Report.xlsx'
SHEETS_2024 = ['MVY 2024', 'OQU 2024']

AIRPORT_CODES = {'KOQU', 'KMVY', 'KACK', 'KACY', 'KEWB', 'KGON', 'KHYA',
                'KLDG', 'KPVD', 'MVY', 'koqu', '6N5', 'BKL1'}

KNOWN_FLEET = {'a932e4', 'a92f2d', 'a9369b', 'a87968'}
TAIL_TO_ICAO24 = {'N693HS': 'a932e4', 'N692HS': 'a92f2d', 'N691HS': 'a9369b'}

ET = ZoneInfo('America/New_York')
UTC = ZoneInfo('UTC')

MATCH_WINDOW_MIN = 120
SOV_MATCH_WINDOW_MIN = 60   # SOV interaction overlap tolerance


def parse_time(val):
    if val is None:
        return None
    if isinstance(val, dt.time):
        return val
    if isinstance(val, dt.timedelta):
        s = int(val.total_seconds())
        h, m = divmod(s // 60, 60)
        return dt.time(h % 24, m)
    if isinstance(val, str) and ':' in val:
        parts = val.strip().split(':')
        return dt.time(int(parts[0]), int(parts[1]))
    return None


def load_mfr_flights() -> list[dict]:
    """All MFR flights with takeoff/landing times when available.

    The OQU sheet typically has destinations but no times — keep those rows
    with to_utc/ldg_utc=None and let sortie matching fall back to date-only.
    Skipping them entirely loses ~70% of offshore flights to vessels.
    """
    wb = openpyxl.load_workbook(FLIGHT_REPORT, read_only=True, data_only=True)
    flights = []
    airports_upper = {a.upper() for a in AIRPORT_CODES}
    for sheet_name in SHEETS_2024:
        ws = wb[sheet_name]
        for row in ws.iter_rows(min_row=3, values_only=True):
            reg, dep, des, _, dof, _, to_t, ldg_t, _ = row[:9]
            if not isinstance(dof, dt.datetime):
                continue
            if dep is None or des is None:
                continue
            dep = str(dep).strip()
            des = str(des).strip()
            tail = str(reg).strip().upper() if reg else None
            icao = TAIL_TO_ICAO24.get(tail)
            if not icao:
                continue
            to_local = parse_time(to_t)
            ldg_local = parse_time(ldg_t)
            to_utc = (dt.datetime.combine(dof.date(), to_local, tzinfo=ET).astimezone(UTC)
                      if to_local else None)
            ldg_utc = (dt.datetime.combine(dof.date(), ldg_local, tzinfo=ET).astimezone(UTC)
                      if ldg_local else None)
            offshore = (dep.upper() not in airports_upper) or (des.upper() not in airports_upper)
            flights.append({
                'icao24': icao, 'tail': tail, 'date': dof.date(),
                'to_utc': to_utc, 'ldg_utc': ldg_utc,
                'dep': dep, 'des': des, 'offshore': offshore,
            })
    wb.close()
    return flights


def group_into_sorties(flights: list[dict]) -> list[dict]:
    """Group MFR flights into sorties per (icao24, date).

    Sortie window: first takeoff to last landing across all legs that have
    times. If NO leg has times, mark the sortie as 'date_only' — matching
    falls back to a same-date check.
    """
    by_key = defaultdict(list)
    for f in flights:
        by_key[(f['icao24'], f['date'])].append(f)
    sorties = []
    for (icao, d), legs in by_key.items():
        any_offshore = any(l['offshore'] for l in legs)
        starts = [l['to_utc'] for l in legs if l['to_utc']]
        ends = [l['ldg_utc'] for l in legs if l['ldg_utc']]
        if starts:
            sortie_start = min(starts)
            sortie_end = max(ends) if ends else sortie_start + timedelta(hours=12)
            date_only = False
        else:
            sortie_start = None
            sortie_end = None
            date_only = True
        sorties.append({
            'icao24': icao, 'date': d,
            'sortie_start': sortie_start,
            'sortie_end': sortie_end,
            'date_only': date_only,
            'offshore': any_offshore,
            'n_legs': len(legs),
        })
    return sorties


def load_sov_interactions(year: int) -> list[dict]:
    """Helicopter-SOV interactions (hoists, flybys) from Stage 4."""
    with psycopg2.connect(**DB) as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT asset_id, interaction_start, interaction_end, interaction_type
            FROM stage4_sov_interactions
            WHERE asset_type = 'helicopter'
              AND EXTRACT(YEAR FROM interaction_start) = %s
        """, (year,))
        return [{'icao24': r[0], 'start': r[1], 'end': r[2], 'type': r[3]}
                for r in cur.fetchall()]


def load_events(year: int) -> list[dict]:
    with psycopg2.connect(**DB) as conn, conn.cursor() as cur:
        cur.execute("""SELECT icao24, visit_start, visit_end, score
                      FROM stage3_helicopter_events
                      WHERE EXTRACT(YEAR FROM visit_start) = %s
                      ORDER BY visit_start""", (year,))
        return [{'icao24': r[0], 'visit_start': r[1],
                'visit_end': r[2], 'score': r[3]} for r in cur.fetchall()]


def event_in_sortie(event: dict, sortie: dict, tol: timedelta) -> bool:
    if event['icao24'] not in KNOWN_FLEET:
        return False
    if event['icao24'] != sortie['icao24']:
        return False
    if sortie['date_only']:
        return event['visit_start'].date() == sortie['date']
    return (event['visit_start'] <= sortie['sortie_end'] + tol
            and event['visit_end'] >= sortie['sortie_start'] - tol)


def event_near_sov(event: dict, sov_interactions: list[dict],
                   tol: timedelta) -> bool:
    for s in sov_interactions:
        if s['icao24'] != event['icao24']:
            continue
        if (event['visit_start'] <= s['end'] + tol
                and event['visit_end'] >= s['start'] - tol):
            return True
    return False


def evaluate(events: list[dict], offshore_sorties: list[dict],
             sov_interactions: list[dict], thr: float) -> dict:
    """Day-level recall. A sortie is 'covered' if the pipeline detected
    ANY helicopter activity by the same icao on the same date — either a
    turbine event above threshold or a Stage 4 SOV interaction. This is
    robust to MFR records with incomplete or missing flight times.

    SOV-aware precision: event matches MFR sortie window OR a SOV
    interaction by the same helicopter."""
    tol = timedelta(minutes=MATCH_WINDOW_MIN)
    sov_tol = timedelta(minutes=SOV_MATCH_WINDOW_MIN)
    surviving = [e for e in events if e['score'] >= thr]

    # Index pipeline activity by (icao, date) for day-level coverage check
    event_dates = set()
    for e in surviving:
        event_dates.add((e['icao24'], e['visit_start'].date()))
    sov_dates = set()
    for sov in sov_interactions:
        sov_dates.add((sov['icao24'], sov['start'].date()))

    matched_event = [False] * len(surviving)
    n_sortie_by_event = 0
    n_sortie_by_sov = 0
    for s in offshore_sorties:
        key = (s['icao24'], s['date'])
        # Mark events in window for precision (still uses tight time match)
        for ei, e in enumerate(surviving):
            if event_in_sortie(e, s, tol):
                matched_event[ei] = True
        # Day-level coverage for recall
        if key in event_dates:
            n_sortie_by_event += 1
        elif key in sov_dates:
            n_sortie_by_sov += 1

    n_matched_sorties = n_sortie_by_event + n_sortie_by_sov
    n_matched_events_mfr = sum(matched_event)

    # Precision: events that matched a sortie are TP. Remaining events are
    # checked against SOV interactions directly (en-route turbine overflights
    # on sorties whose SOV destinations are not flagged as offshore).
    n_matched_events_sov = 0
    for ei, e in enumerate(surviving):
        if matched_event[ei]:
            continue
        if event_near_sov(e, sov_interactions, sov_tol):
            matched_event[ei] = True
            n_matched_events_sov += 1
    n_matched_total = sum(matched_event)
    n_kept = len(surviving)

    rec = n_matched_sorties / len(offshore_sorties) if offshore_sorties else 0.0
    prec_raw = n_matched_events_mfr / n_kept if n_kept else 0.0
    prec_sov = n_matched_total / n_kept if n_kept else 0.0
    f1_raw = 2 * prec_raw * rec / (prec_raw + rec) if (prec_raw + rec) else 0.0
    f1_sov = 2 * prec_sov * rec / (prec_sov + rec) if (prec_sov + rec) else 0.0
    return {
        'thr': thr, 'n_kept': n_kept,
        'sorties_total': len(offshore_sorties),
        'sorties_matched': n_matched_sorties,
        'sorties_by_event': n_sortie_by_event,
        'sorties_by_sov': n_sortie_by_sov,
        'events_matched_mfr': n_matched_events_mfr,
        'events_matched_sov': n_matched_events_sov,
        'events_unmatched': n_kept - n_matched_total,
        'recall': rec,
        'precision_raw': prec_raw,
        'precision_sov': prec_sov,
        'f1_raw': f1_raw,
        'f1_sov': f1_sov,
    }


def render_html(results, path, year):
    import plotly.graph_objects as go
    import plotly.io as pio
    thrs = [r['thr'] for r in results]
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=thrs, y=[r['precision_raw'] for r in results],
                            mode='lines+markers', name='Precision (MFR-only)',
                            line=dict(dash='dot')))
    fig.add_trace(go.Scatter(x=thrs, y=[r['precision_sov'] for r in results],
                            mode='lines+markers', name='Precision (SOV-corrected)'))
    fig.add_trace(go.Scatter(x=thrs, y=[r['recall'] for r in results],
                            mode='lines+markers', name='Recall (sortie-level)'))
    fig.add_trace(go.Scatter(x=thrs, y=[r['f1_sov'] for r in results],
                            mode='lines+markers', name='F1 (SOV-corrected)',
                            line=dict(width=3)))
    fig.update_layout(title=f'Threshold sweep — sortie-level + SOV-corrected ({year})',
                     xaxis_title='Score threshold', yaxis_title='Metric',
                     height=480)
    body = pio.to_html(fig, include_plotlyjs='cdn', full_html=False)
    rows_html = ''.join(
        f'<tr><td>{r["thr"]}</td><td>{r["n_kept"]}</td>'
        f'<td>{r["sorties_matched"]}/{r["sorties_total"]}</td>'
        f'<td>{r["events_matched_mfr"]}</td><td>{r["events_matched_sov"]}</td>'
        f'<td>{r["events_unmatched"]}</td>'
        f'<td>{r["precision_raw"]:.3f}</td><td>{r["precision_sov"]:.3f}</td>'
        f'<td>{r["recall"]:.3f}</td><td>{r["f1_sov"]:.3f}</td></tr>'
        for r in results)
    path.write_text(f"""<!DOCTYPE html><html><head><title>Threshold sweep — {year}</title>
<style>body{{font-family:system-ui;margin:24px}} table{{border-collapse:collapse}}
th,td{{padding:4px 10px;border-bottom:1px solid #ddd;text-align:right}}
th{{font-weight:600}}</style></head><body>
<h1>Helicopter score threshold calibration ({year})</h1>
<p>Sortie-level recall · SOV-corrected precision</p>{body}
<table><tr><th>thr</th><th>n kept</th><th>sorties matched</th>
<th>events↔MFR</th><th>events↔SOV</th><th>events unmatched</th>
<th>precision (raw)</th><th>precision (SOV)</th><th>recall</th><th>F1 (SOV)</th></tr>
{rows_html}</table></body></html>""")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--year', type=int, default=2024)
    ap.add_argument('--html', type=Path,
                   default=Path('/mnt/d/thesis/threshold_sweep.html'))
    ap.add_argument('--thr-min', type=int, default=20)
    ap.add_argument('--thr-max', type=int, default=90)
    ap.add_argument('--thr-step', type=int, default=2)
    args = ap.parse_args()

    flights = load_mfr_flights()
    sorties = group_into_sorties(flights)
    offshore_sorties = [s for s in sorties if s['offshore']]
    print(f'Loaded {len(flights)} flights → {len(sorties)} sorties '
          f'({len(offshore_sorties)} offshore)')

    sov_interactions = load_sov_interactions(args.year)
    print(f'Loaded {len(sov_interactions)} helicopter-SOV interactions')

    events = load_events(args.year)
    print(f'Loaded {len(events)} detected events')

    results = []
    print(f'\n{"thr":>4}  {"kept":>5}  {"sortie":>11}  {"by_e":>4}  {"by_s":>4}  '
          f'{"e↔MFR":>5}  {"e↔SOV":>5}  {"unmt":>5}  {"P_raw":>5}  {"P_sov":>5}  '
          f'{"recall":>6}  {"F1":>5}')
    print('-' * 92)
    for thr in range(args.thr_min, args.thr_max + 1, args.thr_step):
        r = evaluate(events, offshore_sorties, sov_interactions, thr)
        results.append(r)
        print(f'{thr:>4}  {r["n_kept"]:>5}  '
              f'{r["sorties_matched"]:>3}/{r["sorties_total"]:<3}     '
              f'{r["sorties_by_event"]:>4}  {r["sorties_by_sov"]:>4}  '
              f'{r["events_matched_mfr"]:>5}  {r["events_matched_sov"]:>5}  '
              f'{r["events_unmatched"]:>5}  '
              f'{r["precision_raw"]:>5.3f}  {r["precision_sov"]:>5.3f}  '
              f'{r["recall"]:>6.3f}  {r["f1_sov"]:>5.3f}')

    best = max(results, key=lambda r: r['f1_sov'])
    print(f'\nBest F1 (SOV-corrected): thr={best["thr"]}  '
          f'precision={best["precision_sov"]:.3f}  '
          f'recall={best["recall"]:.3f}  F1={best["f1_sov"]:.3f}')
    render_html(results, args.html, args.year)
    print(f'\nWrote {args.html}')


if __name__ == '__main__':
    main()
