"""Build a property's dashboard payload straight from the Search Console API.

The fallback for properties BigQuery does not have yet. A backfill across
fourteen properties takes hours, and a property missing from the warehouse
would otherwise be missing from the dashboard entirely -- this fills it in
live at build time instead, so the page is complete from the first run.

It is a fallback, not the main path. The API serves a rolling ~16 months and
nothing older, it is rate limited, and it cannot answer "what did this page do
last March" once March falls out of the window. BigQuery is the record;
this is the stopgap while the record is being written.

Roughly 17 requests per property: one daily series, two per window for pages,
two per window for queries, one page x query drilldown, two breakdowns.

Same grain discipline as everywhere else: anything with `query` in its
dimensions loses anonymised queries, so page figures and query figures come
from separate requests and are never derived from one another.
"""

from __future__ import annotations

import datetime as dt
import time

from googleapiclient.errors import HttpError

ROW_LIMIT = 25_000


def _run(service, site: str, start: dt.date, end: dt.date,
         dimensions: list[str], cap: int = 25_000) -> list[dict]:
    """One Search Analytics call, paged with startRow until short or capped."""
    rows: list[dict] = []
    start_row = 0
    while len(rows) < cap:
        body = {
            "startDate": start.isoformat(),
            "endDate": end.isoformat(),
            "dimensions": dimensions,
            "type": "web",
            "dataState": "final",
            "rowLimit": min(ROW_LIMIT, cap - len(rows)),
            "startRow": start_row,
        }
        for attempt in range(5):
            try:
                resp = service.searchanalytics().query(siteUrl=site, body=body).execute()
                break
            except HttpError as exc:
                if exc.resp.status in (429, 500, 503) and attempt < 4:
                    time.sleep(2**attempt)
                    continue
                raise
        batch = resp.get("rows", [])
        for r in batch:
            rec = dict(zip(dimensions, r.get("keys", [])))
            rec["clicks"] = int(r.get("clicks", 0))
            rec["impressions"] = int(r.get("impressions", 0))
            rec["position"] = round(float(r.get("position", 0.0)), 2)
            rows.append(rec)
        if len(batch) < body["rowLimit"]:
            break
        start_row += len(batch)
    return rows


def _entity_window(service, site: str, dim: str, latest: dt.date,
                   n: int, top: int) -> list[dict]:
    """Top `top` values of `dim` for the last n days, each with its prior n days.

    Ranked on the union of both periods rather than on the current one, so an
    entity that has collapsed to zero clicks still appears -- those are exactly
    the rows worth looking at.
    """
    cur_start = latest - dt.timedelta(days=n - 1)
    prev_end = cur_start - dt.timedelta(days=1)
    prev_start = prev_end - dt.timedelta(days=n - 1)

    cur = {r[dim]: r for r in _run(service, site, cur_start, latest, [dim])}
    prev = {r[dim]: r for r in _run(service, site, prev_start, prev_end, [dim])}

    out = []
    for k in set(cur) | set(prev):
        c, p = cur.get(k), prev.get(k)
        out.append({
            "k": k,
            "c": c["clicks"] if c else 0,
            "i": c["impressions"] if c else 0,
            "c0": p["clicks"] if p else 0,
            "i0": p["impressions"] if p else 0,
            "p": round(c["position"], 1) if c else None,
            "p0": round(p["position"], 1) if p else None,
        })
    out.sort(key=lambda r: (max(r["c"], r["c0"]), r["i"]), reverse=True)
    return out[:top]


def build_property(service, site: str, latest: dt.date, windows: list[int],
                   pcfg: dict) -> dict:
    """Everything the dashboard needs for one property, from the API alone."""
    trend_start = latest - dt.timedelta(days=min(pcfg["trend_days"], 500) - 1)
    daily_rows = _run(service, site, trend_start, latest, ["date"], cap=1000)
    daily_rows.sort(key=lambda r: r["date"])
    daily = [[r["date"], r["clicks"], r["impressions"], r["position"]]
             for r in daily_rows]

    by_date = {r["date"]: r for r in daily_rows}
    win_out = {}
    for n in windows:
        cur_start = latest - dt.timedelta(days=n - 1)
        prev_end = cur_start - dt.timedelta(days=1)
        prev_start = prev_end - dt.timedelta(days=n - 1)

        def agg(a: dt.date, b: dt.date) -> tuple[int, int, float | None]:
            c = i = 0
            wpos = 0.0
            d = a
            while d <= b:
                r = by_date.get(d.isoformat())
                if r:
                    c += r["clicks"]
                    i += r["impressions"]
                    # Search Console averages position by impressions; summing
                    # the daily averages unweighted would let a 3-impression
                    # day move the month.
                    wpos += r["position"] * r["impressions"]
                d += dt.timedelta(days=1)
            return c, i, (round(wpos / i, 2) if i else None)

        c, i, pos = agg(cur_start, latest)
        c0, i0, pos0 = agg(prev_start, prev_end)
        win_out[str(n)] = {
            "clicks": c, "clicks_prev": c0,
            "impressions": i, "impressions_prev": i0,
            "position": pos, "position_prev": pos0,
            "pages": None,      # filled from the 28d page pull below
        }

    pages, queries = {}, {}
    for n in windows:
        pages[str(n)] = _entity_window(service, site, "page", latest, n,
                                       pcfg["top_pages"])
        queries[str(n)] = _entity_window(service, site, "query", latest, n,
                                         pcfg["top_queries"])
        win_out[str(n)]["pages"] = sum(1 for r in pages[str(n)] if r["c"] > 0)

    # page x query, 28 days, for the Pages tab drilldown.
    drill: dict[str, list] = {}
    top_pages = {r["k"] for r in pages[str(28)][: pcfg["drilldown_pages"]]} \
        if "28" in pages else set()
    if top_pages:
        start28 = latest - dt.timedelta(days=27)
        for r in _run(service, site, start28, latest, ["page", "query"], cap=50_000):
            if r["page"] in top_pages:
                drill.setdefault(r["page"], []).append(
                    [r["query"], r["clicks"], r["impressions"], round(r["position"], 1)]
                )
        for k in drill:
            drill[k].sort(key=lambda x: (-x[1], -x[2]))
            del drill[k][pcfg["drilldown_queries_per_page"]:]

    start28 = latest - dt.timedelta(days=27)
    breakdown = {}
    for dim in ("country", "device"):
        rows = _run(service, site, start28, latest, [dim], cap=200)
        rows.sort(key=lambda r: -r["clicks"])
        breakdown[dim] = [{"k": r[dim], "c": r["clicks"], "i": r["impressions"]}
                          for r in rows[:25]]

    present = sorted(by_date)
    return {
        "windows": win_out,
        "daily": daily,
        "pages": pages,
        "queries": queries,
        "page_queries": drill,
        "country": breakdown["country"],
        "device": breakdown["device"],
        "health": {
            "latest_date": present[-1] if present else latest.isoformat(),
            "earliest_date": present[0] if present else latest.isoformat(),
            "days_present": len(present),
            "last_loaded_at": None,
        },
    }
