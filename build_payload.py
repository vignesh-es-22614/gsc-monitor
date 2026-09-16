"""Turn the BigQuery Search Console tables into the dashboard's static payload.

The dashboard is a static page on GitHub Pages -- it cannot query BigQuery, so
everything it can show has to be precomputed here.

Shape of the output, under docs/data/:

    summary.json        property list, headline metrics, load health, alerts
    prop_<slug>.json    one per property, fetched lazily when it is selected

Why precomputed windows rather than raw daily rows
--------------------------------------------------
Shipping per-page daily rows and letting the browser aggregate would mean
~90k rows per property before the user clicks anything. Instead each window
(7 / 28 / 90 days) is aggregated server-side against its own preceding window,
so the page loads a few hundred KB and does no arithmetic beyond sorting.

One query per (grain, window) covers all fourteen properties at once, using
QUALIFY to keep the top N within each -- fourteen separate queries per window
would scan the same partitions fourteen times.

    python build_payload.py --out docs/data
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import sys

from google.cloud import bigquery

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from gsc_export import default_token_path, load_credentials  # noqa: E402

PROJECT = "it-security-online-marketing"
DATASET = "gsc_data"

# Deliberately the VIEWS, not the raw tables. More than one loader appends to
# gsc_page_daily / gsc_query_daily concurrently, so the raw tables carry
# duplicate keys; the views keep the latest copy of each. Reading the tables
# directly double-counts -- pitstop was 1.96x at one point. See sql/01_views.sql.
PAGE_TABLE = f"`{PROJECT}.{DATASET}.v_page_daily`"
QUERY_TABLE = f"`{PROJECT}.{DATASET}.v_query_daily`"

WINDOWS = [7, 28, 90]

# The www root is a URL-prefix property containing the product properties, so
# its rows duplicate theirs. Flagged in the payload so the UI can say so rather
# than letting someone add the cards up.
ROOT_PROPERTY = "https://www.manageengine.com/"


def slug(site_url: str) -> str:
    s = re.sub(r"^https?://", "", site_url).strip("/")
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


def label(site_url: str) -> str:
    s = re.sub(r"^https?://(www\.)?", "", site_url).strip("/")
    if s == "manageengine.com":
        return "manageengine.com (all)"
    return s.replace("manageengine.com/products/", "").replace(
        "manageengine.com/", ""
    )


def load_config(path: str) -> dict:
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


# --------------------------------------------------------------------------- #
# Queries
# --------------------------------------------------------------------------- #
def q_latest_and_health(bq) -> tuple[dt.date, list[dict]]:
    sql = f"""
    SELECT site_url,
           MAX(date) AS latest_date,
           MIN(date) AS earliest_date,
           MAX(loaded_at) AS last_loaded_at,
           COUNT(DISTINCT date) AS days_present,
           DATE_DIFF(MAX(date), MIN(date), DAY) + 1 AS days_spanned
    FROM {PAGE_TABLE}
    WHERE site_url IS NOT NULL
    GROUP BY site_url
    ORDER BY site_url
    """
    rows = [dict(r) for r in bq.query(sql).result()]
    latest = max(r["latest_date"] for r in rows)
    return latest, rows


def q_property_windows(bq, latest: dt.date) -> dict:
    """Per property, per window: current vs preceding period, at page grain."""
    parts = []
    for n in WINDOWS:
        cur_start = latest - dt.timedelta(days=n - 1)
        prev_end = cur_start - dt.timedelta(days=1)
        prev_start = prev_end - dt.timedelta(days=n - 1)
        parts.append(
            f"""
            SELECT {n} AS win, site_url,
                   SUM(IF(date >= '{cur_start}', clicks, 0)) AS clicks,
                   SUM(IF(date >= '{cur_start}', impressions, 0)) AS impressions,
                   SUM(IF(date <  '{cur_start}', clicks, 0)) AS clicks_prev,
                   SUM(IF(date <  '{cur_start}', impressions, 0)) AS impressions_prev,
                   SAFE_DIVIDE(
                     SUM(IF(date >= '{cur_start}', position * impressions, 0)),
                     SUM(IF(date >= '{cur_start}', impressions, 0))) AS position,
                   SAFE_DIVIDE(
                     SUM(IF(date <  '{cur_start}', position * impressions, 0)),
                     SUM(IF(date <  '{cur_start}', impressions, 0))) AS position_prev,
                   COUNT(DISTINCT IF(date >= '{cur_start}', page, NULL)) AS pages
            FROM {PAGE_TABLE}
            WHERE site_url IS NOT NULL
              AND date BETWEEN '{prev_start}' AND '{latest}'
            GROUP BY site_url
            """
        )
    sql = "\nUNION ALL\n".join(parts)
    out: dict[str, dict] = {}
    for r in bq.query(sql).result():
        out.setdefault(r["site_url"], {})[r["win"]] = {
            "clicks": r["clicks"] or 0,
            "clicks_prev": r["clicks_prev"] or 0,
            "impressions": r["impressions"] or 0,
            "impressions_prev": r["impressions_prev"] or 0,
            "position": round(r["position"], 2) if r["position"] else None,
            "position_prev": round(r["position_prev"], 2)
            if r["position_prev"]
            else None,
            "pages": r["pages"] or 0,
        }
    return out


def q_daily(bq, trend_days: int, latest: dt.date) -> dict:
    start = latest - dt.timedelta(days=trend_days - 1)
    sql = f"""
    SELECT site_url, date,
           SUM(clicks) AS clicks,
           SUM(impressions) AS impressions,
           SAFE_DIVIDE(SUM(position * impressions), SUM(impressions)) AS position
    FROM {PAGE_TABLE}
    WHERE site_url IS NOT NULL AND date >= '{start}'
    GROUP BY site_url, date
    ORDER BY site_url, date
    """
    out: dict[str, list] = {}
    for r in bq.query(sql).result():
        out.setdefault(r["site_url"], []).append(
            [
                r["date"].isoformat(),
                r["clicks"] or 0,
                r["impressions"] or 0,
                round(r["position"], 2) if r["position"] else None,
            ]
        )
    return out


def _entity_sql(table: str, dim: str, n: int, latest: dt.date) -> str:
    """Every value of `dim` for one property and one window, with prior period.

    No LIMIT and no QUALIFY. An earlier version kept only the top 500 per
    property, which made the dashboard's own tables a sample rather than the
    data -- the thing this exists to avoid. The result is written to its own
    file and fetched only when that tab and window are actually opened.

    Ordered by clicks so the first page rendered is the interesting one and the
    browser never has to sort 384,000 rows before showing anything.
    """
    cur_start = latest - dt.timedelta(days=n - 1)
    prev_end = cur_start - dt.timedelta(days=1)
    prev_start = prev_end - dt.timedelta(days=n - 1)
    return f"""
    SELECT {dim} AS k,
           SUM(IF(date >= '{cur_start}', clicks, 0)) AS c,
           SUM(IF(date >= '{cur_start}', impressions, 0)) AS i,
           SUM(IF(date <  '{cur_start}', clicks, 0)) AS c0,
           SUM(IF(date <  '{cur_start}', impressions, 0)) AS i0,
           SAFE_DIVIDE(
             SUM(IF(date >= '{cur_start}', position * impressions, 0)),
             SUM(IF(date >= '{cur_start}', impressions, 0))) AS p,
           SAFE_DIVIDE(
             SUM(IF(date <  '{cur_start}', position * impressions, 0)),
             SUM(IF(date <  '{cur_start}', impressions, 0))) AS p0
    FROM {table}
    WHERE site_url = @site
      AND {dim} IS NOT NULL
      AND date BETWEEN '{prev_start}' AND '{latest}'
    GROUP BY k
    ORDER BY c DESC, i DESC
    """


def write_entity_files(bq, out_dir: str, sites: list[dict], latest: dt.date) -> dict:
    """One file per (property, grain, window), holding every row.

    Rows are compact arrays rather than objects -- [k,c,i,c0,i0,p,p0] instead
    of seven repeated key names per row. On 2.08M rows that naming overhead
    alone would be about 85 MB.
    """
    os.makedirs(os.path.join(out_dir, "f"), exist_ok=True)
    manifest: dict[str, dict] = {}

    for p in sites:
        site = p["site_url"]
        for grain, table, dim in (
            ("pages", PAGE_TABLE, "page"),
            ("queries", QUERY_TABLE, "query"),
        ):
            for n in WINDOWS:
                job = bq.query(
                    _entity_sql(table, dim, n, latest),
                    job_config=bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ScalarQueryParameter("site", "STRING", site)
                        ]
                    ),
                )
                rows = [
                    [
                        r["k"],
                        r["c"] or 0,
                        r["i"] or 0,
                        r["c0"] or 0,
                        r["i0"] or 0,
                        round(r["p"], 1) if r["p"] else None,
                        round(r["p0"], 1) if r["p0"] else None,
                    ]
                    for r in job.result()
                ]
                name = f"f/{p['slug']}-{grain}-{n}.json"
                path = os.path.join(out_dir, name)
                with open(path, "w", encoding="utf-8") as fh:
                    json.dump(
                        {
                            "site_url": site,
                            "grain": grain,
                            "win": n,
                            "cols": ["k", "c", "i", "c0", "i0", "p", "p0"],
                            "n": len(rows),
                            "rows": rows,
                        },
                        fh,
                        separators=(",", ":"),
                    )
                kb = os.path.getsize(path) // 1024
                manifest.setdefault(p["slug"], {})[f"{grain}-{n}"] = {
                    "n": len(rows),
                    "kb": kb,
                }
                print(
                    f"    {name:<52} {len(rows):>8,} rows  {kb:>6,} KB", flush=True
                )
    return manifest


def q_page_query_drilldown(
    bq, latest: dt.date, n_pages: int, n_queries: int, win: int = 28
) -> dict:
    """Top queries for each of the top pages -- the page-wise/query-wise join.

    Two levels of QUALIFY: pick the top pages per property, then the top
    queries within each of those pages.
    """
    start = latest - dt.timedelta(days=win - 1)
    sql = f"""
    WITH pq AS (
      SELECT site_url, page, query,
             SUM(clicks) AS c, SUM(impressions) AS i,
             SAFE_DIVIDE(SUM(position * impressions), SUM(impressions)) AS p
      FROM {QUERY_TABLE}
      WHERE site_url IS NOT NULL AND date >= '{start}'
      GROUP BY site_url, page, query
    ),
    top_pages AS (
      SELECT site_url, page, SUM(c) AS pc
      FROM pq GROUP BY site_url, page
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY site_url ORDER BY SUM(c) DESC, SUM(i) DESC) <= {n_pages}
    )
    SELECT pq.site_url, pq.page, pq.query, pq.c, pq.i, pq.p
    FROM pq JOIN top_pages USING (site_url, page)
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY pq.site_url, pq.page
      ORDER BY pq.c DESC, pq.i DESC) <= {n_queries}
    """
    out: dict[str, dict[str, list]] = {}
    for r in bq.query(sql).result():
        out.setdefault(r["site_url"], {}).setdefault(r["page"], []).append(
            [r["query"], r["c"] or 0, r["i"] or 0, round(r["p"], 1) if r["p"] else None]
        )
    return out


def q_query_coverage(bq) -> dict:
    """How far the query-grain backfill has reached, per property.

    The Queries tab is empty for a property until the load reaches the recent
    window. Without this the UI cannot tell "still loading" from "broken", and
    those want very different reactions from whoever is looking.
    """
    sql = f"""
    SELECT site_url, MIN(date) mn, MAX(date) mx, COUNT(DISTINCT date) days
    FROM {QUERY_TABLE}
    WHERE source = 'query_grain'
    GROUP BY site_url
    """
    return {
        r["site_url"]: {
            "from": r["mn"].isoformat(),
            "to": r["mx"].isoformat(),
            "days": r["days"],
        }
        for r in bq.query(sql).result()
    }


def q_query_page_drilldown(
    bq, latest: dt.date, n_queries: int, n_pages: int, win: int = 28
) -> dict:
    """Which pages rank for each top query -- the inverse of the page drilldown.

    A query rarely maps to one URL: several pages on the same site can rank for
    it, and knowing which one Google actually surfaces is the point of asking.
    """
    start = latest - dt.timedelta(days=win - 1)
    sql = f"""
    WITH qp AS (
      SELECT site_url, query, page,
             SUM(clicks) AS c, SUM(impressions) AS i,
             SAFE_DIVIDE(SUM(position * impressions), SUM(impressions)) AS p
      FROM {QUERY_TABLE}
      WHERE site_url IS NOT NULL AND date >= '{start}'
      GROUP BY site_url, query, page
    ),
    top_q AS (
      SELECT site_url, query
      FROM qp GROUP BY site_url, query
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY site_url ORDER BY SUM(c) DESC, SUM(i) DESC) <= {n_queries}
    )
    SELECT qp.site_url, qp.query, qp.page, qp.c, qp.i, qp.p
    FROM qp JOIN top_q USING (site_url, query)
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY qp.site_url, qp.query
      ORDER BY qp.c DESC, qp.i DESC) <= {n_pages}
    """
    out: dict[str, dict[str, list]] = {}
    for r in bq.query(sql).result():
        out.setdefault(r["site_url"], {}).setdefault(r["query"], []).append(
            [r["page"], r["c"] or 0, r["i"] or 0, round(r["p"], 1) if r["p"] else None]
        )
    return out


def q_breakdowns(bq, latest: dt.date, win: int = 28) -> dict:
    """Country and device splits. Query-grain table, so shares not totals."""
    start = latest - dt.timedelta(days=win - 1)
    out: dict[str, dict] = {}
    for dim in ("country", "device"):
        sql = f"""
        SELECT site_url, {dim} AS k, SUM(clicks) c, SUM(impressions) i
        FROM {QUERY_TABLE}
        WHERE site_url IS NOT NULL AND {dim} IS NOT NULL AND date >= '{start}'
        GROUP BY site_url, k
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY site_url ORDER BY SUM(clicks) DESC) <= 25
        """
        for r in bq.query(sql).result():
            out.setdefault(r["site_url"], {}).setdefault(dim, []).append(
                {"k": r["k"], "c": r["c"] or 0, "i": r["i"] or 0}
            )
    return out


def q_adap_legacy(bq) -> list:
    """ADAP monthly history back to 2022, on the old understated basis.

    Kept separate from `daily` and never spliced onto it. The two halves are
    measured differently and the ratio between them trends, so a single joined
    line would show a decline that is really the anonymised share growing.
    """
    sql = f"""
    SELECT DATE_TRUNC(`Date`, MONTH) AS m,
           SUM(Clicks) AS clicks, SUM(Impressions) AS impressions
    FROM `{PROJECT}.{DATASET}.gsc_api_export`
    WHERE `Date` < DATE '2025-04-29'
    GROUP BY m ORDER BY m
    """
    return [
        [r["m"].isoformat(), int(r["clicks"] or 0), int(r["impressions"] or 0)]
        for r in bq.query(sql).result()
    ]


# --------------------------------------------------------------------------- #
# Assembly
# --------------------------------------------------------------------------- #
def _existing_manifest(out_dir: str) -> dict:
    """Rebuild the entity-file manifest from what is already on disk."""
    d = os.path.join(out_dir, "f")
    out: dict[str, dict] = {}
    if not os.path.isdir(d):
        return out
    for fn in os.listdir(d):
        if not fn.endswith(".json"):
            continue
        stem = fn[:-5]
        slug_part, grain, win = stem.rsplit("-", 2)
        path = os.path.join(d, fn)
        try:
            with open(path, encoding="utf-8") as fh:
                n = json.load(fh).get("n", 0)
        except Exception:  # noqa: BLE001
            continue
        out.setdefault(slug_part, {})[f"{grain}-{win}"] = {
            "n": n,
            "kb": os.path.getsize(path) // 1024,
        }
    return out


def build(cfg: dict, out_dir: str, token: str, api_fallback: bool = False,
          skip_entity_files: bool = False) -> dict:
    creds = load_credentials(token)
    bq = bigquery.Client(project=PROJECT, credentials=creds)

    pcfg = cfg["payload"]
    excluded = set(cfg.get("properties", {}).get("exclude", []))

    print("  load health ...", flush=True)
    latest, health = q_latest_and_health(bq)
    print(f"  latest date = {latest}", flush=True)

    print("  property windows ...", flush=True)
    windows = q_property_windows(bq, latest)

    print("  daily trend ...", flush=True)
    daily = q_daily(bq, pcfg["trend_days"], latest)

    drill, qdrill, breakdowns, qcov = {}, {}, {}, {}
    if table_exists(bq, f"{PROJECT}.{DATASET}.gsc_query_daily"):
        qcov = q_query_coverage(bq)
        print("  page -> query drilldown ...", flush=True)
        drill = q_page_query_drilldown(
            bq, latest, pcfg["drilldown_pages"], pcfg["drilldown_queries_per_page"]
        )
        print("  query -> page drilldown ...", flush=True)
        qdrill = q_query_page_drilldown(
            bq, latest, pcfg["drilldown_queries"], pcfg["drilldown_pages_per_query"]
        )
        print("  country / device ...", flush=True)
        breakdowns = q_breakdowns(bq, latest)
    else:
        print("  gsc_query_daily not present yet -- skipping query grain", flush=True)

    print("  ADAP legacy history ...", flush=True)
    legacy = q_adap_legacy(bq)

    # --- properties BigQuery does not have yet ------------------------------
    # The backfill takes hours across fourteen properties. Rather than showing
    # a dashboard that is silently missing five of them, pull those straight
    # from the Search Console API and mark them as such.
    api_props: dict[str, dict] = {}
    if api_fallback:
        from googleapiclient.discovery import build as gbuild

        from api_source import build_property
        from gsc_export import all_sites

        svc = gbuild("searchconsole", "v1", credentials=creds)
        loaded = {h["site_url"] for h in health}
        missing = [s for s in all_sites(svc) if s not in loaded and s not in excluded]
        for site in missing:
            print(f"  API fallback: {site} ...", flush=True)
            try:
                api_props[site] = build_property(svc, site, latest, WINDOWS, pcfg)
            except Exception as exc:  # noqa: BLE001
                # One property failing must not lose the other thirteen.
                print(f"    failed: {exc}", flush=True)

    os.makedirs(out_dir, exist_ok=True)

    props = []
    for h in health:
        site = h["site_url"]
        if site in excluded:
            continue
        w = windows.get(site, {})
        props.append(
            {
                "site_url": site,
                "slug": slug(site),
                "label": label(site),
                "is_root": site == ROOT_PROPERTY,
                "latest_date": h["latest_date"].isoformat(),
                "earliest_date": h["earliest_date"].isoformat(),
                "days_behind": (latest - h["latest_date"]).days,
                "last_loaded_at": h["last_loaded_at"].isoformat()
                if h["last_loaded_at"]
                else None,
                "days_present": h["days_present"],
                "days_spanned": h["days_spanned"],
                "missing_days": h["days_spanned"] - h["days_present"],
                "source": "bigquery",
                "windows": {str(k): v for k, v in w.items()},
            }
        )

    for site, blob in api_props.items():
        hh = blob["health"]
        props.append(
            {
                "site_url": site,
                "slug": slug(site),
                "label": label(site),
                "is_root": site == ROOT_PROPERTY,
                "latest_date": hh["latest_date"],
                "earliest_date": hh["earliest_date"],
                "days_behind": (latest - dt.date.fromisoformat(hh["latest_date"])).days,
                "last_loaded_at": None,
                "days_present": hh["days_present"],
                "days_spanned": hh["days_present"],
                "missing_days": 0,
                "source": "search_console_api",
                "windows": blob["windows"],
            }
        )

    props.sort(key=lambda p: -(p["windows"].get("28", {}).get("clicks") or 0))

    for p in props:
        site = p["site_url"]
        api = api_props.get(site)
        payload = {
            "site_url": site,
            "label": p["label"],
            "latest_date": p["latest_date"],
            "source": p["source"],
            "daily": api["daily"] if api else daily.get(site, []),
            # Entity tables are NOT inlined here -- they live in their own
            # per-window files and are fetched when that tab is opened, so the
            # first paint does not wait on 384,000 rows of query data.
            "pages": api["pages"] if api else None,
            "queries": api["queries"] if api else None,
            "query_coverage": None if api else qcov.get(site),
            "page_queries": api["page_queries"] if api else drill.get(site, {}),
            "query_pages": {} if api else qdrill.get(site, {}),
            "country": api["country"] if api
                       else breakdowns.get(site, {}).get("country", []),
            "device": api["device"] if api
                      else breakdowns.get(site, {}).get("device", []),
        }
        if site == "https://www.manageengine.com/products/active-directory-audit/":
            payload["legacy_monthly"] = legacy
        path = os.path.join(out_dir, f"prop_{p['slug']}.json")
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, separators=(",", ":"))
        kb = os.path.getsize(path) // 1024
        print(f"    {p['slug']:<44} {kb:>6,} KB", flush=True)

    # The entity files are the expensive half of the build -- 84 queries and
    # ~126 MB. Skipping them lets a change to the small payloads be rebuilt in
    # a minute instead of twenty, reusing the manifest already on disk.
    if skip_entity_files:
        print("\n  entity tables: skipped, reusing the existing manifest", flush=True)
        manifest = _existing_manifest(out_dir)
    else:
        print("\n  full entity tables (every row, one file per window) ...", flush=True)
        bq_props = [p for p in props if p["source"] == "bigquery"]
        manifest = write_entity_files(bq, out_dir, bq_props, latest)

    summary = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "latest_date": latest.isoformat(),
        "windows": WINDOWS,
        "root_property": ROOT_PROPERTY,
        "properties": props,
        "has_query_grain": bool(qcov),
        "api_fallback_used": sorted(api_props),
        # slug -> {"pages-28": {"n": rows, "kb": size}, ...}. The UI reads this
        # to show a row count and a size warning before fetching a big file.
        "entity_files": manifest,
        "notes": {
            "overlap": (
                "manageengine.com is a URL-prefix property containing the "
                "product properties below it. Property figures must not be "
                "added together."
            ),
            "query_grain": (
                "Query-level clicks and impressions exclude anonymised "
                "queries, which Search Console omits entirely from any "
                "query-dimensioned request. They rank reliably but under-count "
                "badly -- one probed day showed 66% of clicks missing. Page "
                "figures come from a separate page-grain pull and are correct."
            ),
            "history": (
                "Page-grain history starts 2025-04-29; the Search Console API "
                "serves only a rolling ~16 months and nothing earlier can ever "
                "be recovered. ADAP has 2022-2025 history from an older export "
                "that under-counts, shown separately and never spliced on."
            ),
        },
    }
    return summary


def table_exists(bq, table_id: str) -> bool:
    try:
        bq.get_table(table_id)
        return True
    except Exception:
        return False


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--out", default=os.path.join(HERE, "docs", "data"))
    p.add_argument("--config", default=os.path.join(HERE, "alerts.config.json"))
    p.add_argument("--token", default=None)
    p.add_argument(
        "--api-fallback",
        action="store_true",
        help=(
            "For properties BigQuery has no rows for, pull the payload "
            "straight from the Search Console API so the dashboard is complete "
            "while the backfill is still running."
        ),
    )
    p.add_argument(
        "--skip-entity-files",
        action="store_true",
        help="Reuse the existing per-window entity files instead of rebuilding "
             "all 84 of them. For iterating on the small payloads.",
    )
    args = p.parse_args()
    token = args.token or default_token_path()

    cfg = load_config(args.config)
    print("Building payload ...")
    summary = build(cfg, args.out, token, api_fallback=args.api_fallback,
                    skip_entity_files=args.skip_entity_files)

    # Alerts are computed by alerts.py and merged in afterwards, so that a
    # payload build and an alert run can happen independently.
    path = os.path.join(args.out, "summary.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=1)
    print(f"\nwrote {path}  ({len(summary['properties'])} properties)")


if __name__ == "__main__":
    main()
