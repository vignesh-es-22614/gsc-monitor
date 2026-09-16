"""Search Console -> BigQuery export, every property, at two grains.

Why two grains
--------------
``gsc_data.gsc_api_export`` was pulled with the ``query`` dimension, and the
Search Console API drops anonymised queries from any request that includes
that dimension. They are not returned in an "other" bucket -- they are simply
absent. So page totals in that table run 1.3x-6.5x low on impressions and up
to 14.7x low on clicks, and can never be reconciled against the Search
Console UI's Pages report.

This script therefore writes two tables:

    gsc_page_daily    dimensions site_url + date + page
                      -> authoritative page totals, matches the UI
    gsc_query_daily   dimensions site_url + date + page + query + country
                      + device
                      -> query-level detail, still missing anonymised
                         queries (unavoidable at this grain)

Use gsc_page_daily for any total or trend. Use gsc_query_daily only for
relative, within-period query analysis.

Why site_url is a column, not a table per property
--------------------------------------------------
Both tables are partitioned by ``date`` and clustered by ``site_url``, so a
single-property read prunes to roughly what a per-property table would cost,
and a cross-property read is one scan instead of fourteen.

**Never SUM across site_url.** ``https://www.manageengine.com/`` is a
URL-prefix property that contains twelve of the other thirteen, so a row under
``/products/service-desk/`` is counted by both properties. Group by site_url,
or filter to one.

The other defect in the old export was a hard 50,000-row/day cut -- 32 days in
2023 sit at that ceiling. Every request here pages with ``startRow`` until the
API returns a short page, so nothing is silently truncated.

Usage
-----
    python gsc_auth.py --client-secret "...json"   # once, adds GSC scope
    python gsc_export.py --list-sites              # see the properties

    # first full backfill, every property the account can read
    python gsc_export.py --all-sites --start 2025-04-01 --workers 6

    # thereafter: incremental, resumes per property
    python gsc_export.py --all-sites --workers 6
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import datetime as dt
import os
import re
import sys
import time

from google.auth.transport.requests import Request
from google.cloud import bigquery
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

SCOPES = [
    "https://www.googleapis.com/auth/webmasters.readonly",
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/cloud-platform",
]

DEFAULT_PROJECT_ID = "it-security-online-marketing"
DEFAULT_DATASET = "gsc_data"
SITE_TABLE = "gsc_site_daily"
PAGE_TABLE = "gsc_page_daily"
QUERY_TABLE = "gsc_query_daily"

# API hard maximum per request. Anything larger is silently clamped, which is
# how the old export lost rows.
ROW_LIMIT = 25_000

# Search Console finalises data on a lag; pulling closer than this returns
# partial days that later change underneath you.
LAG_DAYS = 3

# Dimensions as the API names them. ``site_url`` is added by us, not returned.
#
# SITE_DIMS carries no entity dimension at all, which is the only way to get
# the figure Search Console's Performance overview shows. It is NOT the sum of
# the page grain: one result listing two of your URLs (sitelinks, or two pages
# ranking for the same query) is one impression at property grain and two at
# page grain. For ad-manager over 28 days that gap is +17.8% on impressions,
# +4.6% on clicks, and 10.3 vs 12.2 on average position. Both are correct; they
# answer different questions, and people quote this one.
SITE_DIMS = ["date"]
PAGE_DIMS = ["date", "page"]
QUERY_DIMS = ["date", "page", "query", "country", "device"]

# Rows held in memory before a flush. The query grain runs ~90k rows/day
# across all properties, so this flushes every few days rather than every day
# -- far fewer load jobs over a 500-day backfill.
FLUSH_ROWS = 500_000


# --------------------------------------------------------------------------- #
# Credentials
# --------------------------------------------------------------------------- #
def default_token_path() -> str:
    """Find gsc_token.json beside this repo, or in the parent project.

    In CI the token is written to the repo root. On the original machine it
    lives one level up, alongside the MCP server and the older BigQuery
    scripts, and is shared with them -- so look there too rather than making
    someone keep two copies in sync.
    """
    here = os.path.dirname(os.path.abspath(__file__))
    for candidate in (
        os.environ.get("GSC_TOKEN_PATH"),
        os.path.join(here, "gsc_token.json"),
        os.path.join(os.path.dirname(here), "gsc_token.json"),
    ):
        if candidate and os.path.exists(candidate):
            return candidate
    return os.path.join(here, "gsc_token.json")


def load_credentials(token_path: str) -> Credentials:
    if not os.path.exists(token_path):
        sys.exit(
            f"Token file not found: {token_path}\n"
            "Run gsc_auth.py first -- the BigQuery token does not carry the "
            "webmasters.readonly scope."
        )

    creds = Credentials.from_authorized_user_file(token_path, scopes=SCOPES)

    granted = set(creds.scopes or [])
    if "https://www.googleapis.com/auth/webmasters.readonly" not in granted:
        sys.exit(
            f"{token_path} has no webmasters.readonly scope.\n"
            "Re-run gsc_auth.py -- Search Console cannot be read without it."
        )

    if not creds.valid:
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            with open(token_path, "w", encoding="utf-8") as fh:
                fh.write(creds.to_json())
        else:
            creds.refresh(Request())

    return creds


# --------------------------------------------------------------------------- #
# Search Console
# --------------------------------------------------------------------------- #
def all_sites(service) -> list[str]:
    """Every property this account can read, permitted ones only."""
    resp = service.sites().list().execute()
    return sorted(
        e["siteUrl"]
        for e in resp.get("siteEntry", [])
        if e.get("permissionLevel") != "siteUnverifiedUser"
    )


def label_of(site_url: str) -> str:
    """Short tag for interleaved log lines from parallel workers."""
    s = re.sub(r"^https?://(www\.)?", "", site_url).strip("/")
    s = s.replace("manageengine.com/products/", "").replace("manageengine.com/", "")
    return (s or "root")[:22]


def list_sites(service) -> None:
    resp = service.sites().list().execute()
    entries = resp.get("siteEntry", [])
    if not entries:
        print("No Search Console properties visible to this account.")
        return
    print(f"{'permission':<22} site")
    for e in sorted(entries, key=lambda x: x["siteUrl"]):
        print(f"{e.get('permissionLevel', ''):<22} {e['siteUrl']}")


def fetch_day(service, site: str, day: dt.date, dimensions: list[str]) -> list[dict]:
    """Pull one day at one grain, paging until the API runs out of rows."""
    iso = day.isoformat()
    rows: list[dict] = []
    start_row = 0

    while True:
        body = {
            "startDate": iso,
            "endDate": iso,
            "dimensions": dimensions,
            "type": "web",
            "dataState": "final",
            "rowLimit": ROW_LIMIT,
            "startRow": start_row,
        }

        for attempt in range(6):
            try:
                resp = (
                    service.searchanalytics()
                    .query(siteUrl=site, body=body)
                    .execute()
                )
                break
            except HttpError as exc:
                # 429 / 5xx are transient; back off and retry. A 403 on one
                # property should not kill a 14-property run, so it is raised
                # and handled by the caller.
                if exc.resp.status in (429, 500, 503) and attempt < 5:
                    time.sleep(2**attempt)
                    continue
                raise
        else:  # pragma: no cover - loop always breaks or raises
            raise RuntimeError("retries exhausted")

        batch = resp.get("rows", [])
        rows.extend(batch)

        if len(batch) < ROW_LIMIT:
            break
        start_row += ROW_LIMIT

    return rows


def shape(rows: list[dict], dimensions: list[str], site: str) -> list[dict]:
    # Stamped per batch so "when did this property last actually load?" is
    # answerable from the data itself -- that is what the freshness alert reads.
    loaded_at = dt.datetime.now(dt.timezone.utc).isoformat()
    out = []
    for r in rows:
        rec = dict(zip(dimensions, r["keys"]))
        rec["site_url"] = site
        rec["clicks"] = int(r.get("clicks", 0))
        rec["impressions"] = int(r.get("impressions", 0))
        # ctr is not stored -- it is exactly clicks/impressions at every grain,
        # and 40M redundant floats is not worth the bytes.
        rec["position"] = round(float(r.get("position", 0.0)), 2)
        rec["loaded_at"] = loaded_at
        out.append(rec)
    return out


# --------------------------------------------------------------------------- #
# BigQuery
# --------------------------------------------------------------------------- #
def schema_for(dimensions: list[str]) -> list[bigquery.SchemaField]:
    # site_url is NULLABLE, not REQUIRED: BigQuery only allows NULLABLE columns
    # to be added to an existing table, and gsc_page_daily predates it.
    fields = [
        bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("site_url", "STRING"),
    ]
    for d in dimensions:
        if d == "date":
            continue
        fields.append(bigquery.SchemaField(d, "STRING"))
    fields += [
        bigquery.SchemaField("clicks", "INT64"),
        bigquery.SchemaField("impressions", "INT64"),
        bigquery.SchemaField("position", "FLOAT64"),
        bigquery.SchemaField("loaded_at", "TIMESTAMP"),
    ]
    return fields


def ensure_table(client, table_id: str, dimensions: list[str]) -> None:
    try:
        client.get_table(table_id)
        return
    except Exception:
        pass

    table = bigquery.Table(table_id, schema=schema_for(dimensions))
    table.time_partitioning = bigquery.TimePartitioning(field="date")
    table.clustering_fields = ["site_url"]
    client.create_table(table)
    print(f"  created {table_id}")


def load_rows(client, table_id: str, dimensions: list[str], records: list[dict]) -> None:
    if not records:
        return
    job_config = bigquery.LoadJobConfig(
        schema=schema_for(dimensions),
        write_disposition="WRITE_APPEND",
    )
    client.load_table_from_json(records, table_id, job_config=job_config).result()


def clear_range(client, table_id: str, site: str, lo: dt.date, hi: dt.date) -> None:
    """Drop one property's rows across a date range, once, before loading.

    Done up front rather than per flush. A 500-day query-grain backfill flushes
    ~50 times per property; 50 DELETEs against the same table from several
    worker threads collide on BigQuery's DML concurrency limit, and each one
    costs a full job round trip. One DELETE per (property, grain) is both
    cheaper and conflict-free, and re-running the same range stays idempotent
    because the delete happens again.

    An earlier version staged into ``<table>_staging`` and INSERT..SELECTed
    across. Deleting and recreating that staging table on every flush tripped
    BigQuery's metadata cache -- the second flush died with "Destination
    deleted/expired during operation". Loading straight into the target has no
    such race and is one job fewer.

    The DELETE is scoped to one site_url: properties overlap in page space (the
    www root contains the product sub-paths), so a date-only delete would wipe
    a neighbour's rows.
    """
    for attempt in range(5):
        try:
            client.query(
                f"DELETE FROM `{table_id}` "
                f"WHERE date BETWEEN '{lo.isoformat()}' AND '{hi.isoformat()}' "
                f"AND site_url = @site",
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("site", "STRING", site)
                    ]
                ),
            ).result()
            return
        except Exception as exc:  # noqa: BLE001
            # Concurrent DML against one table serialises; losers are asked to
            # retry rather than being queued.
            if "concurrent" in str(exc).lower() and attempt < 4:
                time.sleep(5 * (attempt + 1))
                continue
            raise


def last_loaded_date(client, table_id: str, site: str) -> dt.date | None:
    try:
        client.get_table(table_id)
    except Exception:
        return None
    job = client.query(
        f"SELECT MAX(date) d FROM `{table_id}` WHERE site_url = @site",
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("site", "STRING", site)]
        ),
    )
    rows = list(job.result())
    return rows[0]["d"] if rows and rows[0]["d"] else None


# --------------------------------------------------------------------------- #
# One property, one grain
# --------------------------------------------------------------------------- #
def export_site_grain(
    bq,
    service,
    site: str,
    label: str,
    table_id: str,
    dims: list[str],
    start: dt.date,
    end: dt.date,
    append_only: bool,
    tag: str = "",
    newest_first: bool = True,
) -> int:
    """Pull [start, end] for one property at one grain. Returns rows written."""
    n_days = (end - start).days + 1
    print(f"{tag}[{label}] {start} -> {end}  ({n_days} days)", flush=True)

    if not append_only:
        clear_range(bq, table_id, site, start, end)

    buffer: list[dict] = []
    total_rows = 0

    # Newest first by default. A dashboard is judged on the last 28 days, and
    # loading chronologically means those arrive last -- a property can be 80%
    # backfilled and still show nothing useful. Reverse order makes every
    # property useful within minutes and the deep history fill in behind it.
    days = []
    d = start
    while d <= end:
        days.append(d)
        d += dt.timedelta(days=1)
    if newest_first:
        days.reverse()

    for day in days:
        try:
            raw = fetch_day(service, site, day, dims)
        except HttpError as exc:
            print(f"{tag}  {day}  API error {exc.resp.status} -- skipped", flush=True)
            continue

        buffer.extend(shape(raw, dims, site))
        total_rows += len(raw)

        # Out-of-window days come back empty. Log only month starts and unusually
        # big days, so a 500-day backfill does not print 500 lines per property.
        if raw and (len(raw) >= 20_000 or day.day == 1):
            print(f"{tag}  {day}  {len(raw):>7,} rows", flush=True)

        if len(buffer) >= FLUSH_ROWS:
            load_rows(bq, table_id, dims, buffer)
            buffer = []

    load_rows(bq, table_id, dims, buffer)
    print(f"{tag}[{label}] {total_rows:,} rows", flush=True)
    return total_rows


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main() -> None:
    p = argparse.ArgumentParser(description="Export Search Console to BigQuery.")
    p.add_argument("--token", default=None)
    p.add_argument(
        "--site",
        action="append",
        help="Property to export. Repeatable. See --list-sites.",
    )
    p.add_argument(
        "--all-sites",
        action="store_true",
        help="Export every property the account can read.",
    )
    p.add_argument("--list-sites", action="store_true")
    p.add_argument("--start", help="First date, YYYY-MM-DD. Default: resume.")
    p.add_argument("--end", help="Last date, YYYY-MM-DD. Default: today - 3d.")
    p.add_argument("--project", default=DEFAULT_PROJECT_ID)
    p.add_argument("--dataset", default=DEFAULT_DATASET)
    p.add_argument(
        "--grain",
        choices=["site", "page", "query", "both", "all"],
        default="all",
        help=(
            "site = property totals matching the UI's Performance overview; "
            "page = per-page totals matching the UI's Pages report; "
            "query = query/country/device detail; both = page+query; "
            "all (default) = every grain."
        ),
    )
    p.add_argument(
        "--workers",
        type=int,
        default=1,
        help=(
            "Pull this many properties in parallel. Search Console quota is "
            "per property, so 5-6 is comfortable; the limit that bites first "
            "is the per-account 1,200 queries/minute."
        ),
    )
    p.add_argument(
        "--oldest-first",
        action="store_true",
        help=(
            "Load a backfill chronologically instead of newest-first. Newest "
            "first is the default so the recent window a dashboard actually "
            "shows is populated early."
        ),
    )
    p.add_argument(
        "--append-only",
        action="store_true",
        help=(
            "Skip the per-flush DELETE and append straight to the table. Only "
            "safe when the (site, date range) is known to hold no rows yet -- "
            "a first backfill of a new property. Re-running it over loaded "
            "dates duplicates them."
        ),
    )
    args = p.parse_args()

    creds = load_credentials(
        os.path.abspath(args.token) if args.token else default_token_path()
    )
    service = build("searchconsole", "v1", credentials=creds)

    if args.list_sites:
        list_sites(service)
        return

    if args.all_sites:
        sites = all_sites(service)
    elif args.site:
        sites = args.site
    else:
        sys.exit("Pass --site (repeatable) or --all-sites. See --list-sites.")

    bq = bigquery.Client(project=args.project, credentials=creds)
    site_id = f"{args.project}.{args.dataset}.{SITE_TABLE}"
    page_id = f"{args.project}.{args.dataset}.{PAGE_TABLE}"
    query_id = f"{args.project}.{args.dataset}.{QUERY_TABLE}"

    grains = []
    if args.grain in ("site", "all"):
        ensure_table(bq, site_id, SITE_DIMS)
        grains.append(("site", site_id, SITE_DIMS))
    if args.grain in ("page", "both", "all"):
        ensure_table(bq, page_id, PAGE_DIMS)
        grains.append(("page", page_id, PAGE_DIMS))
    if args.grain in ("query", "both", "all"):
        ensure_table(bq, query_id, QUERY_DIMS)
        grains.append(("query", query_id, QUERY_DIMS))

    end = (
        dt.date.fromisoformat(args.end)
        if args.end
        else dt.date.today() - dt.timedelta(days=LAG_DAYS)
    )

    started = dt.datetime.now()
    workers = max(1, min(args.workers, len(sites)))
    print(
        f"{len(sites)} propert{'y' if len(sites) == 1 else 'ies'} -> {end}"
        f"  ({workers} worker{'' if workers == 1 else 's'})\n"
    )

    def run_site(i: int, site: str) -> int:
        # googleapiclient's service object is not thread safe -- each worker
        # builds its own. The BigQuery client is, and load jobs to one table
        # from several threads are serialised server side.
        svc = build("searchconsole", "v1", credentials=creds) if workers > 1 else service
        tag = f"[{i}/{len(sites)} {label_of(site)}] " if workers > 1 else "  "
        if workers == 1:
            print(f"[{i}/{len(sites)}] {site}", flush=True)

        written = 0
        for label, table_id, dims in grains:
            if args.start:
                start = dt.date.fromisoformat(args.start)
            else:
                last = last_loaded_date(bq, table_id, site)
                if last is None:
                    print(
                        f"{tag}[{label}] no rows for this property yet -- "
                        f"pass --start for the first backfill.",
                        flush=True,
                    )
                    continue
                start = last + dt.timedelta(days=1)

            if start > end:
                print(f"{tag}[{label}] up to date (through {end}).", flush=True)
                continue

            written += export_site_grain(
                bq, svc, site, label, table_id, dims, start, end,
                args.append_only, tag,
                # Newest-first only for an explicit --start range. On a resume
                # the start comes from MAX(date), so if a newest-first run were
                # interrupted MAX would already sit at the end and the next
                # resume would skip everything it had not reached. An explicit
                # range is re-cleared and re-pulled wholesale, so it is safe.
                newest_first=(not args.oldest_first and bool(args.start)),
            )
        return written

    grand_total = 0
    if workers == 1:
        for i, site in enumerate(sites, 1):
            grand_total += run_site(i, site)
    else:
        with cf.ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(run_site, i, s): s for i, s in enumerate(sites, 1)
            }
            for fut in cf.as_completed(futures):
                site = futures[fut]
                try:
                    grand_total += fut.result()
                except Exception as exc:  # noqa: BLE001
                    # One property failing must not lose the other thirteen.
                    print(f"!! {site} FAILED: {exc}", flush=True)

    elapsed = dt.datetime.now() - started
    print(f"\ndone -- {grand_total:,} rows in {elapsed}")

    # Show page totals per property so they can be eyeballed against the UI.
    if args.grain in ("page", "both", "all"):
        print("\nLast 30 days by property (check vs Search Console > Pages):")
        sql = f"""
        SELECT site_url, SUM(clicks) clicks, SUM(impressions) impressions,
               COUNT(DISTINCT page) pages
        FROM `{page_id}`
        WHERE date BETWEEN '{(end - dt.timedelta(days=29)).isoformat()}'
                       AND '{end.isoformat()}'
        GROUP BY site_url ORDER BY clicks DESC
        """
        for r in bq.query(sql).result():
            print(
                f"    {r['clicks']:>9,} {r['impressions']:>12,} "
                f"{r['pages']:>6,}  {r['site_url']}"
            )


if __name__ == "__main__":
    main()
