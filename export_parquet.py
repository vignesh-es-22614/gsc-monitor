"""BigQuery -> Parquet, so the browser can run SQL over the raw rows.

The dashboard's precomputed windows cannot answer an arbitrary question: a
custom date range crossed with country, device, page and query is a slice of
73M rows, and there is no set of precomputed files that covers every slice
anyone might ask for.

DuckDB-WASM can, if the data is sitting on the same static host as Parquet.
It issues HTTP range requests, reads only the row groups and column chunks a
query actually touches, and never downloads a file whole unless the query needs
it whole. So the shape of these files matters more than their total size:

  * One file per (grain, property, month). A 28-day question opens one or two
    files and ignores the rest; a property nobody selects is never fetched.
  * Sorted by the columns people filter on, so row-group statistics let DuckDB
    skip most of a file.
  * Dictionary-encoded and zstd-compressed. Page URLs and queries repeat
    heavily within a month, which is the best case for dictionary encoding.

    python export_parquet.py --days 365          # rolling year, all grains
    python export_parquet.py --grain query --site "https://..." --days 90
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import sys

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import bigquery

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from gsc_export import default_token_path, load_credentials  # noqa: E402

PROJECT = "it-security-online-marketing"
DATASET = "gsc_data"

# (view, columns). `site` is tiny and ships as one file per property.
#
# The page grain additionally carries the theme dimensions and the CRM
# outcomes, because both are per-page facts and putting them in the same row
# lets every tab group by them without a second fetch or a client-side join.
GRAINS = {
    "site": ("v_site_daily", []),
    "page": ("v_page_daily", ["page"]),
    "query": ("v_query_daily", ["page", "query", "country", "device"]),
    # Paid and organic on the same search term. Its own grain because it has
    # its own metric set and no page dimension -- a search term maps to an ad,
    # not a URL. Terms bought with no organic presence live under the
    # "(paid only)" pseudo-property, since they belong to no GSC property.
    "semseo": ("term_seo_sem_daily", ["term"]),
}

# Lead channel that lands in the page grain. Organic by default: this is a
# Search Console dashboard, and putting paid leads next to organic clicks
# invites the reading that the clicks produced them. SEM outnumbers SEO here
# roughly 16,000 to 4,700, so the distinction is not academic.
DEFAULT_LEAD_CHANNEL = "SEO"

# Bump whenever the Parquet column set changes.
#
# --resume and --refresh-days both keep files that already exist, and CI
# carries them between runs in a cache, so a schema change otherwise never
# reaches the published data: the page grain gained theme and CRM columns and
# the browser went on querying year-old files that lacked them, failing with
# "Referenced column leads_first not found". A file whose recorded version is
# not this one is rebuilt no matter which resume flag is set.
SCHEMA_VERSION = 2

# Narrow types matter at 73M rows: int64 clicks would cost 4 bytes a row more
# than anything in this data needs.
SCHEMA_TYPES = {
    "date": pa.date32(),
    "page": pa.string(),
    "query": pa.string(),
    "country": pa.string(),
    "device": pa.string(),
    "clicks": pa.int32(),
    "impressions": pa.int32(),
    "position": pa.float32(),
    # Page dimensions from the Page Themes workbook.
    "theme": pa.string(),
    "sub_theme": pa.string(),
    "page_type": pa.string(),
    # CRM outcomes, both attributions. A lead's first-source page and
    # last-source page are usually different, so these two sets describe
    # different pages and must never be added together.
    "leads_first": pa.int32(),
    "conv_first": pa.int32(),
    "rev_first": pa.float32(),
    "leads_last": pa.int32(),
    "conv_last": pa.int32(),
    "rev_last": pa.float32(),
    # SEO vs SEM. Cost is INR, as the source column says -- not the USD the
    # semroi tables carry, and the two must never be added.
    "term": pa.string(),
    "seo_clicks": pa.int32(),
    "seo_impressions": pa.int32(),
    "seo_position": pa.float32(),
    "sem_clicks": pa.int32(),
    "sem_impressions": pa.int32(),
    "sem_cost_inr": pa.float32(),
    "sem_conversions": pa.float32(),
}


_USE_BQSTORAGE = True


def slug(site_url: str) -> str:
    s = re.sub(r"^https?://", "", site_url).strip("/")
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


def months_between(start: dt.date, end: dt.date) -> list[tuple[dt.date, dt.date]]:
    out = []
    cur = start.replace(day=1)
    while cur <= end:
        nxt = (cur.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
        out.append((max(cur, start), min(nxt - dt.timedelta(days=1), end)))
        cur = nxt
    return out


def fetch(bq, grain: str, site: str, lo: dt.date, hi: dt.date,
          lead_channel: str = DEFAULT_LEAD_CHANNEL) -> pa.Table:
    view, dims = GRAINS[grain]
    cols = ["date"] + dims
    select = ", ".join(cols)
    # Sorted on the filter columns so each row group covers a narrow slice and
    # DuckDB can skip whole groups from Parquet statistics alone.
    order = ", ".join(dims[:2]) or "date"

    if grain == "semseo":
        sql = f"""
        SELECT date, term,
               CAST(SUM(seo_clicks) AS INT64) AS seo_clicks,
               CAST(SUM(seo_impressions) AS INT64) AS seo_impressions,
               SAFE_DIVIDE(SUM(seo_position * seo_impressions),
                           SUM(seo_impressions)) AS seo_position,
               CAST(SUM(sem_clicks) AS INT64) AS sem_clicks,
               CAST(SUM(sem_impressions) AS INT64) AS sem_impressions,
               SUM(sem_cost_inr) AS sem_cost_inr,
               SUM(sem_conversions) AS sem_conversions
        FROM `{PROJECT}.{DATASET}.term_seo_sem_daily`
        WHERE site_url = @site AND date BETWEEN '{lo}' AND '{hi}'
        GROUP BY date, term
        ORDER BY term
        """
    elif grain != "page":
        sql = f"""
        SELECT {select},
               CAST(SUM(clicks) AS INT64) AS clicks,
               CAST(SUM(impressions) AS INT64) AS impressions,
               SAFE_DIVIDE(SUM(position * impressions), SUM(impressions)) AS position
        FROM `{PROJECT}.{DATASET}.{view}`
        WHERE site_url = @site AND date BETWEEN '{lo}' AND '{hi}'
        GROUP BY {select}
        ORDER BY {order}
        """
    else:
        # Page grain carries the dimensions and the outcomes. The lead joins
        # are LEFT and on (page_path, date): most pages never produce a lead,
        # and a page with no lead must still appear with its clicks.
        sql = f"""
        WITH f AS (
          SELECT date, page,
                 CAST(SUM(clicks) AS INT64) AS clicks,
                 CAST(SUM(impressions) AS INT64) AS impressions,
                 SAFE_DIVIDE(SUM(position*impressions), SUM(impressions)) AS position
          FROM `{PROJECT}.{DATASET}.v_page_daily`
          WHERE site_url = @site AND date BETWEEN '{lo}' AND '{hi}'
          GROUP BY date, page
        ),
        lf AS (
          SELECT date, page_path, SUM(leads) leads, SUM(conversions) conv,
                 SUM(revenue) rev
          FROM `{PROJECT}.{DATASET}.page_leads_daily`
          WHERE attribution = 'first' AND date BETWEEN '{lo}' AND '{hi}'
            AND ('{lead_channel}' = 'ALL' OR channel = '{lead_channel}')
          GROUP BY date, page_path
        ),
        ll AS (
          SELECT date, page_path, SUM(leads) leads, SUM(conversions) conv,
                 SUM(revenue) rev
          FROM `{PROJECT}.{DATASET}.page_leads_daily`
          WHERE attribution = 'last' AND date BETWEEN '{lo}' AND '{hi}'
            AND ('{lead_channel}' = 'ALL' OR channel = '{lead_channel}')
          GROUP BY date, page_path
        )
        SELECT f.date, f.page, f.clicks, f.impressions, f.position,
               IFNULL(d.theme, '(unthemed)')     AS theme,
               IFNULL(d.sub_theme, '(unthemed)') AS sub_theme,
               IFNULL(d.page_type, '(unknown)')  AS page_type,
               CAST(IFNULL(lf.leads, 0) AS INT64) AS leads_first,
               CAST(IFNULL(lf.conv, 0)  AS INT64) AS conv_first,
               IFNULL(lf.rev, 0)                  AS rev_first,
               CAST(IFNULL(ll.leads, 0) AS INT64) AS leads_last,
               CAST(IFNULL(ll.conv, 0)  AS INT64) AS conv_last,
               IFNULL(ll.rev, 0)                  AS rev_last
        FROM f
        LEFT JOIN `{PROJECT}.{DATASET}.page_dim` d
          ON d.site_url = @site AND d.page = f.page
        LEFT JOIN lf ON lf.page_path = `{PROJECT}.{DATASET}.norm_path`(f.page)
                    AND lf.date = f.date
        LEFT JOIN ll ON ll.page_path = `{PROJECT}.{DATASET}.norm_path`(f.page)
                    AND ll.date = f.date
        ORDER BY f.page
        """
    job = bq.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("site", "STRING", site)]
        ),
    )
    # The BigQuery Storage API is much faster, but it speaks gRPC and dies
    # behind an HTTP-only proxy with "failed to connect to all addresses".
    # Fall back to the REST download rather than failing the export -- slower,
    # but it works everywhere. Once the first attempt fails there is no point
    # retrying it for every later chunk.
    global _USE_BQSTORAGE
    if _USE_BQSTORAGE:
        try:
            tbl = job.to_arrow()
        except Exception as exc:  # noqa: BLE001
            print(f"    BigQuery Storage API unavailable ({type(exc).__name__}); "
                  f"falling back to REST for the rest of this run.", flush=True)
            _USE_BQSTORAGE = False
            tbl = job.to_arrow(create_bqstorage_client=False)
    else:
        tbl = job.to_arrow(create_bqstorage_client=False)

    fields = [pa.field(c, SCHEMA_TYPES[c]) for c in tbl.column_names]
    return tbl.cast(pa.schema(fields)), job.total_bytes_processed


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--grain", choices=list(GRAINS) + ["all"], default="all")
    p.add_argument("--site", help="One property. Default: all of them.")
    p.add_argument("--days", type=int, default=365,
                   help="Rolling window to publish. Older history stays in "
                        "BigQuery and is reachable with export_data.py.")
    p.add_argument(
        "--refresh-days",
        type=int,
        help=(
            "Only rebuild the month files touched by the last N days, keeping "
            "the rest. A full year costs ~50 GB of BigQuery scan; daily that "
            "would exceed the 1 TB free tier on its own. Search Console only "
            "revises the last few days, so 10 is ample for the daily run. "
            "Omit for a full rebuild."
        ),
    )
    p.add_argument(
        "--resume",
        action="store_true",
        help="Skip any month whose parquet file already exists on disk. For "
             "picking up an export that died part way without re-scanning "
             "what it already wrote.",
    )
    p.add_argument(
        "--lead-channel",
        default=DEFAULT_LEAD_CHANNEL,
        help=("Which CRM channel's leads land in the page grain. 'SEO' (the "
              "default) is the one comparable with organic clicks; 'ALL' "
              "includes paid, email and the rest, which will not match what "
              "Search Console earned."),
    )
    p.add_argument("--out", default=os.path.join(HERE, "docs", "data", "pq"))
    p.add_argument("--token", default=None)
    args = p.parse_args()

    bq = bigquery.Client(
        project=PROJECT,
        credentials=load_credentials(args.token or default_token_path()),
    )

    end = list(bq.query(
        f"SELECT MAX(date) d FROM `{PROJECT}.{DATASET}.v_page_daily`").result())[0]["d"]
    start = end - dt.timedelta(days=args.days - 1)

    sites = [args.site] if args.site else [
        r["site_url"] for r in bq.query(
            f"SELECT DISTINCT site_url FROM `{PROJECT}.{DATASET}.v_page_daily` "
            f"ORDER BY site_url").result()
    ]
    # Bought-but-not-ranking terms belong to no property, so they need their
    # own bucket or they would be dropped from the export entirely.
    if not args.site and args.grain in ("semseo", "all"):
        sites = sites + ["(paid only)"]
    grains = list(GRAINS) if args.grain == "all" else [args.grain]

    print(f"{start} -> {end}  ({args.days} days)  "
          f"{len(sites)} properties  grains={','.join(grains)}\n")

    # Incremental runs keep the month files they are not rewriting, so the
    # manifest starts from whatever is already published.
    manifest_path = os.path.join(args.out, "manifest.json")
    old: dict = {}
    if args.refresh_days and os.path.exists(manifest_path):
        with open(manifest_path, encoding="utf-8") as fh:
            old = json.load(fh).get("properties", {})

    stale_from = (end - dt.timedelta(days=args.refresh_days - 1)) if args.refresh_days else start

    # A cache written by an older schema must not be reused.
    old_version = 0
    if args.refresh_days and os.path.exists(manifest_path):
        with open(manifest_path, encoding="utf-8") as fh:
            old_version = json.load(fh).get("schema_version", 0)
    if old and old_version != SCHEMA_VERSION:
        print(f"  published data is schema v{old_version}, this is "
              f"v{SCHEMA_VERSION} -- rebuilding every file", flush=True)
        old = {}

    manifest: dict = {"start": start.isoformat(), "end": end.isoformat(),
                      "schema_version": SCHEMA_VERSION,
                      "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(
                          timespec="seconds"),
                      "properties": {}}
    total_bytes = total_rows = scanned = skipped = 0

    for site in sites:
        sl = slug(site)
        for grain in grains:
            d = os.path.join(args.out, grain, sl)
            os.makedirs(d, exist_ok=True)
            files = []
            prior = {f["m"]: f for f in old.get(sl, {}).get(grain, [])}
            for lo, hi in months_between(start, end):
                key = f"{lo:%Y-%m}"
                path_existing = os.path.join(d, f"{key}.parquet")
                # --resume reuses a file only if it was written by this schema;
                # otherwise the browser queries columns it does not have.
                resume_ok = args.resume and os.path.exists(path_existing) and (
                    grain != "page"
                    or "leads_first" in pq.read_schema(path_existing).names
                )
                if resume_ok:
                    sz = os.path.getsize(path_existing)
                    files.append({"m": key, "rows": pq.read_metadata(path_existing).num_rows,
                                  "bytes": sz})
                    total_bytes += sz
                    skipped += 1
                    continue
                # Untouched month whose file is still on disk: keep it.
                if hi < stale_from and key in prior \
                        and os.path.exists(path_existing):
                    files.append(prior[key])
                    total_bytes += prior[key]["bytes"]
                    total_rows += prior[key]["rows"]
                    skipped += 1
                    continue
                tbl, scan = fetch(bq, grain, site, lo, hi, args.lead_channel)
                scanned += scan
                if tbl.num_rows == 0:
                    continue
                name = f"{lo:%Y-%m}.parquet"
                path = os.path.join(d, name)
                pq.write_table(
                    tbl, path,
                    compression="zstd", compression_level=9,
                    use_dictionary=True,
                    # Small enough that a selective filter reads a fraction of
                    # the file, large enough that per-group overhead is noise.
                    row_group_size=120_000,
                )
                size = os.path.getsize(path)
                files.append({"m": f"{lo:%Y-%m}", "rows": tbl.num_rows, "bytes": size})
                total_bytes += size
                total_rows += tbl.num_rows
            manifest["properties"].setdefault(sl, {})[grain] = files
            n = sum(f["rows"] for f in files)
            b = sum(f["bytes"] for f in files)
            if n:
                print(f"  {sl:<46} {grain:<6} {n:>10,} rows  {b/1e6:>7.1f} MB  "
                      f"{b/max(n,1):>4.1f} B/row", flush=True)

    with open(manifest_path, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, separators=(",", ":"))

    print(f"\n{total_rows:,} rows  {total_bytes/1e6:.1f} MB parquet  "
          f"({scanned/1e9:.1f} GB scanned in BigQuery"
          f"{f', {skipped} month files reused' if skipped else ''})")


if __name__ == "__main__":
    main()
