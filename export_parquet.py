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
GRAINS = {
    "site": ("v_site_daily", []),
    "page": ("v_page_daily", ["page"]),
    "query": ("v_query_daily", ["page", "query", "country", "device"]),
}

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


def fetch(bq, grain: str, site: str, lo: dt.date, hi: dt.date) -> pa.Table:
    view, dims = GRAINS[grain]
    cols = ["date"] + dims
    select = ", ".join(cols)
    # Sorted on the filter columns so each row group covers a narrow slice and
    # DuckDB can skip whole groups from Parquet statistics alone.
    order = ", ".join(dims[:2]) or "date"
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

    manifest: dict = {"start": start.isoformat(), "end": end.isoformat(),
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
                if args.resume and os.path.exists(path_existing):
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
                tbl, scan = fetch(bq, grain, site, lo, hi)
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
