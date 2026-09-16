"""Export Search Console data from BigQuery to CSV, with no row cap.

The dashboard ships a bounded payload -- it has to, because a static page has
to download everything it shows. This is the way out of that bound: it streams
straight from BigQuery, so "every query for every property for the last six
months" is a normal thing to ask for.

    # every page, every property, last 28 days
    python export_data.py --grain page --days 28

    # every query for one property, a named range, one file
    python export_data.py --grain query \\
        --site "https://www.manageengine.com/products/ad-manager/" \\
        --start 2026-01-01 --end 2026-09-13

    # page x query x country x device -- the finest grain stored
    python export_data.py --grain page_query --days 28 --split

    # property totals, the figure the Search Console overview shows
    python export_data.py --grain site --days 90

`--split` writes one file per property instead of one combined file.
`--gzip` compresses on the way out; a year of query data is worth it.

Which grain answers which question
----------------------------------
    site        property totals. Matches the UI's Performance overview.
    page        per page. Matches the UI's Pages report. Sums HIGHER than
                `site` -- one result showing two of your URLs is one
                impression at property grain and two at page grain.
    query       per query.      } both omit anonymised queries, so they sum
    page_query  per page+query. } below the real totals. Ranking is sound;
                                  the absolute numbers are not.
    daily_*     add `--daily` to any of the above to keep the date column
                instead of aggregating the range away.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import gzip
import os
import re
import sys

from google.cloud import bigquery

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from gsc_export import default_token_path, load_credentials  # noqa: E402

PROJECT = "it-security-online-marketing"
DATASET = "gsc_data"

# (view, dimension columns) per grain. All read the deduplicating views --
# concurrent loaders mean the raw tables carry duplicate keys.
GRAINS = {
    "site": ("v_site_daily", []),
    "page": ("v_page_daily", ["page"]),
    "query": ("v_query_daily", ["query"]),
    "page_query": ("v_query_daily", ["page", "query"]),
    "country": ("v_query_daily", ["country"]),
    "device": ("v_query_daily", ["device"]),
    "full": ("v_query_daily", ["page", "query", "country", "device"]),
}

UNDERSTATED = {"query", "page_query", "country", "device", "full"}


def slug(site_url: str) -> str:
    s = re.sub(r"^https?://", "", site_url).strip("/")
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


def build_sql(grain: str, start: dt.date, end: dt.date, daily: bool,
              site: str | None) -> str:
    view, dims = GRAINS[grain]
    group = (["date"] if daily else []) + ["site_url"] + dims
    select = ",\n           ".join(group)
    where = f"date BETWEEN '{start}' AND '{end}'"
    if site:
        where += " AND site_url = @site"
    return f"""
    SELECT {select},
           SUM(clicks) AS clicks,
           SUM(impressions) AS impressions,
           SAFE_DIVIDE(SUM(clicks), SUM(impressions)) AS ctr,
           -- Impression-weighted, which is how Search Console averages it.
           SAFE_DIVIDE(SUM(position * impressions), SUM(impressions)) AS position
    FROM `{PROJECT}.{DATASET}.{view}`
    WHERE {where}
    GROUP BY {select}
    ORDER BY clicks DESC, impressions DESC
    """


def write_csv(rows, path: str, use_gzip: bool) -> int:
    opener = (lambda p: gzip.open(p, "wt", newline="", encoding="utf-8")) if use_gzip \
        else (lambda p: open(p, "w", newline="", encoding="utf-8"))
    n = 0
    with opener(path) as fh:
        w = None
        for r in rows:
            d = dict(r)
            if w is None:
                w = csv.DictWriter(fh, fieldnames=list(d))
                w.writeheader()
            if d.get("ctr") is not None:
                d["ctr"] = round(d["ctr"], 6)
            if d.get("position") is not None:
                d["position"] = round(d["position"], 2)
            w.writerow(d)
            n += 1
    return n


def main() -> None:
    p = argparse.ArgumentParser(
        description="Export Search Console data from BigQuery to CSV.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--grain", choices=sorted(GRAINS), default="page")
    p.add_argument("--site", help="One property. Default: all of them.")
    p.add_argument("--start", help="YYYY-MM-DD.")
    p.add_argument("--end", help="YYYY-MM-DD. Default: latest loaded date.")
    p.add_argument("--days", type=int, help="Last N days instead of --start.")
    p.add_argument("--daily", action="store_true",
                   help="Keep a date column instead of aggregating the range.")
    p.add_argument("--split", action="store_true",
                   help="One file per property instead of one combined file.")
    p.add_argument("--gzip", action="store_true", help="Write .csv.gz.")
    p.add_argument("--out", default="exports", help="Output directory.")
    p.add_argument("--token", default=None)
    args = p.parse_args()

    bq = bigquery.Client(
        project=PROJECT,
        credentials=load_credentials(args.token or default_token_path()),
    )

    view = GRAINS[args.grain][0]
    end = (
        dt.date.fromisoformat(args.end) if args.end
        else list(bq.query(
            f"SELECT MAX(date) d FROM `{PROJECT}.{DATASET}.{view}`").result())[0]["d"]
    )
    if args.start:
        start = dt.date.fromisoformat(args.start)
    elif args.days:
        start = end - dt.timedelta(days=args.days - 1)
    else:
        start = end - dt.timedelta(days=27)

    os.makedirs(args.out, exist_ok=True)
    print(f"grain={args.grain}  {start} -> {end}  from {view}")
    if args.grain in UNDERSTATED:
        print("  NOTE: query-dimensioned. Search Console omits anonymised "
              "queries, so these sum below the real totals. Ranking is sound; "
              "use --grain site or page for any figure you intend to quote.")

    sites = [args.site] if args.site else [
        r["site_url"] for r in bq.query(
            f"SELECT DISTINCT site_url FROM `{PROJECT}.{DATASET}.{view}` "
            f"WHERE date BETWEEN '{start}' AND '{end}' ORDER BY site_url").result()
    ]

    ext = ".csv.gz" if args.gzip else ".csv"
    targets = sites if (args.split or args.site) else [None]
    total = 0

    for target in targets:
        sql = build_sql(args.grain, start, end, args.daily, target)
        cfg = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("site", "STRING", target)
        ]) if target else bigquery.QueryJobConfig()
        job = bq.query(sql, job_config=cfg)

        name = "-".join(filter(None, [
            "gsc", args.grain, "daily" if args.daily else None,
            slug(target) if target else "all-properties",
            f"{start}_{end}",
        ])) + ext
        path = os.path.join(args.out, name)

        n = write_csv(job.result(), path, args.gzip)
        total += n
        size = os.path.getsize(path) / 1024 / 1024
        print(f"  {n:>9,} rows  {size:>7.1f} MB  {path}")
        print(f"            scanned {job.total_bytes_processed / 1e9:.2f} GB")

    print(f"\n{total:,} rows written to {os.path.abspath(args.out)}")


if __name__ == "__main__":
    main()
