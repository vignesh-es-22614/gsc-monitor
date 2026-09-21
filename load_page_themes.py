"""Load the Page Themes workbook into BigQuery as gsc_data.page_themes.

The workbook's Page column is a path, not a URL, and it carries locale prefixes
(/za/, /in/, /de/...). Search Console reports absolute URLs. So the join key is
a normalised path: lowercased, query string and fragment dropped, trailing
slash made consistent, and the locale prefix stripped into its own column so a
theme defined once applies to every locale of the same page.

    python load_page_themes.py "Page Themes (3).xlsx"
"""

from __future__ import annotations

import argparse
import os
import re
import sys

import openpyxl
from google.cloud import bigquery

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "gsc-monitor"))

from gsc_export import default_token_path, load_credentials  # noqa: E402

PROJECT = "it-security-online-marketing"
DATASET = "gsc_data"
TABLE = "page_themes"

# Two-letter locale directories ManageEngine uses. Anything else at the front
# of a path is a real section, not a locale.
LOCALES = {
    "au", "br", "ca", "cn", "de", "es", "fr", "in", "it", "jp", "kr", "mx",
    "nl", "pl", "pt", "ru", "sa", "se", "tr", "tw", "uk", "us", "vn", "za",
    "ae", "ar", "at", "be", "ch", "cl", "co", "dk", "fi", "gr", "hk", "id",
    "ie", "il", "my", "no", "nz", "ph", "pk", "sg", "th", "ua", "vi",
}


def norm_path(raw: str) -> tuple[str, str]:
    """Return (normalised path, locale). Locale is '' for the default site."""
    if not raw:
        return "", ""
    p = str(raw).strip()
    p = re.sub(r"^https?://[^/]+", "", p)          # tolerate a full URL
    p = p.split("#", 1)[0].split("?", 1)[0]        # drop fragment and query
    p = p.lower()
    if not p.startswith("/"):
        p = "/" + p
    p = re.sub(r"/{2,}", "/", p)

    locale = ""
    m = re.match(r"^/([a-z]{2})(/|$)", p)
    if m and m.group(1) in LOCALES:
        locale = m.group(1)
        p = p[3:] or "/"
        if not p.startswith("/"):
            p = "/" + p
    return p, locale


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("xlsx")
    ap.add_argument("--sheet", default=None)
    ap.add_argument("--token", default=None)
    args = ap.parse_args()

    wb = openpyxl.load_workbook(args.xlsx, read_only=True, data_only=True)
    ws = wb[args.sheet] if args.sheet else wb[wb.sheetnames[0]]

    rows_in = list(ws.iter_rows(values_only=True))
    header = [str(h or "").strip().lower() for h in rows_in[0]]
    idx = {name: header.index(name) for name in ("page", "theme", "sub theme", "type")
           if name in header}
    missing = {"page", "theme"} - set(idx)
    if missing:
        sys.exit(f"workbook is missing column(s): {', '.join(sorted(missing))}")

    seen: dict[str, dict] = {}
    dupes = blank = 0
    for r in rows_in[1:]:
        raw = r[idx["page"]] if idx.get("page") is not None else None
        if not raw:
            blank += 1
            continue
        path, locale = norm_path(raw)
        if not path:
            blank += 1
            continue
        rec = {
            "page_path": path,
            "locale": locale,
            "theme": (str(r[idx["theme"]]).strip() if r[idx["theme"]] else None),
            "sub_theme": (str(r[idx["sub theme"]]).strip()
                          if idx.get("sub theme") is not None and r[idx["sub theme"]]
                          else None),
            "page_type": (str(r[idx["type"]]).strip()
                          if idx.get("type") is not None and r[idx["type"]] else None),
            "source_page": str(raw).strip(),
        }
        # A path repeated across locales is the same page; keep one row per
        # (path, locale) and let a later duplicate win only if it adds a theme.
        key = f"{path}|{locale}"
        if key in seen:
            dupes += 1
            if not seen[key]["theme"] and rec["theme"]:
                seen[key] = rec
            continue
        seen[key] = rec

    records = list(seen.values())
    print(f"{len(rows_in)-1:,} rows in workbook -> {len(records):,} unique "
          f"(path, locale)   [{dupes:,} duplicates, {blank:,} blank]")

    themes = {r["theme"] for r in records if r["theme"]}
    types_ = {r["page_type"] for r in records if r["page_type"]}
    locales = {r["locale"] for r in records if r["locale"]}
    print(f"  {len(themes)} themes, {len(types_)} page types, "
          f"{len(locales)} locales, {sum(1 for r in records if not r['locale']):,} default-locale rows")

    bq = bigquery.Client(
        project=PROJECT, credentials=load_credentials(args.token or default_token_path())
    )
    table_id = f"{PROJECT}.{DATASET}.{TABLE}"
    schema = [
        bigquery.SchemaField("page_path", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("locale", "STRING"),
        bigquery.SchemaField("theme", "STRING"),
        bigquery.SchemaField("sub_theme", "STRING"),
        bigquery.SchemaField("page_type", "STRING"),
        bigquery.SchemaField("source_page", "STRING"),
    ]
    job = bq.load_table_from_json(
        records, table_id,
        job_config=bigquery.LoadJobConfig(
            schema=schema, write_disposition="WRITE_TRUNCATE",
            clustering_fields=["page_path"],
        ),
    )
    job.result()
    print(f"loaded {bq.get_table(table_id).num_rows:,} rows -> {table_id}")


if __name__ == "__main__":
    main()
