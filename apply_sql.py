"""Apply a .sql file of CREATE OR REPLACE statements to BigQuery.

Comments are stripped before the file is split on semicolons, not after: a
prose comment containing a semicolon would otherwise cut a statement in half,
and the halves fail with errors that point nowhere near the real problem.

    python apply_sql.py sql/01_views.sql sql/02_dimensions.sql
"""

from __future__ import annotations

import argparse
import os
import re
import sys

from google.cloud import bigquery

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from gsc_export import default_token_path, load_credentials  # noqa: E402

PROJECT = "it-security-online-marketing"


def statements(sql: str) -> list[str]:
    no_comments = "\n".join(
        re.sub(r"(^|\s)--.*$", "", line) for line in sql.splitlines()
    )
    return [s.strip() for s in no_comments.split(";") if s.strip()]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("files", nargs="+")
    ap.add_argument("--token", default=None)
    args = ap.parse_args()

    bq = bigquery.Client(
        project=PROJECT,
        credentials=load_credentials(args.token or default_token_path()),
    )

    failed = 0
    for path in args.files:
        print(f"\n{path}")
        for stmt in statements(open(path, encoding="utf-8").read()):
            m = re.search(r"(VIEW|FUNCTION|TABLE)\s+`([^`]+)`", stmt)
            name = m.group(2).split(".")[-1] if m else stmt[:46]
            try:
                bq.query(stmt).result()
                print(f"  ok    {name}")
            except Exception as exc:  # noqa: BLE001
                failed += 1
                print(f"  FAIL  {name}\n        {str(exc).splitlines()[0][:200]}")

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
