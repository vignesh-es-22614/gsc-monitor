"""Anomaly and pipeline-health alerting for the Search Console monitor.

Runs against BigQuery, writes ``docs/data/alerts.json``, merges the result into
``summary.json`` so the dashboard can render it, and emails a digest.

What it compares
----------------
The last N complete days against the N days immediately before them, N from
``alerts.config.json`` (default 7). Same number of Saturdays on each side --
Search Console traffic is strongly weekly, and a "vs last month" comparison
mostly measures how many weekends each period happened to contain.

Every check carries both a percentage threshold and a minimum absolute
movement. A page going from 3 clicks to 1 is a 67% drop and means nothing; the
absolute floor is what stops the digest filling up with those.

Grain discipline
----------------
Property and page checks read ``gsc_page_daily``, which matches the Search
Console UI. Query checks read ``gsc_query_daily``, which omits anonymised
queries -- fine for "this query lost ground", useless as a total, so query
alerts never roll up into a property number.

    python alerts.py --out docs/data            # detect, write, email
    python alerts.py --out docs/data --no-email # detect and write only
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import smtplib
import ssl
import sys
from email.message import EmailMessage
from email.utils import formatdate

from google.cloud import bigquery

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from gsc_export import default_token_path, load_credentials  # noqa: E402

PROJECT = "it-security-online-marketing"
DATASET = "gsc_data"

# The deduplicating views, not the raw tables -- concurrent loaders append
# overlapping rows and a drop alert computed off double-counted clicks is
# worse than no alert. See sql/01_views.sql.
PAGE_TABLE = f"`{PROJECT}.{DATASET}.v_page_daily`"
QUERY_TABLE = f"`{PROJECT}.{DATASET}.v_query_daily`"

SEV_ORDER = {"critical": 0, "warning": 1, "info": 2}


def pct(cur: float, prev: float) -> float | None:
    """Percentage change, or None when there is no base to compare against."""
    if not prev:
        return None
    return round((cur - prev) / prev * 100, 1)


def short(url: str, site: str) -> str:
    """Page URL with the property prefix stripped, for readable alert text."""
    return url[len(site):] if url.startswith(site) else url


# --------------------------------------------------------------------------- #
# Detection
# --------------------------------------------------------------------------- #
def latest_date(bq) -> dt.date:
    row = list(bq.query(f"SELECT MAX(date) d FROM {PAGE_TABLE}").result())[0]
    return row["d"]


def property_alerts(bq, cfg, latest: dt.date, n: int) -> list[dict]:
    t = cfg["thresholds"]["property"]
    cur_start = latest - dt.timedelta(days=n - 1)
    prev_start = cur_start - dt.timedelta(days=n)

    sql = f"""
    SELECT site_url,
           SUM(IF(date >= '{cur_start}', clicks, 0)) c,
           SUM(IF(date <  '{cur_start}', clicks, 0)) c0,
           SUM(IF(date >= '{cur_start}', impressions, 0)) i,
           SUM(IF(date <  '{cur_start}', impressions, 0)) i0,
           SAFE_DIVIDE(SUM(IF(date >= '{cur_start}', position*impressions, 0)),
                       SUM(IF(date >= '{cur_start}', impressions, 0))) p,
           SAFE_DIVIDE(SUM(IF(date <  '{cur_start}', position*impressions, 0)),
                       SUM(IF(date <  '{cur_start}', impressions, 0))) p0
    FROM {PAGE_TABLE}
    WHERE site_url IS NOT NULL AND date BETWEEN '{prev_start}' AND '{latest}'
    GROUP BY site_url
    """
    out = []
    for r in bq.query(sql).result():
        site = r["site_url"]
        c, c0 = r["c"] or 0, r["c0"] or 0
        i, i0 = r["i"] or 0, r["i0"] or 0
        p, p0 = r["p"], r["p0"]

        d = pct(c, c0)
        if d is not None and d <= -t["clicks_drop_pct"] and (c0 - c) >= t["clicks_drop_min_abs"]:
            out.append({
                "severity": "critical" if d <= -40 else "warning",
                "scope": "property", "site_url": site, "entity": site,
                "metric": "clicks", "current": c, "previous": c0, "delta_pct": d,
                "message": f"Clicks {c0:,} -> {c:,} ({d:+.1f}%) over {n}d",
            })

        d = pct(i, i0)
        if d is not None and d <= -t["impressions_drop_pct"] and (i0 - i) >= t["impressions_drop_min_abs"]:
            out.append({
                "severity": "warning",
                "scope": "property", "site_url": site, "entity": site,
                "metric": "impressions", "current": i, "previous": i0, "delta_pct": d,
                "message": f"Impressions {i0:,} -> {i:,} ({d:+.1f}%) over {n}d",
            })

        if p and p0 and (p - p0) >= t["position_worsen_by"]:
            out.append({
                "severity": "warning",
                "scope": "property", "site_url": site, "entity": site,
                "metric": "position", "current": round(p, 2), "previous": round(p0, 2),
                "delta_pct": None,
                "message": f"Average position {p0:.1f} -> {p:.1f} (worse by {p - p0:.1f})",
            })
    return out


def decay_alerts(bq, cfg, latest: dt.date) -> list[dict]:
    """Slow bleeds, which a week-on-week comparison structurally cannot see.

    blogs.manageengine.com went from 7,684 clicks in July 2025 to 0 from May
    2026 -- nine months of steady decline. Every individual week was within a
    few percent of the one before it, so the 7d-vs-7d check was silent for the
    entire collapse. This compares the last 28 days against the 28 days ending
    a quarter earlier, which is long enough for a gradual slide to clear a
    threshold while still being recent enough to act on.

    It also catches the end state: a property that used to earn traffic and now
    earns none. A percentage drop is undefined once the base is zero, so
    "dormant" is its own check rather than a -100%.
    """
    t = cfg["thresholds"]["decay"]
    lag = t["compare_to_days_ago"]
    cur_start = latest - dt.timedelta(days=27)
    old_end = latest - dt.timedelta(days=lag)
    old_start = old_end - dt.timedelta(days=27)

    sql = f"""
    SELECT site_url,
           SUM(IF(date >= '{cur_start}', clicks, 0)) c,
           SUM(IF(date BETWEEN '{old_start}' AND '{old_end}', clicks, 0)) c0,
           SUM(IF(date >= '{cur_start}', impressions, 0)) i,
           SUM(IF(date BETWEEN '{old_start}' AND '{old_end}', impressions, 0)) i0
    FROM {PAGE_TABLE}
    WHERE date BETWEEN '{old_start}' AND '{latest}'
    GROUP BY site_url
    """
    out = []
    for r in bq.query(sql).result():
        site = r["site_url"]
        c, c0 = r["c"] or 0, r["c0"] or 0
        i, i0 = r["i"] or 0, r["i0"] or 0

        if c0 >= t["dormant_min_prior_clicks"] and c == 0:
            out.append({
                "severity": "critical", "scope": "property", "site_url": site,
                "entity": site, "metric": "dormant",
                "current": 0, "previous": c0, "delta_pct": -100.0,
                "message": (
                    f"No clicks at all in the last 28 days, against {c0:,} in "
                    f"the 28 days ending {old_end}. This property has stopped "
                    f"earning search traffic entirely."
                ),
            })
            continue

        d = pct(c, c0)
        if (
            d is not None
            and d <= -t["clicks_drop_pct"]
            and (c0 - c) >= t["clicks_drop_min_abs"]
        ):
            out.append({
                "severity": "warning", "scope": "property", "site_url": site,
                "entity": site, "metric": "decay",
                "current": c, "previous": c0, "delta_pct": d,
                "message": (
                    f"Sustained decline: {c0:,} -> {c:,} clicks ({d:+.1f}%) "
                    f"against the 28 days ending {old_end}. Week-on-week "
                    f"checks will not show this."
                ),
            })

        di = pct(i, i0)
        if (
            di is not None
            and di <= -t["impressions_drop_pct"]
            and (i0 - i) >= t["impressions_drop_min_abs"]
        ):
            out.append({
                "severity": "warning", "scope": "property", "site_url": site,
                "entity": site, "metric": "decay",
                "current": i, "previous": i0, "delta_pct": di,
                "message": (
                    f"Sustained impression decline: {i0:,} -> {i:,} ({di:+.1f}%) "
                    f"against the 28 days ending {old_end}."
                ),
            })
    return out


def entity_alerts(bq, cfg, latest: dt.date, n: int, grain: str) -> list[dict]:
    """Page-level or query-level drops, capped per property."""
    table, dim, t = (
        (PAGE_TABLE, "page", cfg["thresholds"]["page"])
        if grain == "page"
        else (QUERY_TABLE, "query", cfg["thresholds"]["query"])
    )
    cur_start = latest - dt.timedelta(days=n - 1)
    prev_start = cur_start - dt.timedelta(days=n)

    # Only entities with a real base in the prior period can "drop", so filter
    # on c0 in the aggregate rather than pulling every row back.
    sql = f"""
    SELECT * FROM (
      SELECT site_url, {dim} AS k,
             SUM(IF(date >= '{cur_start}', clicks, 0)) c,
             SUM(IF(date <  '{cur_start}', clicks, 0)) c0,
             SUM(IF(date >= '{cur_start}', impressions, 0)) i,
             SUM(IF(date <  '{cur_start}', impressions, 0)) i0,
             SAFE_DIVIDE(SUM(IF(date >= '{cur_start}', position*impressions, 0)),
                         SUM(IF(date >= '{cur_start}', impressions, 0))) p,
             SAFE_DIVIDE(SUM(IF(date <  '{cur_start}', position*impressions, 0)),
                         SUM(IF(date <  '{cur_start}', impressions, 0))) p0
      FROM {table}
      WHERE site_url IS NOT NULL AND {dim} IS NOT NULL
        AND date BETWEEN '{prev_start}' AND '{latest}'
      GROUP BY site_url, k
      HAVING (c0 >= {t["clicks_drop_min_abs"]}
              OR i0 >= {t["position_min_impressions"]})
    )
    """
    by_site: dict[str, list[dict]] = {}
    for r in bq.query(sql).result():
        site = r["site_url"]
        c, c0 = r["c"] or 0, r["c0"] or 0
        i0 = r["i0"] or 0
        p, p0 = r["p"], r["p0"]
        hits = []

        d = pct(c, c0)
        if d is not None and d <= -t["clicks_drop_pct"] and (c0 - c) >= t["clicks_drop_min_abs"]:
            hits.append((
                "critical" if c == 0 else "warning",
                "clicks", c, c0, d,
                f"Clicks {c0:,} -> {c:,} ({d:+.1f}%)"
                + (" -- now zero" if c == 0 else ""),
            ))

        if (
            p and p0
            and (p - p0) >= t["position_worsen_by"]
            and i0 >= t["position_min_impressions"]
        ):
            hits.append((
                "warning", "position", round(p, 2), round(p0, 2), None,
                f"Position {p0:.1f} -> {p:.1f} (worse by {p - p0:.1f})",
            ))

        for sev, metric, cur, prev, delta, msg in hits:
            by_site.setdefault(site, []).append({
                "severity": sev, "scope": grain, "site_url": site,
                "entity": r["k"], "metric": metric, "current": cur,
                "previous": prev, "delta_pct": delta, "message": msg,
                "lost_clicks": c0 - c if metric == "clicks" else 0,
            })

    # Cap per property, keeping the biggest absolute losses. Without this a
    # single site-wide slump produces hundreds of near-identical lines.
    out = []
    for site, items in by_site.items():
        items.sort(key=lambda a: (SEV_ORDER[a["severity"]], -a["lost_clicks"]))
        out.extend(items[: t["max_alerts"]])
    return out


def pipeline_alerts(bq, cfg, latest: dt.date) -> list[dict]:
    p = cfg["pipeline"]
    today = dt.date.today()
    out = []

    sql = f"""
    WITH per_day AS (
      SELECT site_url, date, COUNT(*) rows_
      FROM {PAGE_TABLE}
      WHERE date >= DATE_SUB('{latest}', INTERVAL 28 DAY)
      GROUP BY site_url, date
    ),
    med AS (
      SELECT site_url, APPROX_QUANTILES(rows_, 2)[OFFSET(1)] median_rows
      FROM per_day GROUP BY site_url
    )
    SELECT b.site_url, MAX(b.date) latest, MIN(b.date) earliest,
           COUNT(DISTINCT b.date) days_present,
           DATE_DIFF(MAX(b.date), MIN(b.date), DAY) + 1 days_spanned,
           MAX(b.loaded_at) last_loaded,
           ANY_VALUE(med.median_rows) median_rows
    FROM {PAGE_TABLE} b
    LEFT JOIN med ON med.site_url = b.site_url
    GROUP BY b.site_url
    """
    for r in bq.query(sql).result():
        site = r["site_url"]
        behind = (today - r["latest"]).days
        if behind > p["max_days_behind"]:
            out.append({
                "severity": "critical", "scope": "pipeline", "site_url": site,
                "entity": site, "metric": "freshness",
                "current": behind, "previous": p["max_days_behind"],
                "delta_pct": None,
                "message": (
                    f"No data since {r['latest']} -- {behind} days behind "
                    f"(threshold {p['max_days_behind']}). The daily load may "
                    f"have stopped."
                ),
            })

        # A missing day is only a pipeline fault if the property had traffic
        # to miss. blogs.manageengine.com has fallen to ~1 impression/day, so
        # Search Console genuinely returns nothing for 40 of its days -- those
        # were re-probed against the API and came back empty. Alerting on them
        # buries the real signal, which is the traffic collapse itself and is
        # raised separately by the decay check.
        missing = r["days_spanned"] - r["days_present"]
        if (
            p["alert_on_missing_days"]
            and missing > 0
            and (r["median_rows"] or 0) >= p["gap_min_median_rows"]
        ):
            out.append({
                "severity": "warning", "scope": "pipeline", "site_url": site,
                "entity": site, "metric": "gaps",
                "current": missing, "previous": 0, "delta_pct": None,
                "message": (
                    f"{missing} day(s) missing between {r['earliest']} and "
                    f"{r['latest']} -- re-run the export for that range."
                ),
            })

    # A day that loaded but loaded far too little is the failure mode that
    # freshness checks miss: the job ran, the API returned a partial, nothing
    # looked broken.
    sql = f"""
    WITH d AS (
      SELECT site_url, date, COUNT(*) rows_
      FROM {PAGE_TABLE}
      WHERE site_url IS NOT NULL
        AND date BETWEEN DATE_SUB('{latest}', INTERVAL 28 DAY) AND '{latest}'
      GROUP BY site_url, date
    )
    SELECT site_url,
           MAX(IF(date = '{latest}', rows_, NULL)) last_rows,
           APPROX_QUANTILES(rows_, 2)[OFFSET(1)] median_rows
    FROM d GROUP BY site_url
    """
    for r in bq.query(sql).result():
        last, med = r["last_rows"], r["median_rows"]
        if not last or not med:
            continue
        ratio = last / med * 100
        if ratio < p["min_rows_vs_median_pct"]:
            out.append({
                "severity": "warning", "scope": "pipeline",
                "site_url": r["site_url"], "entity": r["site_url"],
                "metric": "volume", "current": last, "previous": med,
                "delta_pct": round(ratio - 100, 1),
                "message": (
                    f"Only {last:,} rows on {latest} vs a 28-day median of "
                    f"{med:,} ({ratio:.0f}%) -- looks like a partial load."
                ),
            })
    return out


# --------------------------------------------------------------------------- #
# Email
# --------------------------------------------------------------------------- #
def render_email(alerts: list[dict], latest: dt.date, n: int, url: str) -> str:
    colour = {"critical": "#b42318", "warning": "#b54708", "info": "#175cd3"}
    groups: dict[str, list[dict]] = {}
    for a in alerts:
        groups.setdefault(a["scope"], []).append(a)

    order = ["pipeline", "property", "page", "query"]
    title = {
        "pipeline": "Pipeline health",
        "property": "Property level",
        "page": "Pages",
        "query": "Queries",
    }

    rows = []
    for scope in order:
        items = groups.get(scope)
        if not items:
            continue
        rows.append(
            f'<tr><td colspan="3" style="padding:18px 12px 6px;font:600 13px '
            f'system-ui,sans-serif;color:#344054;border-bottom:1px solid #eaecf0">'
            f'{title[scope]} <span style="color:#98a2b3;font-weight:400">'
            f'({len(items)})</span></td></tr>'
        )
        for a in items:
            ent = a["entity"]
            if scope in ("page", "query"):
                ent = short(str(ent), a["site_url"])
            ent = (ent[:90] + "…") if len(str(ent)) > 90 else ent
            prop = a["site_url"].replace("https://www.manageengine.com/", "").replace(
                "https://", ""
            ) or "root"
            rows.append(
                f'<tr>'
                f'<td style="padding:7px 12px;font:600 11px system-ui,sans-serif;'
                f'color:{colour[a["severity"]]};white-space:nowrap;vertical-align:top">'
                f'{a["severity"].upper()}</td>'
                f'<td style="padding:7px 12px;font:13px system-ui,sans-serif;'
                f'color:#101828;vertical-align:top">{ent}'
                f'<div style="color:#98a2b3;font-size:11px;margin-top:2px">{prop}</div></td>'
                f'<td style="padding:7px 12px;font:13px system-ui,sans-serif;'
                f'color:#475467;vertical-align:top">{a["message"]}</td>'
                f'</tr>'
            )

    if not rows:
        rows = [
            '<tr><td style="padding:24px 12px;font:14px system-ui,sans-serif;'
            'color:#475467">No anomalies. All properties loaded and within '
            'thresholds.</td></tr>'
        ]

    # The charset declaration is not optional: page URLs carry percent-encoded
    # and non-ASCII characters, and without it a client decodes the UTF-8 as
    # latin-1 and every truncated entity ends in mojibake.
    return f"""<html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;background:#f9fafb;padding:24px">
<div style="max-width:760px;margin:0 auto;background:#fff;border:1px solid #eaecf0;border-radius:10px;overflow:hidden">
  <div style="padding:20px 24px;border-bottom:1px solid #eaecf0">
    <div style="font:600 17px system-ui,sans-serif;color:#101828">Search Console monitor</div>
    <div style="font:13px system-ui,sans-serif;color:#667085;margin-top:4px">
      Last {n} days through <b>{latest}</b> vs the {n} days before.
      {len(alerts)} alert{"" if len(alerts) == 1 else "s"}.
    </div>
  </div>
  <table style="width:100%;border-collapse:collapse">{"".join(rows)}</table>
  <div style="padding:16px 24px;border-top:1px solid #eaecf0;font:12px system-ui,sans-serif;color:#667085">
    <a href="{url}" style="color:#175cd3">Open the dashboard</a> &nbsp;·&nbsp;
    Query-level figures exclude anonymised queries and under-count; page
    figures are exact. Property figures overlap — manageengine.com contains
    the product properties.
  </div>
</div></body></html>"""


def send_email(cfg: dict, subject: str, html: str) -> None:
    e = cfg["email"]
    password = os.environ.get("SMTP_PASSWORD")
    if not password:
        print("  SMTP_PASSWORD not set -- skipping email.")
        return

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = e["from"]
    msg["To"] = ", ".join(e["to"])
    msg["Date"] = formatdate(localtime=True)
    msg.set_content("This digest is HTML. See the dashboard for details.")
    msg.add_alternative(html, subtype="html")

    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL(e["smtp_host"], e["smtp_port"], context=ctx) as s:
        s.login(e["from"], password)
        s.send_message(msg)
    print(f"  emailed {len(e['to'])} recipient(s)")


# --------------------------------------------------------------------------- #
def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--out", default=os.path.join(HERE, "docs", "data"))
    p.add_argument("--config", default=os.path.join(HERE, "alerts.config.json"))
    p.add_argument("--token", default=None)
    p.add_argument("--no-email", action="store_true")
    p.add_argument(
        "--preview",
        metavar="PATH",
        help=(
            "Write the digest HTML to this file instead of sending it. Use it "
            "to check what the recipients would get before wiring up SMTP."
        ),
    )
    p.add_argument(
        "--url",
        default="https://vignesh-es-22614.github.io/gsc-monitor/",
        help="Dashboard link used in the email footer.",
    )
    args = p.parse_args()

    with open(args.config, encoding="utf-8") as fh:
        cfg = json.load(fh)

    bq = bigquery.Client(
        project=PROJECT,
        credentials=load_credentials(args.token or default_token_path()),
    )
    n = cfg["windows"]["compare_days"]
    latest = latest_date(bq)
    excluded = set(cfg.get("properties", {}).get("exclude", []))
    print(f"Detecting against {latest} ({n}d vs {n}d) ...")

    alerts: list[dict] = []
    alerts += pipeline_alerts(bq, cfg, latest)
    print(f"  pipeline: {len(alerts)}")
    n0 = len(alerts)
    alerts += property_alerts(bq, cfg, latest, n)
    print(f"  property: {len(alerts) - n0}")
    n0 = len(alerts)
    alerts += decay_alerts(bq, cfg, latest)
    print(f"  decay:    {len(alerts) - n0}")
    n0 = len(alerts)
    alerts += entity_alerts(bq, cfg, latest, n, "page")
    print(f"  page:     {len(alerts) - n0}")
    n0 = len(alerts)
    try:
        alerts += entity_alerts(bq, cfg, latest, n, "query")
        print(f"  query:    {len(alerts) - n0}")
    except Exception as exc:  # noqa: BLE001
        print(f"  query:    skipped ({exc})")

    alerts = [a for a in alerts if a["site_url"] not in excluded]
    alerts.sort(key=lambda a: (SEV_ORDER[a["severity"]], a["scope"], a["site_url"]))

    os.makedirs(args.out, exist_ok=True)
    blob = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "latest_date": latest.isoformat(),
        "compare_days": n,
        "thresholds": cfg["thresholds"],
        "counts": {
            s: sum(1 for a in alerts if a["severity"] == s)
            for s in ("critical", "warning", "info")
        },
        "alerts": alerts,
    }
    with open(os.path.join(args.out, "alerts.json"), "w", encoding="utf-8") as fh:
        json.dump(blob, fh, indent=1)

    # Fold into summary.json so the dashboard needs one fetch, not two.
    spath = os.path.join(args.out, "summary.json")
    if os.path.exists(spath):
        with open(spath, encoding="utf-8") as fh:
            summary = json.load(fh)
        summary["alerts"] = blob
        with open(spath, "w", encoding="utf-8") as fh:
            json.dump(summary, fh, indent=1)

    crit = blob["counts"]["critical"]
    warn = blob["counts"]["warning"]
    print(f"\n{len(alerts)} alerts ({crit} critical, {warn} warning)")
    for a in alerts[:20]:
        print(f"  [{a['severity']:8}] {a['scope']:8} {str(a['entity'])[:60]:60} {a['message']}")
    if len(alerts) > 20:
        print(f"  ... and {len(alerts) - 20} more")

    if args.preview:
        with open(args.preview, "w", encoding="utf-8") as fh:
            fh.write(render_email(alerts, latest, n, args.url))
        print(f"  digest preview written to {args.preview}")

    if args.no_email or not cfg["email"]["enabled"]:
        return
    if not alerts and not cfg["email"]["send_when_clean"]:
        print("  clean -- no email sent (send_when_clean is false).")
        return

    bits = []
    if crit:
        bits.append(f"{crit} critical")
    if warn:
        bits.append(f"{warn} warning")
    subject = (
        f"{cfg['email']['subject_prefix']} "
        + (", ".join(bits) if bits else "all clear")
        + f" — {latest}"
    )
    send_email(cfg, subject, render_email(alerts, latest, n, args.url))


if __name__ == "__main__":
    main()
