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

# What each alert usually means and what to do about it.
#
# Deliberately a lookup rather than free text per alert: the same shape of
# movement has the same short list of likely causes every time, and writing it
# once keeps the digest from turning into 180 lines of improvised prose. Each
# entry is (likely causes, what to check first).
#
# These are diagnoses to start from, not conclusions. The check column says
# what would confirm or kill each one -- the point is to make the next step
# obvious, not to pretend the alert knows why.
DIAGNOSIS = {
    ("pages", "clicks"): (
        "Position slipped (check the position tracker for the same page), a "
        "SERP feature took the click (AI overview, featured snippet, People "
        "Also Ask), the title or description changed, or seasonality.",
        "Compare impressions: if impressions held and clicks fell, it is a CTR "
        "problem -- ranking is intact and the listing is losing the click. If "
        "impressions fell too, it is a ranking or demand problem.",
    ),
    ("pages", "impressions"): (
        "Lost rankings for the queries that fed the page, a Google core update, "
        "deindexing, a noindex or canonical added by mistake, or the page was "
        "moved or redirected.",
        "Search Console URL Inspection on the page, then the Queries tab "
        "filtered to this page: if the queries themselves vanished it is "
        "indexing, if they merely dropped it is ranking.",
    ),
    ("queries", "clicks"): (
        "A competitor outranked you, the SERP layout changed for this query, "
        "or intent shifted so the page no longer matches.",
        "Search the query and look at what now occupies the top of the page. "
        "If your URL is still there, the click is going somewhere else on the "
        "SERP; if it is not, you lost the ranking.",
    ),
    ("queries", "impressions"): (
        "The query stopped matching your page, or total search demand for it "
        "fell. Demand moves are seasonal and affect competitors equally.",
        "Check whether neighbouring queries moved the same way. A single query "
        "falling alone is a ranking problem; a whole cluster falling together "
        "is usually demand.",
    ),
    ("position", "position"): (
        "A Google update, a competitor improving, content going stale, lost "
        "internal links, or the page cannibalising another of your own pages "
        "that targets the same intent.",
        "Whether one page slipped or the whole property did. Property-wide is "
        "an algorithm update; a single page is that page's own problem. Then "
        "check whether another of your pages now ranks for the same query.",
    ),
    ("missing_queries", "missing"): (
        "The page lost the ranking entirely, was deindexed, or the query no "
        "longer matches it at all. Zero impressions is stronger than a drop -- "
        "the page is not being shown for this query at all.",
        "Search the query. If a different page of yours appears, it is "
        "cannibalisation and the wrong page is now eligible. If none appears, "
        "check indexing on the page that used to rank.",
    ),
    ("themes", "clicks"): (
        "A whole content cluster losing ground at once, which is rarely one "
        "page's fault: a core update hitting this topic, a competitor "
        "publishing across it, or the cluster ageing together.",
        "The Pages tab filtered to this theme. If the loss is spread evenly "
        "the topic is losing authority; if one or two pages carry all of it, "
        "treat those as page-level problems instead.",
    ),
    ("themes", "impressions"): (
        "The theme's pages are matching fewer queries -- lost rankings across "
        "the cluster, or falling demand for the topic.",
        "The Queries tab filtered to this theme's pages: whether the queries "
        "themselves disappeared or merely moved down.",
    ),
    ("themes", "position"): (
        "The cluster is slipping as a group, which points at topical "
        "authority or a competitor's coverage rather than any single page.",
        "Whether other themes on the same property held steady. If they did, "
        "it is this topic; if none did, it is site-wide.",
    ),
    ("new_queries", "new"): (
        "New or updated content became eligible, a page started matching a new "
        "intent, or a competitor stopped ranking for it.",
        "Whether the page Google chose is the one you would have chosen. A "
        "query landing on the wrong page is an internal-linking or content fix "
        "and usually converts worse.",
    ),
    ("property", "clicks"): (
        "A site-wide change: a Google update, a migration, a robots.txt or "
        "sitemap change, a template edit affecting every page, or a CDN or "
        "availability incident during the period.",
        "Whether the drop is spread across pages or concentrated in a few. "
        "Spread evenly means site-wide; concentrated means a section.",
    ),
    ("property", "impressions"): (
        "Wide loss of indexed pages or ranking positions -- a core update or a "
        "crawling and indexing problem.",
        "Search Console Pages report for a rise in excluded or non-indexed "
        "URLs, and the Coverage trend for the same dates.",
    ),
    ("property", "dormant"): (
        "The property has stopped earning search traffic entirely. Normally a "
        "migration, a deindexing, or the site being retired without anyone "
        "telling the analytics.",
        "Whether the site still resolves and still returns indexable HTML, "
        "then whether the traffic reappeared on a different property.",
    ),
    ("property", "decay"): (
        "A slow bleed rather than a cliff: content ageing, competitors "
        "steadily improving, or accumulated small losses across many pages.",
        "The Pages tab over the same long window rather than the last week -- "
        "a decline this gradual never breaches a weekly threshold.",
    ),
    ("pipeline", "freshness"): (
        "The daily load did not run or failed: expired credentials, a changed "
        "Search Console permission, or the workflow erroring.",
        "The last Actions run, then the token's scopes. Nothing on this "
        "dashboard is trustworthy while this alert stands.",
    ),
    ("pipeline", "gaps"): (
        "Days missing inside the loaded range -- an interrupted backfill or an "
        "API error swallowed mid-run.",
        "Re-run the export for the affected range; it is idempotent and will "
        "overwrite rather than duplicate.",
    ),
    ("pipeline", "volume"): (
        "The job ran and wrote far less than usual, which a freshness check "
        "cannot see: a partial API response, or a real collapse in traffic.",
        "Whether other properties loaded normally on the same day. If they "
        "did, it is this property; if none did, it is the loader.",
    ),
}

GENERIC_DIAGNOSIS = (
    "No standard diagnosis for this combination.",
    "Compare the same window on the Pages and Queries tabs to see whether the "
    "movement is isolated or site-wide.",
)


def diagnose(alert: dict) -> dict:
    """Attach the likely cause and the first thing to check."""
    key = (alert.get("group", alert.get("scope")), alert.get("metric"))
    cause, check = DIAGNOSIS.get(key, GENERIC_DIAGNOSIS)
    alert["cause"] = cause
    alert["check"] = check
    return alert


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
                "scope": "property", "group": "property", "site_url": site, "entity": site,
                "metric": "clicks", "current": c, "previous": c0, "delta_pct": d,
                "message": f"Clicks {c0:,} -> {c:,} ({d:+.1f}%) over {n}d",
            })

        d = pct(i, i0)
        if d is not None and d <= -t["impressions_drop_pct"] and (i0 - i) >= t["impressions_drop_min_abs"]:
            out.append({
                "severity": "warning",
                "scope": "property", "group": "property", "site_url": site, "entity": site,
                "metric": "impressions", "current": i, "previous": i0, "delta_pct": d,
                "message": f"Impressions {i0:,} -> {i:,} ({d:+.1f}%) over {n}d",
            })

        if p and p0 and (p - p0) >= t["position_worsen_by"]:
            out.append({
                "severity": "warning",
                "scope": "property", "group": "position", "site_url": site,
                "entity": site,
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
                "severity": "critical", "scope": "property", "group": "property", "site_url": site,
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
                "severity": "warning", "scope": "property", "group": "property", "site_url": site,
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
                "severity": "warning", "scope": "property", "group": "property", "site_url": site,
                "entity": site, "metric": "decay",
                "current": i, "previous": i0, "delta_pct": di,
                "message": (
                    f"Sustained impression decline: {i0:,} -> {i:,} ({di:+.1f}%) "
                    f"against the 28 days ending {old_end}."
                ),
            })
    return out


# Digest section each grain's drops belong to. Spelled out rather than
# pluralised, because "query" + "s" is not "queries".
GRAIN_GROUP = {"page": "pages", "query": "queries"}


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

        i = r["i"] or 0

        d = pct(c, c0)
        if d is not None and d <= -t["clicks_drop_pct"] and (c0 - c) >= t["clicks_drop_min_abs"]:
            hits.append((
                "critical" if c == 0 else "warning",
                "clicks", c, c0, d,
                f"Clicks {c0:,} -> {c:,} ({d:+.1f}%)"
                + (" -- now zero" if c == 0 else ""),
                GRAIN_GROUP[grain],
            ))

        di = pct(i, i0)
        if (
            di is not None
            and di <= -t["impressions_drop_pct"]
            and (i0 - i) >= t["impressions_drop_min_abs"]
        ):
            hits.append((
                "warning", "impressions", i, i0, di,
                f"Impressions {i0:,} -> {i:,} ({di:+.1f}%)",
                GRAIN_GROUP[grain],
            ))

        # Position drops are grouped separately -- a page can hold its clicks
        # while sliding down the results, and that is the leading indicator.
        if (
            p and p0
            and (p - p0) >= t["position_worsen_by"]
            and i0 >= t["position_min_impressions"]
        ):
            hits.append((
                "warning", "position", round(p, 2), round(p0, 2), None,
                f"Position {p0:.1f} -> {p:.1f} (worse by {p - p0:.1f})",
                "position",
            ))

        for sev, metric, cur, prev, delta, msg, group in hits:
            by_site.setdefault(site, []).append({
                "severity": sev, "scope": grain, "group": group, "site_url": site,
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


def theme_alerts(bq, cfg, latest: dt.date, n: int) -> list[dict]:
    """Movements by content theme, from the Page Themes workbook.

    A theme aggregates many pages, so a theme-level drop means something
    systematic -- a whole content cluster losing ground -- rather than one
    page's own problem. That makes it worth its own section: the page-level
    alerts will be full of individual URLs from the same theme without ever
    saying they belong together.

    Only themed pages are considered. Coverage is uneven by design (the
    workbook covers the security and AD products, not ITSM), and alerting on
    an '(unthemed)' bucket that is 74% of the www root would say nothing.
    """
    t = cfg["thresholds"]["theme"]
    cur_start = latest - dt.timedelta(days=n - 1)
    prev_start = cur_start - dt.timedelta(days=n)

    sql = f"""
    SELECT f.site_url, d.theme,
           SUM(IF(f.date >= '{cur_start}', f.clicks, 0)) c,
           SUM(IF(f.date <  '{cur_start}', f.clicks, 0)) c0,
           SUM(IF(f.date >= '{cur_start}', f.impressions, 0)) i,
           SUM(IF(f.date <  '{cur_start}', f.impressions, 0)) i0,
           SAFE_DIVIDE(SUM(IF(f.date >= '{cur_start}', f.position*f.impressions, 0)),
                       SUM(IF(f.date >= '{cur_start}', f.impressions, 0))) p,
           SAFE_DIVIDE(SUM(IF(f.date <  '{cur_start}', f.position*f.impressions, 0)),
                       SUM(IF(f.date <  '{cur_start}', f.impressions, 0))) p0
    FROM {PAGE_TABLE} f
    JOIN `{PROJECT}.{DATASET}.page_dim` d
      ON d.site_url = f.site_url AND d.page = f.page
    WHERE f.date BETWEEN '{prev_start}' AND '{latest}' AND d.is_themed
    GROUP BY f.site_url, d.theme
    """
    by_site: dict[str, list[dict]] = {}
    for r in bq.query(sql).result():
        site, theme = r["site_url"], r["theme"]
        c, c0 = r["c"] or 0, r["c0"] or 0
        i, i0 = r["i"] or 0, r["i0"] or 0
        p, p0 = r["p"], r["p0"]
        hits = []

        d = pct(c, c0)
        if d is not None and d <= -t["clicks_drop_pct"] and (c0 - c) >= t["clicks_drop_min_abs"]:
            hits.append(("warning", "clicks", c, c0, d,
                         f"Clicks {c0:,} -> {c:,} ({d:+.1f}%) across this theme"))

        di = pct(i, i0)
        if di is not None and di <= -t["impressions_drop_pct"] \
                and (i0 - i) >= t["impressions_drop_min_abs"]:
            hits.append(("warning", "impressions", i, i0, di,
                         f"Impressions {i0:,} -> {i:,} ({di:+.1f}%) across this theme"))

        if p and p0 and (p - p0) >= t["position_worsen_by"] \
                and i0 >= t["position_min_impressions"]:
            hits.append(("warning", "position", round(p, 2), round(p0, 2), None,
                         f"Average position {p0:.1f} -> {p:.1f} across this theme"))

        for sev, metric, cur, prev, delta, msg in hits:
            by_site.setdefault(site, []).append({
                "severity": sev, "scope": "theme", "group": "themes",
                "site_url": site, "entity": theme, "metric": metric,
                "current": cur, "previous": prev, "delta_pct": delta,
                "message": msg, "lost_clicks": c0 - c if metric == "clicks" else 0,
            })

    out = []
    for site, items in by_site.items():
        items.sort(key=lambda x: -x["lost_clicks"])
        out.extend(items[: t["max_alerts"]])
    return out


def query_churn_alerts(bq, cfg, latest: dt.date, n: int) -> list[dict]:
    """Queries that stopped ranking, and queries that started.

    A drop check can only see a query that is still there. A query that earned
    clicks last week and returns *no rows at all* this week never appears in a
    comparison, because there is nothing to compare against -- it is the most
    complete kind of loss and the easiest to miss.

    The mirror case is worth surfacing for the opposite reason: a query that
    appeared from nothing is usually new content landing, or a competitor's
    term starting to match, and either is something to know about.

    Query grain, so anonymised queries are absent from both sides. That is
    fine here: the check is about presence, and a query too rare to be
    reported was never visible to begin with.
    """
    t = cfg["thresholds"]["churn"]
    n = t.get("window_days", n)
    cur_start = latest - dt.timedelta(days=n - 1)
    prev_start = cur_start - dt.timedelta(days=n)

    sql = f"""
    SELECT site_url, query,
           SUM(IF(date >= '{cur_start}', clicks, 0)) c,
           SUM(IF(date <  '{cur_start}', clicks, 0)) c0,
           SUM(IF(date >= '{cur_start}', impressions, 0)) i,
           SUM(IF(date <  '{cur_start}', impressions, 0)) i0
    FROM {QUERY_TABLE}
    WHERE site_url IS NOT NULL AND query IS NOT NULL
      AND date BETWEEN '{prev_start}' AND '{latest}'
    GROUP BY site_url, query
    HAVING (i = 0 AND c0 >= {t["missing_min_prior_clicks"]})
        OR (i0 = 0 AND c >= {t["new_min_clicks"]})
    """
    missing: dict[str, list] = {}
    fresh: dict[str, list] = {}
    for r in bq.query(sql).result():
        site = r["site_url"]
        if (r["i"] or 0) == 0:
            missing.setdefault(site, []).append({
                "severity": "critical" if (r["c0"] or 0) >= t["missing_critical_clicks"]
                            else "warning",
                "scope": "query", "group": "missing_queries", "site_url": site,
                "entity": r["query"], "metric": "missing",
                "current": 0, "previous": r["c0"] or 0, "delta_pct": -100.0,
                "lost_clicks": r["c0"] or 0,
                "message": (
                    f"Gone: {r['c0']:,} clicks and {r['i0']:,} impressions in the "
                    f"previous {n} days, no impressions at all now."
                ),
            })
        else:
            fresh.setdefault(site, []).append({
                "severity": "info",
                "scope": "query", "group": "new_queries", "site_url": site,
                "entity": r["query"], "metric": "new",
                "current": r["c"] or 0, "previous": 0, "delta_pct": None,
                "lost_clicks": 0,
                "message": (
                    f"New: {r['c']:,} clicks and {r['i']:,} impressions, with no "
                    f"impressions at all in the previous {n} days."
                ),
            })

    out = []
    for bucket in (missing, fresh):
        for site, items in bucket.items():
            items.sort(key=lambda a: -max(a["lost_clicks"], a["current"]))
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
                "severity": "critical", "scope": "pipeline", "group": "pipeline", "site_url": site,
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
                "severity": "warning", "scope": "pipeline", "group": "pipeline", "site_url": site,
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
                "severity": "warning", "scope": "pipeline", "group": "pipeline",
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
        groups.setdefault(a.get("group", a["scope"]), []).append(a)

    # Same order as the dashboard: a broken pipeline first, because it makes
    # everything under it meaningless; gains last.
    order = ["pipeline", "property", "pages", "queries",
             "missing_queries", "position", "new_queries"]
    title = {
        "pipeline": "Pipeline health",
        "property": "Property level",
        "pages": "Pages &mdash; clicks &amp; impressions",
        "queries": "Queries &mdash; clicks &amp; impressions",
        "missing_queries": "Missing queries &mdash; no impressions at all now",
        "position": "Position tracker &mdash; slipped down the results",
        "new_queries": "New queries &mdash; appeared from nothing",
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
            if a["scope"] in ("page", "query"):
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
                f'color:#475467;vertical-align:top">{a["message"]}'
                f'<div style="margin-top:5px;font-size:11.5px;color:#667085;line-height:1.45">'
                f'<b style="color:#475467">Likely:</b> {a.get("cause","")}<br>'
                f'<b style="color:#475467">Check:</b> {a.get("check","")}</div></td>'
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
        "--force-email",
        action="store_true",
        help="Send the digest even when nothing breached a threshold, "
             "overriding email.send_when_clean.",
    )
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
        alerts += theme_alerts(bq, cfg, latest, n)
        print(f"  theme:    {len(alerts) - n0}")
    except Exception as exc:  # noqa: BLE001
        print(f"  theme:    skipped ({str(exc).splitlines()[0][:90]})")
    n0 = len(alerts)
    try:
        alerts += entity_alerts(bq, cfg, latest, n, "query")
        print(f"  query:    {len(alerts) - n0}")
        n0 = len(alerts)
        alerts += query_churn_alerts(bq, cfg, latest, n)
        print(f"  churn:    {len(alerts) - n0}")
    except Exception as exc:  # noqa: BLE001
        print(f"  query:    skipped ({exc})")

    alerts = [a for a in alerts if a["site_url"] not in excluded]
    alerts = [diagnose(a) for a in alerts]
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
    if not alerts and not (cfg["email"]["send_when_clean"] or args.force_email):
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
