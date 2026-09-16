# Search Console monitor

A static dashboard and daily alerting job over all fourteen ManageEngine Search
Console properties, sourced from BigQuery.

- **Dashboard** — GitHub Pages, served from `docs/`.
- **Refresh + alerts** — the `Daily Search Console refresh` Action, 04:00 UTC
  (09:30 IST): pulls Search Console into BigQuery, rebuilds the payload,
  detects anomalies, emails a digest via Zoho SMTP, commits the result.

```
gsc_export.py        Search Console  ->  BigQuery   (two grains, all properties)
api_source.py        Search Console  ->  payload    (fallback for properties BQ lacks)
build_payload.py     BigQuery        ->  docs/data/*.json
alerts.py            BigQuery        ->  docs/data/alerts.json + email
docs/index.html      the dashboard   (vanilla JS, no build step)
sql/01_views.sql     reporting views over the raw tables
alerts.config.json   thresholds, recipients, payload sizes
```

### Read the views, never the raw tables

**More than one loader writes `gsc_page_daily` and `gsc_query_daily`** —
`gsc_export.py` here and `gsc_sync.py` in the parent project, both appending.
They overlap, so the raw tables hold duplicate keys: pitstop once carried
4,257,945 rows for 2,175,590 distinct `(date, page)` keys, every day written
twice about 100 seconds apart.

`v_page_daily` and `v_query_daily` deduplicate with `QUALIFY ROW_NUMBER() ...
ORDER BY loaded_at DESC`, keeping the newest copy of each key. That makes any
number of concurrent writers harmless to every reader, which is why
`build_payload.py` and `alerts.py` both read the views. **Summing the raw
tables double-counts.** `v_dupe_report` shows how bad the overlap currently is.

Physical compaction (`CREATE OR REPLACE TABLE ... QUALIFY ... = 1`) is safe
only while no loader is running — a rebuild races any concurrent load and would
drop rows written during it. The views mean you never have to.

---

## The three things that will bite you

### 1. Never add properties together

`https://www.manageengine.com/` is a **URL-prefix property that contains twelve
of the other thirteen**. A row under `/products/service-desk/` is counted by
both that property and the service-desk property. Group by `site_url`, or filter
to one. The dashboard marks the root property `CONTAINS OTHERS` and excludes it
from the comparison chart for this reason.

### 2. Page figures are exact; query figures under-count

Search Console **drops anonymised queries entirely** from any request that
includes the `query` dimension — they are not returned in an "other" bucket.
One probed day on the root property: 10,774 clicks at page grain, 3,657 at query
grain. **66% missing.**

So the export writes two tables and they answer different questions:

| Table | Grain | Use for | Never use for |
|---|---|---|---|
| `gsc_page_daily` | date + site + page | totals, trends, anything quoted | — |
| `gsc_query_daily` | + query, country, device | ranking queries, movement, splits | totals |

Everything on the Pages tab comes from the first. Everything on the Queries and
Country/device tabs comes from the second and is labelled as understated.

### 3. History starts 2025-04-29 and cannot be extended

The Search Console API serves a **rolling ~16 months**. Every day that passes,
one more day falls off the back permanently. Page-grain history therefore
begins 2025-04-29 for every property and nothing earlier can ever be recovered.

The one exception is **ADAP**, which has `gsc_api_export` covering 2022-01-01 →
2026-06-30 — but that table was pulled at query grain, so its page totals run
1.3x–6.5x low, and its 2022–23 pull was additionally clipped at 50,000 rows/day
(32 days in 2023 sit at that ceiling). It is surfaced separately as
`legacy_monthly`, never spliced onto the current line.

Do not be tempted to scale the old half up to match. The overlap ratio *trends*
— impressions 2.05x → 3.15x and clicks 3.26x → 4.02x across the fourteen shared
months — so a correction factor would manufacture a decline that is really just
the anonymised share growing.

---

## Running it

```bash
pip install -r requirements.txt

# once: mint a token carrying webmasters.readonly + BigQuery
python ../gsc_auth.py --client-secret "...json"

python gsc_export.py --list-sites
python gsc_export.py --all-sites --start 2025-04-01 --grain both --workers 6  # backfill
python gsc_export.py --all-sites --grain both --workers 6                     # incremental

python build_payload.py --out docs/data --api-fallback
python alerts.py --out docs/data --no-email

python -m http.server -d docs 8080    # then open http://localhost:8080
```

`--workers` pulls that many properties in parallel. Search Console quota is
per property, so 6 is comfortable; the ceiling that bites first is the
per-account 1,200 queries/minute. Serially the backfill takes most of a day.

A backfill with an explicit `--start` loads **newest day first**. A dashboard
is judged on its last 28 days, and loading chronologically means those arrive
last — a property can be 80% backfilled and still show an empty Queries tab.
Pass `--oldest-first` to reverse that. Resumes (no `--start`) always run
chronologically: the resume point is `MAX(date)`, so a newest-first run that
was interrupted would leave `MAX` at the end and the next resume would skip
everything it had not reached.

The exporter is **idempotent**: it deletes the (property, date-range) it is
about to write before writing it, so re-running any range is safe. `--start`
forces a range; without it each property resumes from its own last loaded date.

`--api-fallback` on `build_payload.py` fills in any property BigQuery has no
rows for by querying the Search Console API directly (~17 requests per
property), so the dashboard is complete on the first run rather than after the
backfill finishes. Those properties are badged `LIVE API` and carry only the
rolling window — no stored history until the loader reaches them.

### Applying the views

```bash
bq query --use_legacy_sql=false < sql/01_views.sql
```

---

## Deploying

1. Create the repo and push. In **Settings → Pages**, serve from `main` / `docs`.
2. Add repository secrets:
   - `GSC_TOKEN_JSON` — the full contents of `gsc_token.json`. It carries a
     refresh token, so it works unattended. A service account would need adding
     as a user on all fourteen properties individually.
   - `SMTP_PASSWORD` — the Zoho app-specific password.
3. Add repository variable `DASHBOARD_URL` (the Pages URL) so the email links back.

Zoho SMTP (`smtp.zoho.in:465`) is the delivery path. Catalyst Mail refuses to
send from `zohocorp.com` without domain verification, which is why the spend
monitor ended up on the same route.

---

## Alerting

Each run compares the **last 7 complete days against the 7 immediately before**
— the same number of Saturdays on each side. A "vs last month" comparison mostly
measures how many weekends each period happened to contain.

| Scope | Fires on |
|---|---|
| Pipeline | a property more than 5 days stale; missing days inside its range; a day that loaded under 40% of its 28-day median rows |
| Property | clicks −20% (≥50 absolute), impressions −25% (≥2,000), average position +1.5 |
| **Decay** | last 28 days vs the 28 days ending a quarter ago: clicks −40% (≥200), impressions −45% (≥20,000), or zero clicks against ≥50 before |
| Page | clicks −35% (≥20 absolute), position +3.0 on a page with ≥500 impressions |
| Query | clicks −40% (≥15 absolute), position +3.0 with ≥300 impressions |

### Why the decay check exists

A week-on-week comparison **structurally cannot see a slow bleed**.
`blogs.manageengine.com` fell from 7,684 clicks in July 2025 to 0 from May 2026
— nine months — and no individual week ever breached a threshold. Backtested
against that collapse:

| As of | 7d vs 7d | Decay |
|---|---|---|
| 2025-12-15 | 3 alerts | −82.0% |
| 2026-01-15 | position drift only | −99.4% |
| 2026-02-15 | **silent** | −99.4% |
| 2026-03-15 | position drift only | −99.4% |

The two are complementary: weekly catches cliffs, decay catches bleeds. Keep
both.

A missing day is only reported as a pipeline fault when the property's 28-day
median row count clears `gap_min_median_rows`. blogs genuinely returns **zero
rows** for 40 of its days — re-probed against the API to confirm — because it
is down to roughly one impression a day. Alerting on those buries the real
problem, which decay raises instead.

Every check pairs a percentage with a **minimum absolute movement**. A page
going 3 clicks → 1 is a 67% drop and means nothing; the absolute floor keeps
those out of the digest. Page and query alerts are capped at 15 per property,
keeping the biggest absolute losses, so one site-wide slump cannot produce
hundreds of near-identical lines.

Edit `alerts.config.json` and commit. There is no server to save settings to.

The `min_rows_vs_median_pct` check exists for the failure mode freshness checks
miss: the job ran, the API returned a partial, `MAX(date)` looks current, and
nothing appears broken.

---

## BigQuery tables

| Table | Rows | Span | Note |
|---|---|---|---|
| `gsc_page_daily` | grows daily | 2025-04-29 → | **authoritative.** Partitioned on `date`, clustered on `site_url` |
| `gsc_query_daily` | grows daily | 2025-04-29 → | query/country/device detail, understated |
| `gsc_api_export` | 53.2M | 2022-01-01 → 2026-06-30 | ADAP only, under-counted, frozen |
| `gsc_page_daily_legacy_adap` | 797K | 2025-04-29 → 2026-09-08 | pre-migration snapshot, safe to drop once verified |
| `de_stm_search_console` | 537K | 2025-01 → 2026-05 | 5 German pages; those URLs also appear under the www root property |

`v_page_daily`, `v_page_daily_full`, `v_query_daily` and `v_load_health` in
`sql/01_views.sql` wrap these with the caveats encoded as columns
(`source`, `totals_reliable`).
