-- Search Console reporting views.
--
-- Two grains, two very different histories. Read this before using either.
--
--   PAGE grain  (v_page_daily)   authoritative totals, matches the Search
--                                Console UI Pages report. All 14 properties,
--                                but only from 2025-04-29 -- that is as far
--                                back as the API window reached when the
--                                export was built, and it can never be
--                                extended backwards.
--
--   QUERY grain (v_query_daily)  query / country / device detail. Search
--                                Console drops anonymised queries from any
--                                query-dimensioned request, so these clicks
--                                sum to well below the truth (66% low on one
--                                probed day). Use for RELATIVE ranking only.
--                                v_query_daily_full reaches back to 2022 for
--                                ADAP but scans 15x the bytes -- read the
--                                plain view unless you need that history.
--
-- NEVER SUM ACROSS site_url. https://www.manageengine.com/ is a URL-prefix
-- property that contains twelve of the other thirteen, so every product-page
-- row is counted twice. Group by site_url, or filter to one property.

-- --------------------------------------------------------------------------
-- Property grain: the number Search Console's Performance overview shows, and
-- the one people quote.
--
-- It is NOT the sum of v_page_daily, and the difference is not an error. A
-- single result listing two of your URLs -- sitelinks, or two pages ranking
-- for one query -- is ONE impression at property grain and TWO at page grain.
-- Measured on ad-manager, 2026-08-17 to 2026-09-13:
--
--     property grain   16,405 clicks   1,227,982 impressions   pos 10.3
--     page grain       17,160 clicks   1,447,009 impressions   pos 12.2
--
-- Both reconcile exactly against the Search Console API. Use this view for a
-- headline figure and v_page_daily for anything per-page.
-- --------------------------------------------------------------------------
CREATE OR REPLACE VIEW `it-security-online-marketing.gsc_data.v_site_daily` AS
SELECT
  date,
  site_url,
  clicks,
  impressions,
  SAFE_DIVIDE(clicks, impressions) AS ctr,
  position,
  loaded_at
FROM `it-security-online-marketing.gsc_data.gsc_site_daily`
WHERE site_url IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY site_url, date
  ORDER BY loaded_at DESC NULLS LAST) = 1;

-- --------------------------------------------------------------------------
-- Page grain: the number you quote.
--
-- DEDUPLICATING BY DESIGN. More than one loader writes these tables -- both
-- gsc_export.py and gsc_sync.py append, and they overlap -- so the raw table
-- can hold the same (site_url, date, page) two or more times. It did: pitstop
-- carried 4,257,945 rows for 2,175,590 distinct keys, every day written twice.
-- Summing the raw table therefore double-counts and stops matching the Search
-- Console UI.
--
-- QUALIFY keeps the most recently loaded copy of each key, so any number of
-- concurrent writers is harmless to every reader. Read THIS VIEW, never
-- gsc_page_daily directly.
-- --------------------------------------------------------------------------
CREATE OR REPLACE VIEW `it-security-online-marketing.gsc_data.v_page_daily` AS
SELECT
  date,
  site_url,
  page,
  clicks,
  impressions,
  SAFE_DIVIDE(clicks, impressions) AS ctr,
  position,
  loaded_at
FROM `it-security-online-marketing.gsc_data.gsc_page_daily`
WHERE site_url IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY site_url, date, page
  ORDER BY loaded_at DESC NULLS LAST) = 1;

-- --------------------------------------------------------------------------
-- Page grain stretched back to 2022 -- ADAP only, and the two halves are NOT
-- comparable. `totals_reliable` is the guard: anything charting a trend across
-- 2025-04-29 must either filter on it or show the break.
--
-- The overlap ratio between the two halves is not a usable correction factor:
-- it trends (impressions 2.05x -> 3.15x, clicks 3.26x -> 4.02x across the 14
-- shared months), so scaling the old half up would manufacture a decline that
-- is really just the anonymised share growing.
-- --------------------------------------------------------------------------
CREATE OR REPLACE VIEW `it-security-online-marketing.gsc_data.v_page_daily_full` AS
SELECT
  date, site_url, page, clicks, impressions, position,
  'page_grain' AS source,
  TRUE AS totals_reliable
FROM `it-security-online-marketing.gsc_data.v_page_daily`

UNION ALL

SELECT
  `Date` AS date,
  'https://www.manageengine.com/products/active-directory-audit/' AS site_url,
  REPLACE(`Page`, 'http://', 'https://') AS page,
  CAST(SUM(Clicks) AS INT64) AS clicks,
  CAST(SUM(Impressions) AS INT64) AS impressions,
  -- Impression-weighted, which is how Search Console averages position.
  SAFE_DIVIDE(SUM(Position * Impressions), SUM(Impressions)) AS position,
  'api_export' AS source,
  FALSE AS totals_reliable
FROM `it-security-online-marketing.gsc_data.gsc_api_export`
WHERE `Date` < DATE '2025-04-29'
GROUP BY date, page;

-- --------------------------------------------------------------------------
-- Query grain: relative ranking only. Deduplicated on the full key, for the
-- same concurrent-writer reason as v_page_daily above.
--
-- THIS is what the pipeline reads. It deliberately does NOT union the legacy
-- gsc_api_export: that table is unpartitioned and 12.1 GB, so including it
-- makes BigQuery scan all of it on every read no matter how narrow the date
-- filter. Measured on a 14-day window: 2.56 GB through the unioned view
-- against 0.17 GB through this one, a 15x bill for history the window never
-- touches. Use v_query_daily_full when you actually want pre-2025 ADAP.
-- --------------------------------------------------------------------------
CREATE OR REPLACE VIEW `it-security-online-marketing.gsc_data.v_query_daily` AS
SELECT * EXCEPT(loaded_at) FROM (
  SELECT
    date,
    site_url,
    page,
    query,
    country,
    device,
    clicks,
    impressions,
    position,
    'query_grain' AS source,
    loaded_at
  FROM `it-security-online-marketing.gsc_data.gsc_query_daily`
  WHERE site_url IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY site_url, date, page, query, country, device
    ORDER BY loaded_at DESC NULLS LAST) = 1
);

-- --------------------------------------------------------------------------
-- Query grain back to 2022 -- ADAP only beyond 2025-04-29.
--
-- Unlike the page-grain pair, BOTH halves here are measured the same way:
-- each was pulled with the query dimension, so each is missing anonymised
-- queries. Query-level trends therefore DO legitimately span the join, which
-- page totals never can. One caveat survives: the 2022-2023 pull was clipped
-- at 50,000 rows/day and 32 days in 2023 sit at that ceiling, so the long tail
-- of those days is absent and history is understated there.
--
-- Expensive by construction (see the note above). Query it with an explicit
-- date range and expect to scan the legacy table.
-- --------------------------------------------------------------------------
CREATE OR REPLACE VIEW `it-security-online-marketing.gsc_data.v_query_daily_full` AS
SELECT * FROM `it-security-online-marketing.gsc_data.v_query_daily`

UNION ALL

SELECT
  `Date` AS date,
  'https://www.manageengine.com/products/active-directory-audit/' AS site_url,
  REPLACE(`Page`, 'http://', 'https://') AS page,
  `Search Query` AS query,
  `Country Code` AS country,
  LOWER(Device) AS device,
  CAST(Clicks AS INT64) AS clicks,
  CAST(Impressions AS INT64) AS impressions,
  Position AS position,
  'api_export' AS source
FROM `it-security-online-marketing.gsc_data.gsc_api_export`
WHERE `Date` < DATE '2025-04-29';

-- --------------------------------------------------------------------------
-- Load health, one row per property. Drives the pipeline-health alert.
-- --------------------------------------------------------------------------
CREATE OR REPLACE VIEW `it-security-online-marketing.gsc_data.v_load_health` AS
SELECT
  site_url,
  MAX(date) AS latest_date,
  DATE_DIFF(CURRENT_DATE(), MAX(date), DAY) AS days_behind_today,
  MAX(loaded_at) AS last_loaded_at,
  COUNT(DISTINCT date) AS days_present,
  DATE_DIFF(MAX(date), MIN(date), DAY) + 1 AS days_spanned,
  DATE_DIFF(MAX(date), MIN(date), DAY) + 1 - COUNT(DISTINCT date) AS missing_days
FROM `it-security-online-marketing.gsc_data.v_page_daily`
GROUP BY site_url;

-- --------------------------------------------------------------------------
-- How badly are the concurrent loaders overlapping? Nothing reads this in the
-- pipeline -- it is the diagnostic for deciding when a physical compaction is
-- worth running, since the views already hide the duplication from readers.
-- --------------------------------------------------------------------------
CREATE OR REPLACE VIEW `it-security-online-marketing.gsc_data.v_dupe_report` AS
SELECT
  'gsc_page_daily' AS table_name, site_url,
  COUNT(*) AS raw_rows,
  COUNT(DISTINCT FORMAT('%t|%t', date, page)) AS distinct_keys,
  COUNT(*) - COUNT(DISTINCT FORMAT('%t|%t', date, page)) AS duplicate_rows
FROM `it-security-online-marketing.gsc_data.gsc_page_daily`
WHERE site_url IS NOT NULL
GROUP BY site_url;
