-- Page dimensions and lead outcomes, joined onto Search Console pages.
--
-- Everything here hangs off one idea: a NORMALISED PATH. Search Console
-- reports absolute URLs, the Page Themes workbook uses site-relative paths,
-- and the CRM stores its own cleaned paths. They only meet if all three are
-- reduced the same way: strip scheme and host, drop query and fragment,
-- lowercase, and pull a two-letter locale directory off the front so a theme
-- defined once applies to every locale of the same page.
--
-- Coverage is honest and uneven, because the workbook covers the security and
-- AD products and not the ITSM ones. Measured on 2026-08-17..09-15 clicks:
--
--     active-directory-audit 100%   log-management   70%
--     exchange-reports        91%   eventlog         54%
--     ad-manager              86%   data-security    55%
--     cloud-siem              81%   www root         26%
--     self-service-password   81%   service-desk / pitstop / endpoint-dlp  0%
--
-- The zeros are the workbook's scope, not a broken join. v_theme_coverage
-- reports it per property so the dashboard can say so rather than implying a
-- property has no themed traffic.

-- --------------------------------------------------------------------------
-- The shared normaliser, as a UDF so the three sources cannot drift apart.
-- --------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION `it-security-online-marketing.gsc_data.norm_path`(u STRING)
RETURNS STRING AS ((
  SELECT
    CASE
      WHEN REGEXP_CONTAINS(p, r'^/(au|br|ca|cn|de|es|fr|in|it|jp|kr|mx|nl|pl|pt|ru|sa|se|tr|tw|uk|us|vn|za|ae|ar|at|be|ch|cl|co|dk|fi|gr|hk|id|ie|il|my|no|nz|ph|pk|sg|th|ua|vi)(/|$)')
        THEN IFNULL(NULLIF(REGEXP_REPLACE(p, r'^/[a-z]{2}', ''), ''), '/')
      ELSE p
    END
  FROM (
    SELECT LOWER(REGEXP_REPLACE(
             REGEXP_REPLACE(IFNULL(u, ''), r'^https?://[^/]+', ''),
             r'[?#].*$', '')) AS p
  )
));

-- --------------------------------------------------------------------------
-- Every Search Console page with its theme, sub-theme and page type.
-- --------------------------------------------------------------------------
-- Materialised, not a view. The lead source is 261k rows across ~280 STRING
-- columns and is unpartitioned, so every read of it is a full scan: a single
-- property-month of the page-grain export cost 4.2 GB through the view, which
-- across 14 properties and 13 months would be ~700 GB per export. As tables
-- partitioned on date, the same export reads only the month it asks for.
-- Refresh them before the Parquet export; apply_sql.py on this file does it.
CREATE OR REPLACE VIEW `it-security-online-marketing.gsc_data.v_page_dim` AS
WITH pages AS (
  SELECT DISTINCT site_url, page,
         `it-security-online-marketing.gsc_data.norm_path`(page) AS page_path
  FROM `it-security-online-marketing.gsc_data.v_page_daily`
),
-- The workbook holds a row per (path, locale); collapse to one row per path so
-- a page themed only under /in/ still themes the default-locale URL.
themes AS (
  SELECT page_path,
         ANY_VALUE(theme)     AS theme,
         ANY_VALUE(sub_theme) AS sub_theme,
         ANY_VALUE(page_type) AS page_type
  FROM `it-security-online-marketing.gsc_data.page_themes`
  WHERE theme IS NOT NULL
  GROUP BY page_path
)
SELECT p.site_url, p.page, p.page_path,
       IFNULL(t.theme, '(unthemed)')      AS theme,
       IFNULL(t.sub_theme, '(unthemed)')  AS sub_theme,
       IFNULL(t.page_type, '(unknown)')   AS page_type,
       t.theme IS NOT NULL                AS is_themed
FROM pages p
LEFT JOIN themes t USING (page_path);

-- --------------------------------------------------------------------------
-- How much of each property the workbook actually covers, by clicks.
-- --------------------------------------------------------------------------
CREATE OR REPLACE VIEW `it-security-online-marketing.gsc_data.v_theme_coverage` AS
SELECT d.site_url,
       SUM(f.clicks) AS clicks,
       SUM(IF(d.is_themed, f.clicks, 0)) AS themed_clicks,
       SAFE_DIVIDE(SUM(IF(d.is_themed, f.clicks, 0)), SUM(f.clicks)) AS themed_share,
       COUNT(DISTINCT IF(d.is_themed, NULL, f.page)) AS unthemed_pages
FROM `it-security-online-marketing.gsc_data.v_page_daily` f
JOIN `it-security-online-marketing.gsc_data.v_page_dim` d
  ON d.site_url = f.site_url AND d.page = f.page
WHERE f.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY d.site_url;

-- --------------------------------------------------------------------------
-- Leads, conversions and revenue per landing page and day.
--
-- Two attributions, because they answer different questions and the CRM
-- stores both: FIRST source is the page that introduced the lead (what SEO
-- earned), LAST source is the page they converted from (what closed). A lead
-- normally has a different page in each, so the two never sum to the same
-- number and must not be added together.
--
-- Leads are counted DISTINCT on CRM_ID: the table has 261,179 rows for
-- 259,945 leads, so a plain COUNT(*) over-reports.
--
-- Every column in the source is STRING, including Revenue and the dates, so
-- everything is SAFE_CAST or SAFE.PARSE_DATETIME -- a bad row becomes NULL
-- rather than failing the whole build. Created_Date looks like
-- "31 Oct, 2025 23:50:22"; note that MAX() on it as a string returns the
-- alphabetically largest, not the latest.
-- --------------------------------------------------------------------------
CREATE OR REPLACE VIEW `it-security-online-marketing.gsc_data.v_page_leads_daily` AS
WITH base AS (
  SELECT
    CRM_ID,
    DATE(SAFE.PARSE_DATETIME('%d %b, %Y %H:%M:%S', Created_Date)) AS date,
    `it-security-online-marketing.gsc_data.norm_path`(FIRST_SRC_URL_CLEANED) AS first_path,
    `it-security-online-marketing.gsc_data.norm_path`(LAST_SRC_URL_CLEANED)  AS last_path,
    -- Channel of the lead. On a Search Console dashboard the organic subset is
    -- the comparable one; total leads beside organic clicks invites a false
    -- reading, so the channel travels with the row and the caller filters.
    IFNULL(NULLIF(NEW_TRAFFIC_SRC_GRP, ''), 'Unknown') AS channel,
    CONVERTED = '1' AS converted,
    IFNULL(SAFE_CAST(Revenue AS FLOAT64), 0) AS revenue
  FROM `it-security-online-marketing.sales_presales_leads_no_pi.lead_score_qt`
),
first_src AS (
  SELECT date, channel, 'first' AS attribution, first_path AS page_path,
         COUNT(DISTINCT CRM_ID) AS leads,
         COUNT(DISTINCT IF(converted, CRM_ID, NULL)) AS conversions,
         SUM(IF(converted, revenue, 0)) AS revenue
  FROM base
  WHERE date IS NOT NULL AND first_path IS NOT NULL AND first_path != ''
  GROUP BY date, channel, page_path
),
last_src AS (
  SELECT date, channel, 'last' AS attribution, last_path AS page_path,
         COUNT(DISTINCT CRM_ID) AS leads,
         COUNT(DISTINCT IF(converted, CRM_ID, NULL)) AS conversions,
         SUM(IF(converted, revenue, 0)) AS revenue
  FROM base
  WHERE date IS NOT NULL AND last_path IS NOT NULL AND last_path != ''
  GROUP BY date, channel, page_path
)
SELECT * FROM first_src
UNION ALL
SELECT * FROM last_src;

-- --------------------------------------------------------------------------
-- The materialised copies the export actually reads.
-- --------------------------------------------------------------------------
CREATE OR REPLACE TABLE `it-security-online-marketing.gsc_data.page_leads_daily`
PARTITION BY date
CLUSTER BY page_path, attribution AS
SELECT * FROM `it-security-online-marketing.gsc_data.v_page_leads_daily`
WHERE date IS NOT NULL;

CREATE OR REPLACE TABLE `it-security-online-marketing.gsc_data.page_dim`
CLUSTER BY site_url, page AS
SELECT * FROM `it-security-online-marketing.gsc_data.v_page_dim`;
