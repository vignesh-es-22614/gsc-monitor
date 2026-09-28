// DuckDB-WASM query layer.
//
// The dashboard's precomputed windows answer fixed questions fast. This
// answers arbitrary ones: any date range, crossed with country, device, page
// and query, over the raw rows.
//
// It works on a static host because DuckDB reads Parquet over HTTP with range
// requests -- it pulls the footer, decides which row groups can possibly match
// from their statistics, and fetches only those byte ranges. Files are split
// one per (grain, property, month) and sorted on the filter columns, so a
// 28-day question opens one or two files and reads a fraction of each.
//
// Loaded lazily: nothing here is fetched until someone opens the Explore tab,
// so the normal dashboard never pays for the ~3 MB of wasm.

const DUCKDB_CDN = 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/+esm';

let _db = null, _conn = null, _manifest = null, _booting = null;

export function manifest() { return _manifest; }

export async function boot(onStatus = () => {}) {
  if (_conn) return _conn;
  if (_booting) return _booting;
  _booting = (async () => {
    onStatus('Loading query engine…');
    const duckdb = await import(/* @vite-ignore */ DUCKDB_CDN);
    const bundles = duckdb.getJsDelivrBundles();
    const bundle = await duckdb.selectBundle(bundles);

    // The worker script has to come from a same-origin URL, so wrap the CDN
    // URL in a blob that imports it.
    const workerUrl = URL.createObjectURL(
      new Blob([`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' })
    );
    const worker = new Worker(workerUrl);
    const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING);
    _db = new duckdb.AsyncDuckDB(logger, worker);
    await _db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    URL.revokeObjectURL(workerUrl);
    _conn = await _db.connect();

    // Every query reads Parquet over HTTP, so the round trips dominate, not
    // the scan. Caching the footer and metadata means a second query against
    // the same month does not re-fetch them, and keep-alive stops each range
    // request paying for a new TLS handshake. Wrapped because an option name
    // that a future DuckDB drops must not take the whole dashboard down.
    for (const pragma of [
      "SET enable_http_metadata_cache=true",
      "SET enable_object_cache=true",
      "SET http_keep_alive=true",
      "SET http_timeout=30000",
    ]) {
      try { await _conn.query(pragma); } catch (e) { /* option not supported */ }
    }

    onStatus('Loading data index…');
    const r = await fetch('data/pq/manifest.json', { cache: 'no-cache' });
    if (!r.ok) throw new Error(`manifest.json ${r.status} — run export_parquet.py`);
    _manifest = await r.json();
    onStatus('Ready');
    return _conn;
  })();
  return _booting;
}

/** Absolute URLs for the month files a (grain, property, range) touches. */
export function filesFor(grain, slug, from, to) {
  const p = _manifest?.properties?.[slug]?.[grain] || [];
  const lo = from.slice(0, 7), hi = to.slice(0, 7);
  const base = new URL('data/pq/', location.href).href;
  return p.filter(f => f.m >= lo && f.m <= hi)
          .map(f => `${base}${grain}/${slug}/${f.m}.parquet`);
}

export function bytesFor(grain, slug, from, to) {
  const p = _manifest?.properties?.[slug]?.[grain] || [];
  const lo = from.slice(0, 7), hi = to.slice(0, 7);
  return p.filter(f => f.m >= lo && f.m <= hi).reduce((a, f) => a + f.bytes, 0);
}

const lit = s => "'" + String(s).replace(/'/g, "''") + "'";

/**
 * Build the WHERE clause. `f` carries the filter bar's state:
 *   from, to          ISO dates, inclusive
 *   page, pageOp      term list + 'contains' | 'not'
 *   query, queryOp    same, query grain only
 *   join              'any' | 'all'
 *   countries[]       ISO-3 codes, empty = all
 *   devices[]         desktop | mobile | tablet, empty = all
 */
function where(f, grain) {
  const c = [`date BETWEEN DATE ${lit(f.from)} AND DATE ${lit(f.to)}`];

  const textClause = (col, terms, op) => {
    const list = terms.split(',').map(t => t.trim()).filter(Boolean);
    if (!list.length) return null;
    // All three modes are case-insensitive, matching the table filters.
    // `exact` is equality, not a substring: "exact" on a page filter means
    // that URL and nothing under it, and on a query means that query and not
    // the longer ones containing it.
    const each = op === 'exact'
      ? list.map(t => `lower(${col}) = ${lit(t.toLowerCase())}`)
      : list.map(t => `lower(${col}) LIKE ${lit('%' + t.toLowerCase() + '%')}`);
    // Several exact terms are alternatives, never a conjunction -- a column
    // cannot equal two different values, so ALL would always return nothing.
    const glue = (f.join === 'all' && op !== 'exact') ? ' AND ' : ' OR ';
    const joined = each.join(glue);
    return op === 'not' ? `NOT (${joined})` : `(${joined})`;
  };

  const pc = textClause('page', f.page || '', f.pageOp);
  if (pc && grain !== 'site') c.push(pc);
  if (grain === 'query') {
    const qc = textClause('query', f.query || '', f.queryOp);
    if (qc) c.push(qc);
    if (f.countries?.length) c.push(`country IN (${f.countries.map(lit).join(',')})`);
    if (f.devices?.length) c.push(`device IN (${f.devices.map(lit).join(',')})`);
  }
  return c.join(' AND ');
}

/**
 * Metric expressions, shared by every query so totals stay consistent.
 *
 * Leads, conversions and revenue exist only on the page grain, and in two
 * attributions: `first` is the page that introduced the lead, `last` the page
 * it converted from. They describe different pages and are never summed
 * together, so the caller picks one and gets that one.
 */
function metrics(grain, attribution = 'first', hasLeads = true) {
  const base = `
  SUM(clicks)::BIGINT AS clicks,
  SUM(impressions)::BIGINT AS impressions,
  SUM(clicks) / NULLIF(SUM(impressions), 0) AS ctr,
  SUM(position * impressions) / NULLIF(SUM(impressions), 0) AS position`;
  // Either the wrong grain, or page files written before the CRM columns
  // existed. Keep the shape identical so the table code needs no special case.
  if (grain !== 'page' || !hasLeads) {
    return base + `,
  NULL::BIGINT AS leads, NULL::BIGINT AS conversions, NULL::DOUBLE AS revenue`;
  }
  const s = attribution === 'last' ? 'last' : 'first';
  return base + `,
  SUM(leads_${s})::BIGINT AS leads,
  SUM(conv_${s})::BIGINT AS conversions,
  SUM(rev_${s})::DOUBLE AS revenue`;
}

/**
 * Which optional columns a set of Parquet files actually has.
 *
 * The published files are carried between CI runs in a cache, so a schema
 * change does not reach them until they are rebuilt: a page file written
 * before the theme and CRM columns existed is still perfectly valid, and
 * selecting leads_first from it fails the whole query. Probing costs one
 * footer read and is cached per file set.
 */
// Schema version at which the page grain gained the theme and CRM columns.
const SCHEMA_WITH_EXTRAS = 2;

const COLS_CACHE = new Map();
async function columnsOf(src, key) {
  if (COLS_CACHE.has(key)) return COLS_CACHE.get(key);
  let set = new Set();
  try {
    const rows = await run(`DESCRIBE SELECT * FROM ${src} LIMIT 0`);
    set = new Set(rows.map(r => r.column_name));
  } catch (e) {
    // If even DESCRIBE fails the real query will report it properly.
  }
  COLS_CACHE.set(key, set);
  return set;
}

/**
 * What one (grain, property, range) can answer.
 *
 * Read from the manifest's schema_version when it has one -- a DESCRIBE is a
 * whole extra network round trip per query, and on a static host over HTTP
 * that is most of the time a small query takes. Only data published before
 * the version stamp existed gets probed, and then once per file set.
 */
async function caps(grain, slug, f) {
  const src = source(grain, slug, f);
  if (!src) return { src: null, hasLeads: false, hasThemes: false };

  const v = _manifest?.schema_version;
  if (typeof v === 'number') {
    const ok = grain === 'page' && v >= SCHEMA_WITH_EXTRAS;
    return { src, hasLeads: ok, hasThemes: ok };
  }

  const cols = await columnsOf(src, `${grain}|${slug}|${f.from}|${f.to}`);
  return {
    src,
    hasLeads: cols.has('leads_first') && cols.has('rev_last'),
    hasThemes: cols.has('theme'),
  };
}

function source(grain, slug, f) {
  const files = filesFor(grain, slug, f.from, f.to);
  if (!files.length) return null;
  return `read_parquet([${files.map(lit).join(',')}])`;
}

/**
 * Which grain can answer this question.
 *
 * The page grain is exact -- it matches Search Console's Pages report -- but
 * it carries no query, country or device column, because the pull that
 * produced it deliberately omitted the query dimension to avoid losing
 * anonymised queries. The moment a question touches any of those three, only
 * the query grain can answer it, and that grain under-counts.
 *
 * So: use the exact source whenever the question allows, and tell the caller
 * which one it got so the UI can say whether the number is exact.
 */
export function grainFor(dims, f) {
  const needsQueryGrain =
    dims.some(d => d === 'query' || d === 'country' || d === 'device') ||
    (f.query || '').trim() !== '' ||
    (f.countries?.length || 0) > 0 ||
    (f.devices?.length || 0) > 0;
  return needsQueryGrain ? 'query' : 'page';
}

/** Dimensions that exist only on the page grain. */
export const PAGE_ONLY_DIMS = ['theme', 'sub_theme', 'page_type'];

/** Metrics that exist only on the page grain (the CRM join lives there). */
export const LEAD_METRICS = ['leads', 'conversions', 'revenue'];

/**
 * A question mixing a page-only dimension with a query-only one cannot be
 * answered from either file. Say so rather than returning a wrong number.
 */
export function unanswerable(dims, f) {
  const wantsPageOnly = dims.some(d => PAGE_ONLY_DIMS.includes(d));
  return wantsPageOnly && grainFor(dims, f) === 'query'
    ? 'Theme, sub-theme and page type are page-level facts, and country, '
      + 'device and query filters can only be answered from the query-grain '
      + 'file, which has no theme column. Clear the query, country and device '
      + 'filters to group by theme.'
    : null;
}

async function run(sql) {
  const res = await _conn.query(sql);
  return res.toArray().map(r => {
    const o = r.toJSON();
    // Arrow hands back BigInt for 64-bit ints, which JSON and Math choke on.
    for (const k in o) if (typeof o[k] === 'bigint') o[k] = Number(o[k]);
    return o;
  });
}

/** Top-level totals for the current filter, on the most exact grain available. */
export async function totals(slug, f, dims = []) {
  const grain = grainFor(dims, f);
  const { src, hasLeads } = await caps(grain, slug, f);
  if (!src) return null;
  const [row] = await run(
    `SELECT ${metrics(grain, f.attribution, hasLeads)} FROM ${src} WHERE ${where(f, grain)}`);
  if (row) { row.grain = grain; row.exact = grain === 'page'; }
  return row;
}

/**
 * Grouped rows. `dims` is one or more of page, query, country, device — more
 * than one gives a cross-tab, which is what the report builder needs.
 *
 * Returns {rows, grain, exact} so the caller can label the numbers honestly.
 */
export async function group(slug, f, dims, limit = null, minClicks = 0) {
  const list = Array.isArray(dims) ? dims : [dims];
  const grain = grainFor(list, f);
  const { src, hasLeads, hasThemes } = await caps(grain, slug, f);
  if (!src) return { rows: [], grain, exact: grain === 'page' };
  // Grouping by a theme column the published files predate would fail the
  // query; say so instead, the same way an impossible grain combination does.
  if (!hasThemes && list.some(d => PAGE_ONLY_DIMS.includes(d))) {
    throw new Error(
      'The published data does not carry theme columns yet. Re-run the '
      + 'workflow so the Parquet export rebuilds with them.');
  }
  const sel = list.join(', ');
  const rows = await run(`
    SELECT ${sel}, ${metrics(grain, f.attribution, hasLeads)}
    FROM ${src}
    WHERE ${where(f, grain)} AND ${list.map(d => `${d} IS NOT NULL`).join(' AND ')}
    GROUP BY ${sel}
    ${minClicks ? `HAVING SUM(clicks) >= ${Number(minClicks) || 0}` : ''}
    ORDER BY clicks DESC, impressions DESC
    ${limit ? `LIMIT ${limit}` : ''}`);
  // The table code reads a single `k`; a cross-tab joins its dimensions.
  for (const r of rows) r.k = list.map(d => r[d]).join(' · ');
  return { rows, grain, exact: grain === 'page' };
}

/** Daily series for charting, honouring every active filter. */
export async function daily(slug, f, dims = []) {
  const grain = grainFor(dims, f);
  const { src, hasLeads } = await caps(grain, slug, f);
  if (!src) return [];
  return run(`
    SELECT date::VARCHAR AS d, ${metrics(grain, f.attribution, hasLeads)}
    FROM ${src} WHERE ${where(f, grain)}
    GROUP BY d ORDER BY d`);
}

/** Distinct values of a dimension, to populate the multi-selects. */
export async function distinct(slug, f, dim) {
  const { src } = await caps('query', slug, f);
  if (!src) return [];
  return run(`
    SELECT ${dim} AS k, SUM(clicks)::BIGINT AS clicks
    FROM ${src}
    WHERE date BETWEEN DATE ${lit(f.from)} AND DATE ${lit(f.to)} AND ${dim} IS NOT NULL
    GROUP BY k ORDER BY clicks DESC`);
}

/**
 * Drill from one dimension into another — the top `limit` values of `into`
 * for a single value of `dim`, under the same filters as the parent query.
 *
 * Live rather than precomputed, so it respects the active date range, country
 * and device selection. The precomputed drilldowns on the Pages and Queries
 * tabs are always the last 28 days unfiltered; this one is whatever is on
 * screen.
 */
export async function drill(slug, f, dim, value, into, limit = 10) {
  const { src } = await caps('query', slug, f);
  if (!src) return [];
  return run(`
    SELECT ${into} AS k, ${metrics('query', f.attribution)}
    FROM ${src}
    WHERE ${where(f, 'query')} AND ${dim} = ${lit(value)} AND ${into} IS NOT NULL
    GROUP BY k
    ORDER BY clicks DESC, impressions DESC
    LIMIT ${limit}`);
}

/**
 * Everything one data tab needs, in a single round trip.
 *
 * The tab used to issue three queries -- totals, the grouped rows, the daily
 * series -- and each one re-opened the same Parquet files over HTTP. On a
 * static host the latency of those round trips is most of the wait, so they
 * are one statement now, with the grouped rows and the series separated by a
 * marker column. Totals come from summing the groups rather than a third
 * scan, which is exact because the grouping partitions the same rows.
 */
export async function tabData(slug, f, dim, limit = null, minClicks = 0) {
  const grain = grainFor([dim], f);
  const { src, hasLeads, hasThemes } = await caps(grain, slug, f);
  if (!src) {
    return { rows: [], series: [], tot: null, grain, exact: grain === 'page' };
  }
  if (!hasThemes && PAGE_ONLY_DIMS.includes(dim)) {
    throw new Error(
      'The published data does not carry theme columns yet. Re-run the '
      + 'workflow so the Parquet export rebuilds with them.');
  }

  const m = metrics(grain, f.attribution, hasLeads);
  const w = where(f, grain);
  const having = minClicks ? `HAVING SUM(clicks) >= ${Number(minClicks) || 0}` : '';

  const all = await run(`
    SELECT 'g' AS kind, ${dim}::VARCHAR AS k, ${m}
    FROM ${src} WHERE ${w} AND ${dim} IS NOT NULL
    GROUP BY k ${having}
    ORDER BY clicks DESC, impressions DESC
    ${limit ? `LIMIT ${limit}` : ''}
  UNION ALL
    SELECT 's' AS kind, date::VARCHAR AS k, ${m}
    FROM ${src} WHERE ${w}
    GROUP BY k`);

  const rows = [], series = [];
  const tot = { clicks: 0, impressions: 0, leads: 0, conversions: 0, revenue: 0,
                _posw: 0 };
  for (const r of all) {
    if (r.kind === 'g') {
      rows.push(r);
      tot.clicks += r.clicks || 0;
      tot.impressions += r.impressions || 0;
      tot.leads += r.leads || 0;
      tot.conversions += r.conversions || 0;
      tot.revenue += r.revenue || 0;
      // Position averages by impressions, so re-weight rather than mean it.
      tot._posw += (r.position || 0) * (r.impressions || 0);
    } else {
      series.push({ d: r.k, clicks: r.clicks, impressions: r.impressions });
    }
  }
  series.sort((a, b) => a.d < b.d ? -1 : 1);
  tot.ctr = tot.impressions ? tot.clicks / tot.impressions : null;
  tot.position = tot.impressions ? tot._posw / tot.impressions : null;
  delete tot._posw;
  if (!hasLeads) { tot.leads = null; tot.conversions = null; tot.revenue = null; }

  return { rows, series, tot, grain, exact: grain === 'page' };
}

/**
 * Paid and organic on the same search term.
 *
 * `bucket` picks which comparison is being asked for, and they are genuinely
 * different questions:
 *   both     ranking AND buying -- possibly paying for a click already earned
 *   paid     spend with no organic presence -- a content gap
 *   organic  ranking with no spend -- earned free, and what would be at risk
 */
export async function semseo(slug, f, bucket = 'both', limit = 500,
                             sort = 'sem_cost_inr') {
  // A term bought with no organic presence belongs to no Search Console
  // property, so those rows live under their own slug. Asking a product
  // property for its paid-only terms would otherwise always return nothing,
  // which reads as "no gaps" when it means "looked in the wrong place".
  const readSlug = bucket === 'paid' ? 'paid-only' : slug;
  const files = filesFor('semseo', readSlug, f.from, f.to);
  if (!files.length) return { rows: [], missing: true, bucket };
  const src = `read_parquet([${files.map(lit).join(',')}])`;

  const termClause = (() => {
    const list = (f.query || '').split(',').map(t => t.trim()).filter(Boolean);
    if (!list.length) return null;
    const each = f.queryOp === 'exact'
      ? list.map(t => `lower(term) = ${lit(t.toLowerCase())}`)
      : list.map(t => `lower(term) LIKE ${lit('%' + t.toLowerCase() + '%')}`);
    const glue = (f.join === 'all' && f.queryOp !== 'exact') ? ' AND ' : ' OR ';
    const j = each.join(glue);
    return f.queryOp === 'not' ? `NOT (${j})` : `(${j})`;
  })();

  const having = bucket === 'both'
      ? 'HAVING SUM(seo_impressions) > 0 AND SUM(sem_impressions) > 0'
    : bucket === 'paid'
      ? 'HAVING SUM(seo_impressions) = 0 AND SUM(sem_impressions) > 0'
    : bucket === 'organic'
      ? 'HAVING SUM(seo_impressions) > 0 AND SUM(sem_impressions) = 0'
      : '';

  const allowed = ['sem_cost_inr', 'sem_clicks', 'seo_clicks',
                   'seo_impressions', 'sem_impressions', 'sem_conversions'];
  const order = allowed.includes(sort) ? sort : 'sem_cost_inr';

  const rows = await run(`
    SELECT term,
      SUM(seo_clicks)::BIGINT       AS seo_clicks,
      SUM(seo_impressions)::BIGINT  AS seo_impressions,
      SUM(seo_position * seo_impressions) / NULLIF(SUM(seo_impressions),0) AS seo_position,
      SUM(sem_clicks)::BIGINT       AS sem_clicks,
      SUM(sem_impressions)::BIGINT  AS sem_impressions,
      SUM(sem_cost_inr)::DOUBLE     AS sem_cost_inr,
      SUM(sem_conversions)::DOUBLE  AS sem_conversions
    FROM ${src}
    WHERE date BETWEEN DATE ${lit(f.from)} AND DATE ${lit(f.to)}
      ${termClause ? 'AND ' + termClause : ''}
    GROUP BY term
    ${having}
    ORDER BY ${order} DESC
    LIMIT ${Number(limit) || 500}`);

  const tot = rows.reduce((a, r) => ({
    seo_clicks: a.seo_clicks + (r.seo_clicks || 0),
    sem_clicks: a.sem_clicks + (r.sem_clicks || 0),
    seo_impressions: a.seo_impressions + (r.seo_impressions || 0),
    sem_impressions: a.sem_impressions + (r.sem_impressions || 0),
    sem_cost_inr: a.sem_cost_inr + (r.sem_cost_inr || 0),
    sem_conversions: a.sem_conversions + (r.sem_conversions || 0),
  }), {seo_clicks:0, sem_clicks:0, seo_impressions:0, sem_impressions:0,
       sem_cost_inr:0, sem_conversions:0});

  return { rows, tot, bucket, missing: false };
}

/** Escape hatch: run arbitrary SQL against the current property's files. */
export async function raw(sql) { return run(sql); }

/**
 * Period bucketing for the trend view.
 *
 * DuckDB's date_trunc handles week/month/quarter/year; half-year has no
 * built-in, so it is expressed as the month floored to a 6-month boundary.
 * Weeks start Monday, which is what date_trunc('week') already does.
 */
function bucketExpr(period) {
  switch (period) {
    case 'weekly':      return `date_trunc('week', date)`;
    case 'monthly':     return `date_trunc('month', date)`;
    case 'quarterly':   return `date_trunc('quarter', date)`;
    case 'halfyearly':  return `make_date(year(date), CASE WHEN month(date) <= 6 THEN 1 ELSE 7 END, 1)`;
    case 'yearly':      return `date_trunc('year', date)`;
    default:            return `date_trunc('week', date)`;
  }
}

/**
 * One row per entity, one column per period -- the shape the table renders as
 * `page | clicks (w1, w2, ...) | impressions (w1, w2, ...)`.
 *
 * Returned long rather than pivoted: DuckDB would need the period list baked
 * into the SQL to pivot, and the caller has to group by entity anyway to lay
 * the columns out. Long keeps one query working for any number of periods.
 */
export async function trend(slug, f, dim, period, limit = 200) {
  const dims = [dim];
  const grain = grainFor(dims, f);
  const { src, hasLeads, hasThemes } = await caps(grain, slug, f);
  if (!src) return { rows: [], periods: [], grain, exact: grain === 'page' };
  if (!hasThemes && PAGE_ONLY_DIMS.includes(dim)) {
    throw new Error(
      'The published data does not carry theme columns yet. Re-run the '
      + 'workflow so the Parquet export rebuilds with them.');
  }
  const b = bucketExpr(period);

  // Rank entities on the whole range first, so the table shows the same top N
  // in every period instead of a different set per column.
  //
  // GROUP BY names the qualified column rather than the alias: the ranked CTE
  // also exposes a column called k, so GROUP BY k binds to the join side and
  // leaves the selected s.<dim> ungrouped. (Kept as a JS comment -- a SQL
  // comment here would sit inside the template literal, and a backtick in it
  // would end the string.)
  const rows = await run(`
    WITH ranked AS (
      SELECT ${dim} AS k, SUM(clicks) AS total
      FROM ${src} WHERE ${where(f, grain)} AND ${dim} IS NOT NULL
      GROUP BY k ORDER BY total DESC LIMIT ${Number(limit) || 200}
    )
    SELECT s.${dim} AS k, ${b}::VARCHAR AS period, ${metrics(grain, f.attribution, hasLeads)}
    FROM ${src} s
    JOIN ranked r ON r.k = s.${dim}
    WHERE ${where(f, grain)} AND s.${dim} IS NOT NULL
    GROUP BY s.${dim}, ${b}
    ORDER BY period`);

  // Newest first: the column next to the row label should be the most recent
  // period, because that is the one being read.
  const periods = [...new Set(rows.map(r => r.period))].sort().reverse();
  return { rows, periods, grain, exact: grain === 'page' };
}
