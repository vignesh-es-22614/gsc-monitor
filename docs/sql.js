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

    onStatus('Loading data index…');
    const r = await fetch('data/pq/manifest.json', { cache: 'no-cache' });
    if (!r.ok) throw new Error(`manifest.json ${r.status} — run export_parquet.py`);
    _manifest = await r.json();
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
    // Case-insensitive substring, the same semantics as the table filters.
    const each = list.map(t => `lower(${col}) LIKE ${lit('%' + t.toLowerCase() + '%')}`);
    const joined = each.join(f.join === 'all' ? ' AND ' : ' OR ');
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

/** Metric expressions, shared by every query so totals stay consistent. */
const METRICS = `
  SUM(clicks)::BIGINT AS clicks,
  SUM(impressions)::BIGINT AS impressions,
  SUM(clicks) / NULLIF(SUM(impressions), 0) AS ctr,
  SUM(position * impressions) / NULLIF(SUM(impressions), 0) AS position`;

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
  const src = source(grain, slug, f);
  if (!src) return null;
  const [row] = await run(`SELECT ${METRICS} FROM ${src} WHERE ${where(f, grain)}`);
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
  const src = source(grain, slug, f);
  if (!src) return { rows: [], grain, exact: grain === 'page' };
  const sel = list.join(', ');
  const rows = await run(`
    SELECT ${sel}, ${METRICS}
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
  const src = source(grain, slug, f);
  if (!src) return [];
  return run(`
    SELECT date::VARCHAR AS d, ${METRICS}
    FROM ${src} WHERE ${where(f, grain)}
    GROUP BY d ORDER BY d`);
}

/** Distinct values of a dimension, to populate the multi-selects. */
export async function distinct(slug, f, dim) {
  const src = source('query', slug, f);
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
  const src = source('query', slug, f);
  if (!src) return [];
  return run(`
    SELECT ${into} AS k, ${METRICS}
    FROM ${src}
    WHERE ${where(f, 'query')} AND ${dim} = ${lit(value)} AND ${into} IS NOT NULL
    GROUP BY k
    ORDER BY clicks DESC, impressions DESC
    LIMIT ${limit}`);
}

/** Escape hatch: run arbitrary SQL against the current property's files. */
export async function raw(sql) { return run(sql); }
