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

async function run(sql) {
  const res = await _conn.query(sql);
  return res.toArray().map(r => {
    const o = r.toJSON();
    // Arrow hands back BigInt for 64-bit ints, which JSON and Math choke on.
    for (const k in o) if (typeof o[k] === 'bigint') o[k] = Number(o[k]);
    return o;
  });
}

/** Top-level totals for the current filter. */
export async function totals(slug, f, grain = 'page') {
  const src = source(grain, slug, f);
  if (!src) return null;
  const [row] = await run(`SELECT ${METRICS} FROM ${src} WHERE ${where(f, grain)}`);
  return row;
}

/** Grouped rows — dim is 'page', 'query', 'country' or 'device'. */
export async function group(slug, f, dim, limit = null) {
  const grain = dim === 'page' ? 'page' : 'query';
  const src = source(grain, slug, f);
  if (!src) return [];
  return run(`
    SELECT ${dim} AS k, ${METRICS}
    FROM ${src}
    WHERE ${where(f, grain)} AND ${dim} IS NOT NULL
    GROUP BY k
    ORDER BY clicks DESC, impressions DESC
    ${limit ? `LIMIT ${limit}` : ''}`);
}

/** Daily series for charting, honouring every active filter. */
export async function daily(slug, f, grain = 'page') {
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

/** Escape hatch: run arbitrary SQL against the current property's files. */
export async function raw(sql) { return run(sql); }
