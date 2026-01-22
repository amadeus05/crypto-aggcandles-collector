import 'dotenv/config';

function num(envVal: string | undefined, fallback: number): number {
  const n = Number(envVal);
  return Number.isFinite(n) ? n : fallback;
}

function int(envVal: string | undefined, fallback: number): number {
  const n = parseInt(envVal ?? '', 10);
  return Number.isFinite(n) ? n : fallback;
}

function normalizeSymbolMode(raw: string | undefined): 'TOP' | 'ALL' {
  const v = (raw || 'TOP').toUpperCase();
  return v === 'ALL' ? 'ALL' : 'TOP';
}

export const CONFIG = {
  PORT: num(process.env.PORT, 3000),
  DB: {
    SUPABASE_URL: process.env.SUPABASE_URL!,
    SUPABASE_KEY: process.env.SUPABASE_KEY!,
    COCKROACH_CONN: process.env.COCKROACH_CONNECTION_STRING!,
    COCKROACH_CERT:
      process.env.COCKROACH_CERT_PATH ||
      `${process.env.HOME ?? process.env.USERPROFILE ?? ''}/.postgresql/root.crt`,
    TTL_DAYS: int(process.env.DATA_TTL_DAYS, 2),
    USE_COCKROACH: process.env.USE_COCKROACH === 'true',
  },
  EXCHANGE: {
    SYMBOL_MODE: normalizeSymbolMode(process.env.SYMBOL_MODE),
    TOP_LIMIT: Math.max(1, int(process.env.TOP_SYMBOLS_LIMIT, 50)),
    WHITELIST: (process.env.SYMBOL_WHITELIST || '').split(',').map(s => s.trim().toUpperCase()).filter(Boolean),
    BLACKLIST: (process.env.SYMBOL_BLACKLIST || '').split(',').map(s => s.trim().toUpperCase()).filter(Boolean),
    MAX_REQ_SEC: Math.max(1, int(process.env.MAX_REQ_SEC, 35)),
    USE_RATE_LIMITER: process.env.USE_RATE_LIMITER === 'true',
  },
  AGGREGATION: {
    INTERVAL: process.env.CANDLE_AGG_INTERVAL || '1m', // '1m' or '5m'
  }
};