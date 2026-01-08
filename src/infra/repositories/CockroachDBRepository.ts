import { Pool, PoolClient } from 'pg';
import { SmartCandleRow } from '../../domain/types';
import fs from 'fs';

export class CockroachDBRepository {
  private pool: Pool;
  private buffer: SmartCandleRow[] = [];
  private flushInterval: NodeJS.Timeout | null = null;
  private initialized = false;

  constructor(
    private connectionString: string,
    private certPath: string,
    private batchSize = 50
  ) {
    // Проверяем существование сертификата
    if (!fs.existsSync(certPath)) {
      throw new Error(`SSL certificate not found at: ${certPath}`);
    }

    this.pool = new Pool({
      connectionString,
      ssl: {
        rejectUnauthorized: true,
        ca: fs.readFileSync(certPath).toString(),
      },
    });
  }

  /**
   * Инициализация — создаёт таблицу если не существует
   */
  async init(): Promise<void> {
    if (this.initialized) return;

    await this.ensureTable();
    await this.truncateAll();
    this.flushInterval = setInterval(() => this.flush(), 1000);
    this.initialized = true;
    console.log('[CockroachDBRepository] Initialized');
  }

  /**
   * Очищает все данные в таблице при старте
   */
  private async truncateAll(): Promise<void> {
    let client: PoolClient | null = null;
    try {
      client = await this.pool.connect();
      await client.query('TRUNCATE TABLE candles');
      console.log('[CockroachDBRepository] Cleared all existing data');
    } catch (error) {
      console.error('[CockroachDBRepository] Truncate error:', error);
    } finally {
      if (client) client.release();
    }
  }

  /**
   * Создаёт таблицу candles если её нет
   */
  private async ensureTable(): Promise<void> {
    let client: PoolClient | null = null;
    try {
      client = await this.pool.connect();

      const createTableSQL = `
        CREATE TABLE IF NOT EXISTS candles (
          symbol TEXT NOT NULL,
          ts BIGINT NOT NULL,
          o DOUBLE PRECISION,
          h DOUBLE PRECISION,
          l DOUBLE PRECISION,
          c DOUBLE PRECISION,
          v DOUBLE PRECISION,
          cvd DOUBLE PRECISION,
          delta DOUBLE PRECISION,
          oi DOUBLE PRECISION,
          funding DOUBLE PRECISION,
          liquidations JSONB,
          last_price DOUBLE PRECISION,
          created_at TIMESTAMPTZ DEFAULT NOW(),
          PRIMARY KEY (symbol, ts)
        );

        CREATE INDEX IF NOT EXISTS idx_candles_ts ON candles (ts);
        CREATE INDEX IF NOT EXISTS idx_candles_symbol ON candles (symbol);
      `;

      await client.query(createTableSQL);
      console.log('[CockroachDBRepository] Table "candles" ensured');

    } catch (error) {
      console.error('[CockroachDBRepository] Error ensuring table:', error);
      throw error;
    } finally {
      if (client) client.release();
    }
  }

  enqueue(row: SmartCandleRow): void {
    if (!this.initialized) {
      console.warn('[CockroachDBRepository] Not initialized, dropping row');
      return;
    }
    this.buffer.push(row);
    if (this.buffer.length >= this.batchSize) this.flush();
  }

  async flush(): Promise<void> {
    if (!this.buffer.length) return;
    const rows = [...this.buffer];
    this.buffer = [];

    // Strip runtime-only fields that don't exist in the DB schema
    const dbRows = rows.map(({ isClosed, isFinalized, quote_v, ...rest }) => rest);

    // Deduplicate by (symbol, ts) - keep only the latest version of each candle
    const deduped = new Map<string, typeof dbRows[0]>();
    for (const row of dbRows) {
      deduped.set(`${row.symbol}:${row.ts}`, row);
    }
    const uniqueRows = Array.from(deduped.values());

    if (!uniqueRows.length) return;

    let client: PoolClient | null = null;
    try {
      client = await this.pool.connect();

      // Build parameterized query
      const columns = ['symbol', 'ts', 'o', 'h', 'l', 'c', 'v', 'cvd', 'delta', 'oi', 'funding', 'liquidations', 'last_price'];
      const paramCount = columns.length;

      // Generate placeholders: ($1, $2, ..., $13), ($14, $15, ..., $26), ...
      const valuePlaceholders = uniqueRows.map((_, rowIdx) => {
        const start = rowIdx * paramCount + 1;
        const placeholders = columns.map((_, colIdx) => `$${start + colIdx}`);
        return `(${placeholders.join(', ')})`;
      }).join(', ');

      // Flatten all values into a single array
      const params: (string | number | null | object)[] = [];
      for (const row of uniqueRows) {
        params.push(
          row.symbol,
          row.ts,
          row.o ?? null,
          row.h ?? null,
          row.l ?? null,
          row.c ?? null,
          row.v ?? null,
          row.cvd ?? null,
          row.delta ?? null,
          row.oi ?? null,
          row.funding ?? null,
          row.liquidations ? JSON.stringify(row.liquidations) : null,
          row.last_price ?? null
        );
      }

      const query = `
        INSERT INTO candles (${columns.join(', ')})
        VALUES ${valuePlaceholders}
        ON CONFLICT (symbol, ts) DO UPDATE SET
          o = EXCLUDED.o,
          h = EXCLUDED.h,
          l = EXCLUDED.l,
          c = EXCLUDED.c,
          v = EXCLUDED.v,
          cvd = EXCLUDED.cvd,
          delta = EXCLUDED.delta,
          oi = EXCLUDED.oi,
          funding = EXCLUDED.funding,
          liquidations = EXCLUDED.liquidations,
          last_price = EXCLUDED.last_price
      `;

      await client.query(query, params);

    } catch (error) {
      console.error('[CockroachDBRepository] Flush error:', error);
      this.buffer.push(...rows); // вернуть в буфер при ошибке
    } finally {
      if (client) client.release();
    }
  }

  async deleteOld(ttlDays: number): Promise<void> {
    let client: PoolClient | null = null;
    try {
      client = await this.pool.connect();
      const cutoff = Date.now() - ttlDays * 24 * 60 * 60 * 1000;
      await client.query('DELETE FROM candles WHERE ts < $1', [cutoff]);
    } catch (error) {
      console.error('[CockroachDBRepository] deleteOld error:', error);
    } finally {
      if (client) client.release();
    }
  }

  async shutdown(): Promise<void> {
    if (this.flushInterval) {
      clearInterval(this.flushInterval);
      this.flushInterval = null;
    }
    await this.flush();
    await this.pool.end();
    console.log('[CockroachDBRepository] Shutdown complete');
  }
}
