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

    let client: PoolClient | null = null;
    try {
      client = await this.pool.connect();
      
      const values = rows.map(row => 
        `('${row.symbol}', ${row.ts}, ${row.o || 'NULL'}, ${row.h || 'NULL'}, ${row.l || 'NULL'}, 
          ${row.c || 'NULL'}, ${row.v || 'NULL'}, ${row.cvd || 'NULL'}, ${row.delta || 'NULL'}, 
          ${row.oi || 'NULL'}, ${row.funding || 'NULL'}, 
          ${row.liquidations ? `'${JSON.stringify(row.liquidations)}'` : 'NULL'}, 
          ${row.last_price || 'NULL'})`
      ).join(',');

      const query = `
        INSERT INTO candles (symbol, ts, o, h, l, c, v, cvd, delta, oi, funding, liquidations, last_price)
        VALUES ${values}
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

      await client.query(query);
      
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
