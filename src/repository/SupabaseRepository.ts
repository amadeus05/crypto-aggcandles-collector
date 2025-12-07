import { createClient, SupabaseClient } from '@supabase/supabase-js';
import { SmartCandleRow } from '../types';

export class SupabaseRepository {
  private client: SupabaseClient;
  private buffer: SmartCandleRow[] = [];
  private flushInterval: NodeJS.Timeout | null = null;
  private initialized = false;

  constructor(private url: string, private key: string, private batchSize = 50) {
    this.client = createClient(url, key);
  }

  /**
   * Инициализация — создаёт таблицу если не существует
   */
  async init(): Promise<void> {
    if (this.initialized) return;

    await this.ensureTable();
    // await this.truncateAll();
    this.flushInterval = setInterval(() => this.flush(), 1000);
    this.initialized = true;
    console.log('[SupabaseRepository] Initialized');
  }

  /**
   * Очищает все данные в таблице при старте
   */
  private async truncateAll(): Promise<void> {
    const { error, count } = await this.client
      .from('candles')
      .delete()
      .neq('id', 0); // Delete all rows (id is never 0 with BIGSERIAL)

    if (error) {
      console.error('[SupabaseRepository] Truncate error:', error.message);
    } else {
      console.log(`[SupabaseRepository] Cleared all existing data`);
    }
  }

  /**
   * Создаёт таблицу candles если её нет
   */
  private async ensureTable(): Promise<void> {
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

    const { error } = await this.client.rpc('exec_sql', { sql: createTableSQL });

    if (error) {
      // Если RPC не доступен, попробуем через raw SQL (требует service_role key)
      console.warn('[SupabaseRepository] RPC exec_sql failed, trying alternative...');

      // Проверяем существует ли таблица через select
      const { error: selectError } = await this.client
        .from('candles')
        .select('symbol')
        .limit(1);

      if (selectError?.code === '42P01') {
        // Таблица не существует - нужно создать вручную
        console.error('[SupabaseRepository] Table "candles" does not exist!');
        console.error('Please create it manually in Supabase Dashboard with this SQL:');
        console.error(createTableSQL);
        throw new Error('Table "candles" does not exist. See console for CREATE TABLE SQL.');
      } else if (selectError) {
        console.error('[SupabaseRepository] Error checking table:', selectError);
      } else {
        console.log('[SupabaseRepository] Table "candles" exists');
      }
    } else {
      console.log('[SupabaseRepository] Table "candles" ensured');
    }
  }

  enqueue(row: SmartCandleRow): void {
    if (!this.initialized) {
      console.warn('[SupabaseRepository] Not initialized, dropping row');
      return;
    }
    this.buffer.push(row);
    if (this.buffer.length >= this.batchSize) this.flush();
  }

  async flush(): Promise<void> {
    if (!this.buffer.length) return;
    const rows = [...this.buffer];
    this.buffer = [];

    try {
      const { error } = await this.client
        .from('candles')
        .upsert(rows, { onConflict: 'symbol,ts' });

      if (error) {
        console.error('[SupabaseRepository] Flush error:', error.message);
        this.buffer.push(...rows); // вернуть в буфер при ошибке
      }
    } catch (e) {
      console.error('[SupabaseRepository] Flush exception:', e);
      this.buffer.push(...rows);
    }
  }

  async deleteOld(ttlDays: number): Promise<void> {
    const cutoff = Date.now() - ttlDays * 24 * 60 * 60 * 1000;
    const { error } = await this.client.from('candles').delete().lt('ts', cutoff);
    if (error) {
      console.error('[SupabaseRepository] deleteOld error:', error.message);
    }
  }

  async shutdown(): Promise<void> {
    if (this.flushInterval) {
      clearInterval(this.flushInterval);
      this.flushInterval = null;
    }
    await this.flush();
    console.log('[SupabaseRepository] Shutdown complete');
  }
}
