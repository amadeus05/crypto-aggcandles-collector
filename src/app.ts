import { CONFIG } from './config';
import { MarketUniverse } from './domain/MarketUniverse';
import { BinanceConnector } from './infra/BinanceConnector';
import { StateEngine } from './core/StateEngine';
import { CandleAggregator } from './core/Aggregator';
import { CockroachDBRepository } from './infra/repositories/CockroachDBRepository';
import { SupabaseRepository } from './infra/repositories/SupabaseRepository';
import express from 'express';

async function main() {
  // 1. Init Repository
  const repo = CONFIG.DB.USE_COCKROACH
    ? new CockroachDBRepository(CONFIG.DB.COCKROACH_CONN, CONFIG.DB.COCKROACH_CERT)
    : new SupabaseRepository(CONFIG.DB.SUPABASE_URL, CONFIG.DB.SUPABASE_KEY);
  await repo.init();

  // 2. Init Universe
  const universe = new MarketUniverse();
  const symbols = await universe.initialize();

  // 3. Build Pipeline: Source -> Engine -> Aggregator -> DB
  const aggregator = new CandleAggregator((row) => repo.enqueue(row));
  const engine = new StateEngine((row) => aggregator.push(row));
  const connector = new BinanceConnector();

  // 4. Wire Events
  connector.onEvent((event) => engine.processEvent(event));

  // 5. Start
  await connector.connect(symbols);

  // 6. Maintenance & API
  setInterval(() => repo.deleteOld(CONFIG.DB.TTL_DAYS), 60000);

  const app = express();
  app.get('/health', (req, res) => res.json({ status: 'ok', symbols: symbols.length }));
  app.listen(CONFIG.PORT, '0.0.0.0', () => console.log(`PumpScout v3 running on ${CONFIG.PORT}`));

  // Graceful Shutdown
  const shutdown = async () => {
    console.log('Stopping...');
    await connector.disconnect();
    await repo.shutdown();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main().catch(console.error);