import { CONFIG } from './config';
import { MarketUniverse } from './domain/MarketUniverse';
import { BinanceConnector } from './infra/BinanceConnector';
import { StateEngine } from './core/StateEngine';
import { TimeframeAggregator } from './core/TimeframeAggregator';
import { CockroachDBRepository } from './infra/repositories/CockroachDBRepository';
import { SupabaseRepository } from './infra/repositories/SupabaseRepository';
import { StreamBroadcaster } from './infra/StreamBroadcaster';
import express from 'express';
import http from 'http';

async function main() {
  // 0. Setup Express & HTTP Server (Поднимаем сервер раньше)
  const app = express();
  const server = http.createServer(app);

  // Инициализируем стример, передавая ему HTTP сервер
  const streamer = new StreamBroadcaster(server);

  // 1. Init Repository
  const repo = CONFIG.DB.USE_COCKROACH
    ? new CockroachDBRepository(CONFIG.DB.COCKROACH_CONN, CONFIG.DB.COCKROACH_CERT)
    : new SupabaseRepository(CONFIG.DB.SUPABASE_URL, CONFIG.DB.SUPABASE_KEY);
  await repo.init();

  // 2. Init Universe
  const universe = new MarketUniverse();
  const symbols = await universe.initialize();

  // 3. Build Timeframe Aggregators (5m, 15m)
  const agg5m = new TimeframeAggregator(5, row => {
    repo.enqueue(row);
    streamer.broadcastTF(row, '5m');
  });
  const agg15m = new TimeframeAggregator(15, row => {
    repo.enqueue(row);
    streamer.broadcastTF(row, '15m');
  });

  // 4. Build Pipeline: Source -> Engine -> [DB + Stream + Aggregators]
  const engine = new StateEngine(
    // onStreamUpdate: вызывается ОЧЕНЬ часто (Live UI)
    (row) => {
      streamer.broadcastTF(row, '1m');
    },
    // onPersistUpdate: вызывается при закрытии или коррекции (DB)
    (row) => {
      repo.enqueue(row);     // 1m -> DB
      agg5m.push(row);       // 5m aggregation
      agg15m.push(row);      // 15m aggregation
    }
  );

  // 4. Wire Events
  const connector = new BinanceConnector();
  connector.onEvent((event) => engine.processEvent(event));

  await connector.connect(symbols);

  // 6. Maintenance & API
  setInterval(() => repo.deleteOld(CONFIG.DB.TTL_DAYS), 60000);

  app.get('/health', (req, res) => res.json({ status: 'ok', symbols: symbols.length }));

  // ВАЖНО: Заменяем app.listen на server.listen
  server.listen(CONFIG.PORT, () => console.log(`PumpScout v3 running on ${CONFIG.PORT} with Socket.IO`));

  // Graceful Shutdown
  const shutdown = async () => {
    console.log('Stopping...');
    await connector.disconnect();
    agg5m.shutdown();        // Flush pending 5m buckets
    agg15m.shutdown();       // Flush pending 15m buckets
    await repo.shutdown();
    streamer.shutdown();     // Закрываем сокеты
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main().catch(console.error);