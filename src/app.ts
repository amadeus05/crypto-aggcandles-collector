import 'dotenv/config';
import express from 'express';
import { BinanceMarketDataProvider } from './provider/BinanceMarketDataProvider';
import { SupabaseRepository } from './repository/SupabaseRepository';
import { CockroachDBRepository } from './repository/CockroachDBRepository';
import { CandleService } from './service/CandleService';

const PORT = process.env.SERVER_PORT || 8000;
const SUPABASE_URL = process.env.SUPABASE_URL!;
const SUPABASE_KEY = process.env.SUPABASE_KEY!;
const COCKROACH_CONNECTION_STRING = process.env.COCKROACH_CONNECTION_STRING!;
const COCKROACH_CERT_PATH = process.env.COCKROACH_CERT_PATH || `${process.env.HOME || process.env.USERPROFILE}/.postgresql/root.crt`;
const DATA_TTL_DAYS = parseInt(process.env.DATA_TTL_DAYS || '2');
const USE_COCKROACH = process.env.USE_COCKROACH === 'true';

async function main() {
  let repo;
  
  if (USE_COCKROACH) {
    console.log('Using CockroachDB repository');
    repo = new CockroachDBRepository(COCKROACH_CONNECTION_STRING, COCKROACH_CERT_PATH);
  } else {
    console.log('Using Supabase repository');
    repo = new SupabaseRepository(SUPABASE_URL, SUPABASE_KEY);
  }
  
  await repo.init();

  const provider = new BinanceMarketDataProvider('futures');
  const candleSvc = new CandleService(repo, provider);

  await provider.connect();

  // Periodic TTL cleanup
  setInterval(() => repo.deleteOld(DATA_TTL_DAYS), 60_000);

  const app = express();
  app.get('/health', (req, res) => {
    res.json({
      ok: true,
      uptime_seconds: candleSvc.getUptimeSecs(),
      lastUpdateTime: provider.getHealthStatus().lastUpdateTime,
    });
  });

  app.get('/uptime', (req, res) => {
    const totalSeconds = candleSvc.getUptimeSecs();
    const days = Math.floor(totalSeconds / 86400);
    const hours = Math.floor((totalSeconds % 86400) / 3600);
    const minutes = Math.floor((totalSeconds % 3600) / 60);
    const seconds = totalSeconds % 60;

    res.json({
      uptime: `${days}d ${hours}h ${minutes}m ${seconds}s`,
      uptime_seconds: totalSeconds,
      started_at: new Date(Date.now() - totalSeconds * 1000).toISOString(),
    });
  });

  const server = app.listen(PORT, () => console.log('Service listening on', PORT));

  const shutdown = async () => {
    console.log('Shutting down...');
    server.close();
    await provider.disconnect();
    await repo.shutdown();
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main().catch((e) => { console.error('Fatal', e); process.exit(1); });
