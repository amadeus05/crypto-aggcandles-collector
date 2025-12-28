import { BinanceMarketDataProvider } from '../provider/BinanceMarketDataProvider';
import { Repository, MarketData, SmartCandleRow } from '../types';

// ─────────────────────────────────────────────────────────────
// ENV: выбор таймфрейма агрегации (1m | 5m)
// ─────────────────────────────────────────────────────────────
const AGG_INTERVAL = process.env.CANDLE_AGG_INTERVAL || '1m';
const AGG_MINUTES = AGG_INTERVAL === '5m' ? 5 : 1;

export class CandleService {
  private startedAt = Date.now();

  // In-memory аккумулятор для 5m агрегации
  private aggBuffer = new Map<string, SmartCandleRow>();

  // 🔥 Отслеживание последнего bucket для каждого символа
  private lastBucketBySymbol = new Map<string, number>();

  constructor(private repo: Repository, private provider: BinanceMarketDataProvider) {
    this.provider.onPriceUpdate((d) => this.handleMarketData(d as MarketData));
    console.log(`[CandleService] Aggregation interval: ${AGG_INTERVAL} (${AGG_MINUTES} min)`);
  }

  // Универсальный bucket для любого интервала
  private candleStart(ts: number): number {
    const bucketMs = AGG_MINUTES * 60_000;
    return Math.floor(ts / bucketMs) * bucketMs;
  }

  // Агрегация 1m свечей в 5m (или passthrough для 1m)
  private aggregate(symbol: string, bucketTs: number, row: SmartCandleRow): void {
    if (AGG_MINUTES === 1) {
      // 1m — сразу пишем без агрегации
      this.repo.enqueue(row);
      return;
    }

    const key = `${symbol}:${bucketTs}`;
    const prev = this.aggBuffer.get(key);

    if (!prev) {
      // Первая свеча в 5m окне — сохраняем в буфер
      this.aggBuffer.set(key, { ...row });
      return;
    }

    // Агрегируем OHLCV (open остаётся от первой свечи)
    prev.h = Math.max(prev.h ?? 0, row.h ?? 0);
    prev.l = Math.min(prev.l ?? Infinity, row.l ?? Infinity);
    prev.c = row.c;
    prev.v = (prev.v ?? 0) + (row.v ?? 0);

    // CVD — всегда последнее значение (накопленный)
    prev.cvd = row.cvd;

    // Delta — сумма за период
    prev.delta = (prev.delta ?? 0) + (row.delta ?? 0);

    // OI — snapshot на закрытие (последнее значение)
    prev.oi = row.oi;

    // Funding — последнее значение
    prev.funding = row.funding;

    // Liquidations — аккумулируем
    if (prev.liquidations && row.liquidations) {
      prev.liquidations.long = (prev.liquidations.long ?? 0) + (row.liquidations.long ?? 0);
      prev.liquidations.short = (prev.liquidations.short ?? 0) + (row.liquidations.short ?? 0);
      prev.liquidations.countLong = (prev.liquidations.countLong ?? 0) + (row.liquidations.countLong ?? 0);
      prev.liquidations.countShort = (prev.liquidations.countShort ?? 0) + (row.liquidations.countShort ?? 0);
      prev.liquidations.maxLong = Math.max(prev.liquidations.maxLong ?? 0, row.liquidations.maxLong ?? 0);
      prev.liquidations.maxShort = Math.max(prev.liquidations.maxShort ?? 0, row.liquidations.maxShort ?? 0);
    }

    prev.last_price = row.last_price;
  }

  // 🔥 Flush предыдущего bucket при смене окна
  private flushPreviousBucket(symbol: string, prevBucketTs: number): void {
    const key = `${symbol}:${prevBucketTs}`;
    const finalRow = this.aggBuffer.get(key);
    if (finalRow) {
      this.repo.enqueue(finalRow);
      this.aggBuffer.delete(key);
    }
  }

  private handleMarketData(d: MarketData) {
    if (!d.isCandleClosed || !d.ohlc) return;

    const bucketTs = this.candleStart(d.timestamp);
    const symbol = d.symbol;

    // 🔥 Проверяем смену bucket (закрытие 5m свечи)
    if (AGG_MINUTES > 1) {
      const prevBucket = this.lastBucketBySymbol.get(symbol);

      // Если bucket изменился — flush предыдущего ПЕРЕД агрегацией нового
      if (prevBucket !== undefined && prevBucket !== bucketTs) {
        this.flushPreviousBucket(symbol, prevBucket);
      }

      this.lastBucketBySymbol.set(symbol, bucketTs);
    }

    const row: SmartCandleRow = {
      symbol,
      ts: bucketTs,
      o: d.ohlc.open,
      h: d.ohlc.high,
      l: d.ohlc.low,
      c: d.ohlc.close,
      v: d.ohlc.volume,
      cvd: d.indicators.cvd,
      delta: d.indicators.candleDelta,
      oi: d.indicators.openInterest,
      funding: d.indicators.fundingRate,
      liquidations: {
        long: d.indicators.liquidationsLong,
        short: d.indicators.liquidationsShort,
        countLong: d.indicators.liqCountLong,
        countShort: d.indicators.liqCountShort,
        maxLong: d.indicators.liqMaxLong,
        maxShort: d.indicators.liqMaxShort,
      },
      last_price: d.ohlc.close,
    };

    // Агрегируем (для 1m — сразу пишет, для 5m — накапливает)
    this.aggregate(symbol, bucketTs, row);
  }

  public getUptimeSecs() {
    return Math.floor((Date.now() - this.startedAt) / 1000);
  }
}
