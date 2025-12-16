import { BinanceMarketDataProvider } from '../provider/BinanceMarketDataProvider';
import { Repository, MarketData, SmartCandleRow } from '../types';

export class CandleService {
  private startedAt = Date.now();

  constructor(private repo: Repository, private provider: BinanceMarketDataProvider) {
    this.provider.onPriceUpdate((d) => this.handleMarketData(d as MarketData));
  }

  private minuteStart(ts: number) { 
    return Math.floor(ts / 60000) * 60000; 
  }

  private handleMarketData(d: MarketData) {
    if (d.isCandleClosed && d.ohlc) {
      const ts = this.minuteStart(d.timestamp);

      // Берём актуальное состояние символа из провайдера
      const state = (this.provider as any).marketStates?.get(d.symbol);

      const row: SmartCandleRow = {
        symbol: d.symbol,
        ts,
        o: d.ohlc.open,
        h: d.ohlc.high,
        l: d.ohlc.low,
        c: d.ohlc.close,
        v: d.ohlc.volume,
        cvd: d.indicators.cvd,
        delta: d.indicators.candleDelta,
        // всегда пишем последнее известное значение OI
        oi: state?.openInterest ?? d.indicators.openInterest,
        funding: d.indicators.fundingRate,
        liquidations: {
          long: d.indicators.liquidationsLong,
          short: d.indicators.liquidationsShort,
          countLong: d.indicators.liqCountLong,
          countShort: d.indicators.liqCountShort,
          maxLong: d.indicators.liqMaxLong,
          maxShort: d.indicators.liqMaxShort,
        },
        last_price: d.price,
      };

      this.repo.enqueue(row);
    }
  }

  public getUptimeSecs() { 
    return Math.floor((Date.now() - this.startedAt) / 1000); 
  }
}
