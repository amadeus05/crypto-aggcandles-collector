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

    // if (d.symbol === "ACEUSDT") {
    //   console.log(`[WRITE] ${d.symbol} oi = ${d.indicators.openInterest}`);
    // }

    const row: SmartCandleRow = {
      symbol: d.symbol,
      ts: this.minuteStart(d.timestamp),
      o: d.ohlc.open,
      h: d.ohlc.high,
      l: d.ohlc.low,
      c: d.ohlc.close,
      v: d.ohlc.volume,
      cvd: d.indicators.cvd,
      delta: d.indicators.candleDelta,
      oi: d.indicators.openInterest, // всегда последнее значение
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
