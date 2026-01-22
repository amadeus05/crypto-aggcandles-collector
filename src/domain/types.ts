export interface SmartCandleRow {
  symbol: string;
  ts: number;
  
  // OHLC
  o: number; h: number; l: number; c: number;
  
  // Объемы
  v: number;       // Base Volume (Contracts/Coins) - совпадает с Binance kline.v
  quote_v: number; // Quote Volume (USDT) - совпадает с Binance kline.q
  
  // Дельта и CVD
  delta: number;   // Delta за эту свечу (USDT или Contracts, зависит от логики, здесь USDT)
  cvd: number;     // Cumulative Volume Delta (не сбрасывается)
  
  // Мета
  oi: number;
  funding: number;
  
  liquidations: {
    // TODO(алго/фичи): Сейчас храним только notional (price*qty) + count + max.
    // Для ботов обычно полезно дополнительно:
    // - liqLongQty / liqShortQty (base/contract qty), чтобы не терять информацию о размере в контрактах
    // - liqTotal / liqNet / liqImbalance (готовые derived-метрики)
    // - (опционально) сжатый профиль по цене (например, topN уровней или bucket-histogram),
    //   если планируются стратегии на "карте ликвидаций" внутри свечи.
    long: number; short: number;
    countLong: number; countShort: number;
    maxLong: number; maxShort: number;
  };
  
  last_price: number;
  
  // Флаги состояния
  isClosed: boolean;     // Soft-close (пришел kline closed)
  isFinalized: boolean;  // Hard-close (прошло время Grace, данные заморожены)
}

export interface MarketProvider {
  connect(symbols: string[]): Promise<void>;
  disconnect(): Promise<void>;
  onEvent(cb: (event: import('./events').AnyMarketEvent) => void): void;
}

export interface Repository {
  init(): Promise<void>;
  enqueue(row: SmartCandleRow): void;
  deleteOld(ttlDays: number): Promise<void>;
  shutdown(): Promise<void>;
}