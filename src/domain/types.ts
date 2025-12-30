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