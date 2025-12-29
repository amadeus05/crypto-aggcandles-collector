export interface SmartCandleRow {
  symbol: string;
  ts: number;
  o: number; h: number; l: number; c: number; v: number;
  cvd: number;
  delta: number;
  oi: number;
  funding: number;
  liquidations: {
    long: number; short: number;
    countLong: number; countShort: number;
    maxLong: number; maxShort: number;
  };
  last_price: number;
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