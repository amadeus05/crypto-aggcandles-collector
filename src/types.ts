export interface SmartCandleRow {
  symbol: string;
  ts: number;
  o?: number;
  h?: number;
  l?: number;
  c?: number;
  v?: number;
  cvd?: number;
  delta?: number;
  oi?: number;
  funding?: number;
  liquidations?: {
    long?: number;
    short?: number;
    countLong?: number;
    countShort?: number;
    maxLong?: number;
    maxShort?: number;
  };
  last_price?: number;
}

export interface MarketData {
  providerId: string;
  marketType: 'futures';
  symbol: string;
  timestamp: number;
  price: number;
  isCandleClosed: boolean;
  ohlc?: {
    open: number;
    high: number;
    low: number;
    close: number;
    volume: number;
  };
  indicators: {
    cvd: number;
    candleDelta: number;
    openInterest: number;
    fundingRate: number;
    liquidationsLong: number;
    liquidationsShort: number;
    liqCountLong: number;
    liqCountShort: number;
    liqMaxLong: number;
    liqMaxShort: number;
  };
}
