// Единые типы событий, с которыми работает ядро
export type MarketEventType = 'trade' | 'kline' | 'oi' | 'liquidation' | 'funding';

export interface BaseEvent {
  type: MarketEventType;
  symbol: string;
  ts: number; // Unified timestamp (ms)
}

export interface TradeEvent extends BaseEvent {
  type: 'trade';
  price: number;
  qty: number;
  isBuyerMaker: boolean; // true = sell agg, false = buy agg
}

export interface KlineEvent extends BaseEvent {
  type: 'kline';
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  isClosed: boolean;
  closeTs: number; // Время закрытия свечи
}

export interface OIEvent extends BaseEvent {
  type: 'oi';
  openInterest: number;
}

export interface LiquidationEvent extends BaseEvent {
  type: 'liquidation';
  price: number;
  qty: number;
  side: 'LONG' | 'SHORT'; // Кого ликвидировало
}

export interface FundingEvent extends BaseEvent {
  type: 'funding';
  rate: number;
}

export type AnyMarketEvent = TradeEvent | KlineEvent | OIEvent | LiquidationEvent | FundingEvent;