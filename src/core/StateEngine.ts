import { SmartCandleRow } from '../domain/types';
import { AnyMarketEvent } from '../domain/events';

interface SymbolState {
  symbol: string;
  // Aggregators
  cvd: number;
  candleDelta: number;
  accLiqLong: number; accLiqShort: number;
  cntLiqLong: number; cntLiqShort: number;
  maxLiqLong: number; maxLiqShort: number;
  
  // Snapshots
  funding: number;
  lastPrice: number;
  
  // Time tracking
  currentCandleStart: number;
  
  // OI History buffer
  oiHistory: Array<{ ts: number, val: number }>;
  lastKnownOI: number;
}

export class StateEngine {
  private states = new Map<string, SymbolState>();

  constructor(private emitCandle: (row: SmartCandleRow) => void) {}

  public processEvent(e: AnyMarketEvent) {
    let state = this.states.get(e.symbol);
    if (!state) {
      state = this.createState(e.symbol);
      this.states.set(e.symbol, state);
    }

    switch (e.type) {
      case 'trade':
        this.handleTrade(state, e);
        break;
      case 'liquidation':
        this.handleLiquidation(state, e);
        break;
      case 'funding':
        state.funding = e.rate;
        break;
      case 'oi':
        state.lastKnownOI = e.openInterest;
        state.oiHistory.push({ ts: e.ts, val: e.openInterest });
        if (state.oiHistory.length > 20) state.oiHistory.shift();
        break;
      case 'kline':
        this.handleKline(state, e);
        break;
    }
  }

  private handleTrade(state: SymbolState, e: import('../domain/events').TradeEvent) {
    const usd = e.price * e.qty;
    // BuyerMaker = true -> Sell Agg -> Delta negative
    const delta = e.isBuyerMaker ? -usd : usd;
    
    // Check for candle time boundary (simple 1m alignment)
    const tradeCandleStart = Math.floor(e.ts / 60000) * 60000;
    
    if (tradeCandleStart > state.currentCandleStart) {
      // New minute started in trade stream (reset delta)
      state.currentCandleStart = tradeCandleStart;
      state.candleDelta = 0;
    }
    
    state.cvd += delta;
    state.candleDelta += delta;
    state.lastPrice = e.price;
  }

  private handleLiquidation(state: SymbolState, e: import('../domain/events').LiquidationEvent) {
    // Only count if relevant to current candle (ignore delayed packets)
    if (e.ts < state.currentCandleStart) return;

    const val = e.price * e.qty;
    if (e.side === 'LONG') {
      state.accLiqLong += val;
      state.cntLiqLong++;
      state.maxLiqLong = Math.max(state.maxLiqLong, val);
    } else {
      state.accLiqShort += val;
      state.cntLiqShort++;
      state.maxLiqShort = Math.max(state.maxLiqShort, val);
    }
  }

  private handleKline(state: SymbolState, e: import('../domain/events').KlineEvent) {
    // We only emit when the candle is closed
    if (!e.isClosed) {
      state.lastPrice = e.close; // Update price from kline too
      return; 
    }

    // Find best matching OI (timestamp <= closeTs)
    let bestOI = state.lastKnownOI;
    let bestTs = 0;
    for (const p of state.oiHistory) {
      if (p.ts <= e.closeTs && p.ts > bestTs) {
        bestOI = p.val;
        bestTs = p.ts;
      }
    }

    const row: SmartCandleRow = {
      symbol: state.symbol,
      ts: e.ts, // Kline start time
      o: e.open, h: e.high, l: e.low, c: e.close, v: e.volume,
      cvd: state.cvd,
      delta: state.candleDelta,
      oi: bestOI,
      funding: state.funding,
      liquidations: {
        long: state.accLiqLong, short: state.accLiqShort,
        countLong: state.cntLiqLong, countShort: state.cntLiqShort,
        maxLong: state.maxLiqLong, maxShort: state.maxLiqShort
      },
      last_price: e.close
    };

    this.emitCandle(row);

    // Reset accumulators for next candle
    state.accLiqLong = 0; state.accLiqShort = 0;
    state.cntLiqLong = 0; state.cntLiqShort = 0;
    state.maxLiqLong = 0; state.maxLiqShort = 0;
    // candleDelta resets naturally in handleTrade on next timestamp
  }

  private createState(symbol: string): SymbolState {
    return {
      symbol,
      cvd: 0, candleDelta: 0,
      accLiqLong: 0, accLiqShort: 0, cntLiqLong: 0, cntLiqShort: 0, maxLiqLong: 0, maxLiqShort: 0,
      funding: 0, lastPrice: 0,
      currentCandleStart: 0,
      oiHistory: [], lastKnownOI: 0
    };
  }
}