import WebSocket from 'ws';
import axios, { AxiosInstance } from 'axios';
import * as https from 'https';
import PQueue from 'p-queue';
import { MarketProvider } from '../domain/types';
import { AnyMarketEvent, KlineEvent, TradeEvent, LiquidationEvent, FundingEvent, OIEvent } from '../domain/events';
import { CONFIG } from '../config';

const STREAM_BASE = 'wss://fstream.binance.com/stream';
const OI_API = 'https://fapi.binance.com/fapi/v1/openInterest';

export class BinanceConnector implements MarketProvider {
  private wsList: WebSocket[] = [];
  private tickerWs: WebSocket | null = null;
  private axios: AxiosInstance;
  private eventCallback: ((e: AnyMarketEvent) => void) | null = null;
  
  private activeSymbols = new Set<string>();
  private isRunning = false;
  
  // Logic for OI Priority
  private priorityMap = new Map<string, { priority: number; lastUpdated: number }>();
  private oiQueue: PQueue | null = null;

  constructor() {
    this.axios = axios.create({
      httpsAgent: new https.Agent({ keepAlive: true, maxSockets: 50 }),
      timeout: 10000
    });

    if (CONFIG.EXCHANGE.USE_RATE_LIMITER) {
      this.oiQueue = new PQueue({ interval: 1000, intervalCap: CONFIG.EXCHANGE.MAX_REQ_SEC });
    }
  }

  public onEvent(cb: (event: AnyMarketEvent) => void): void {
    this.eventCallback = cb;
  }

  public async connect(symbols: string[]): Promise<void> {
    this.activeSymbols = new Set(symbols);
    this.isRunning = true;

    // Init priorities
    symbols.forEach(s => this.priorityMap.set(s, { priority: 5, lastUpdated: 0 }));

    this.startWebSockets(symbols);
    this.startTickerStream(); // For dynamic priority adjustment
    this.startOIPolling();
    
    console.log(`[BinanceConnector] Connected for ${symbols.length} symbols`);
  }

  public async disconnect(): Promise<void> {
    this.isRunning = false;
    this.wsList.forEach(ws => ws.terminate());
    this.tickerWs?.terminate();
    console.log('[BinanceConnector] Disconnected');
  }

  private startWebSockets(symbols: string[]) {
    const BATCH = 30;
    for (let i = 0; i < symbols.length; i += BATCH) {
      const batch = symbols.slice(i, i + BATCH);
      const streams = batch.map(s => {
        const sym = s.toLowerCase();
        return `${sym}@kline_1m/${sym}@markPrice/${sym}@forceOrder/${sym}@aggTrade`;
      }).join('/');
      
      const ws = new WebSocket(`${STREAM_BASE}?streams=${streams}`);
      ws.on('message', (data) => this.parseMessage(data.toString()));
      ws.on('close', () => {
        if (this.isRunning) setTimeout(() => this.startWebSockets(batch), 3000); // Simple reconnect
      });
      this.wsList.push(ws);
    }
  }

  private parseMessage(raw: string) {
    if (!this.eventCallback) return;
    try {
      const msg = JSON.parse(raw);
      if (!msg.data) return;
      const d = msg.data;
      const stream = msg.stream;

      if (stream.includes('aggTrade')) {
        const e: TradeEvent = {
          type: 'trade',
          symbol: d.s,
          ts: d.T, // Trade time
          price: parseFloat(d.p),
          qty: parseFloat(d.q),
          isBuyerMaker: d.m
        };
        this.eventCallback(e);
      } 
      else if (stream.includes('kline')) {
        const k = d.k;
        const e: KlineEvent = {
          type: 'kline',
          symbol: k.s,
          ts: k.t,
          open: parseFloat(k.o),
          high: parseFloat(k.h),
          low: parseFloat(k.l),
          close: parseFloat(k.c),
          volume: parseFloat(k.v),
          isClosed: k.x,
          closeTs: k.T
        };
        this.eventCallback(e);
      }
      else if (stream.includes('forceOrder')) {
        const o = d.o;
        const e: LiquidationEvent = {
          type: 'liquidation',
          symbol: o.s,
          ts: o.T,
          price: parseFloat(o.p),
          qty: parseFloat(o.q),
          side: o.S === 'SELL' ? 'LONG' : 'SHORT'
        };
        this.eventCallback(e);
      }
      else if (stream.includes('markPrice')) {
        const e: FundingEvent = {
          type: 'funding',
          symbol: d.s,
          ts: d.E,
          rate: parseFloat(d.r)
        };
        this.eventCallback(e);
      }
    } catch (err) {
      // suppress parse errors
    }
  }

  // --- OI POLLING (Transport Layer Logic) ---
  // We keep this here because it involves HTTP requests and Rate Limiting
  private async startOIPolling() {
    while (this.isRunning) {
      const candidates = this.getOICandidates();
      
      if (candidates.length > 0) {
        const task = (sym: string) => this.fetchOI(sym);
        
        if (this.oiQueue) {
          await Promise.all(candidates.map(s => this.oiQueue!.add(() => task(s))));
        } else {
          await Promise.all(candidates.map(task));
        }
      }
      
      await new Promise(r => setTimeout(r, 500));
    }
  }

  private getOICandidates(): string[] {
    const now = Date.now();
    const candidates: { s: string, urgency: number }[] = [];
    
    // Configurable intervals
    const FAST = 5000, BASE = 15000, SLOW = 60000;

    for (const [sym, meta] of this.priorityMap.entries()) {
      let interval = BASE;
      if (meta.priority >= 10) interval = FAST;
      if (meta.priority <= 1) interval = SLOW;

      const elapsed = now - meta.lastUpdated;
      if (elapsed > interval) {
        candidates.push({ s: sym, urgency: elapsed - interval });
      }
    }
    
    return candidates
      .sort((a, b) => b.urgency - a.urgency)
      .slice(0, CONFIG.EXCHANGE.MAX_REQ_SEC)
      .map(c => c.s);
  }

  private async fetchOI(symbol: string) {
    try {
      const res = await this.axios.get(OI_API, { params: { symbol } });
      const val = parseFloat(res.data.openInterest);
      const ts = res.data.time || Date.now();
      
      if (this.eventCallback) {
        const e: OIEvent = { type: 'oi', symbol, ts, openInterest: val };
        this.eventCallback(e);
      }
      
      const meta = this.priorityMap.get(symbol);
      if (meta) meta.lastUpdated = Date.now();
    } catch (e) {
      // Handle 429 backoff locally
    }
  }

  // Monitor Tickers to adjust priority (Infrastructure concern: optimization)
  private startTickerStream() {
    this.tickerWs = new WebSocket(`${STREAM_BASE}?streams=!ticker@arr`);
    this.tickerWs.on('message', (data) => {
      try {
        const arr = JSON.parse(data.toString()).data;
        if (!Array.isArray(arr)) return;
        for (const t of arr) {
          if (!this.activeSymbols.has(t.s)) continue;
          const change = Math.abs(parseFloat(t.P));
          const vol = parseFloat(t.q);
          const meta = this.priorityMap.get(t.s);
          if (meta) {
            meta.priority = (change > 3 || vol > 50_000_000) ? 10 : 5;
          }
        }
      } catch {}
    });
  }
}