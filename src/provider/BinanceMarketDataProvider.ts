import WebSocket from 'ws';
import axios from 'axios';
import * as https from 'https';

// КОНФИГУРАЦИЯ
const BINANCE_FUTURES_EXCHANGE_INFO_URL = 'https://fapi.binance.com/fapi/v1/exchangeInfo';
const BINANCE_FUTURES_OI_API = 'https://fapi.binance.com/fapi/v1/openInterest';
const BINANCE_FUTURES_STREAM_BASE = 'wss://fstream.binance.com/stream';

const BATCH_SIZE = 30;
const KLINE_INTERVAL = '1m';
const MAX_REQ_PER_SEC = 20;
const HTTP_TIMEOUT = 2000;

// Список пар с принудительно низким приоритетом (крупные стабильные монеты)
const LOW_PRIORITY_SYMBOLS = new Set([
  'BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'XRPUSDT', 'DOGEUSDT', 'ADAUSDT',
  'TRXUSDT', 'LINKUSDT', 'AVAXUSDT', 'MATICUSDT', 'DOTUSDT', 'LTCUSDT',
  'USDCUSDT', 'BUSDUSDT', 'EURUSDT'
]);

interface SymbolState {
  symbol: string;
  cumulativeCVD: number;
  lastCandleTimestamp: number;
  fundingRate: number;
  openInterest: number;
  lastPrice: number;
  accLiqLong: number;
  accLiqShort: number;
  countLiqLong: number;
  countLiqShort: number;
  maxLiqLong: number;
  maxLiqShort: number;
}

interface SymbolPriority {
  priority: number;
  lastUpdated: number;
}

export interface MarketDataUpdate {
  providerId: string;
  marketType: 'futures';
  symbol: string;
  price: number;
  timestamp: number;
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
    fundingRate: number;
    openInterest: number;
    liquidationsLong: number;
    liquidationsShort: number;
    liqCountLong: number;
    liqCountShort: number;
    liqMaxLong: number;
    liqMaxShort: number;
  };
}

export interface ProviderHealthStatus {
  providerId: string;
  marketType: 'futures';
  isConnected: boolean;
  lastUpdateTime: number;
  messageCount: number;
  reconnectAttempts: number;
  errorCount: number;
}

export type MarketDataCallback = (data: MarketDataUpdate) => void;

export class BinanceMarketDataProvider {
  public readonly providerId: string;

  private wsList: WebSocket[] = [];
  private tickerWs: WebSocket | null = null;
  private symbols = new Set<string>();
  private marketStates = new Map<string, SymbolState>();
  private priorityMap = new Map<string, SymbolPriority>();

  private connected = false;
  private reconnectTimers = new Set<NodeJS.Timeout>();
  private readyPromise: Promise<void>;
  private isPollingOI = false;
  private axiosInstance = axios.create({
    httpsAgent: new https.Agent({ keepAlive: true }),
    timeout: HTTP_TIMEOUT,
  });

  private callback: MarketDataCallback | null = null;
  private messageCount = 0;
  private errorCount = 0;
  private reconnectAttempts = 0;
  private lastUpdateTime = 0;

  constructor(public marketType: 'futures') {
    this.providerId = `binance-${marketType}-ws`;
    this.readyPromise = this.loadSymbolsWithRetry();
  }

  private async loadSymbolsWithRetry(): Promise<void> {
    for (let attempt = 1; attempt <= 5; attempt++) {
      try {
        await this.loadSymbols();
        return;
      } catch (e) {
        console.warn(`[${this.providerId}] loadSymbols attempt ${attempt} failed, retrying...`);
        await new Promise((r) => setTimeout(r, 2000 * attempt));
      }
    }
    throw new Error('Failed to load symbols after 5 attempts');
  }

  private async loadSymbols(): Promise<void> {
    const res = await this.axiosInstance.get(BINANCE_FUTURES_EXCHANGE_INFO_URL);
    const data = res.data;

    this.symbols.clear();
    this.marketStates.clear();
    this.priorityMap.clear();

    for (const s of data.symbols || []) {
      if (s.contractType === 'PERPETUAL' && s.marginAsset === 'USDT' && s.status === 'TRADING') {
        this.symbols.add(s.symbol);
        this.marketStates.set(s.symbol, {
          symbol: s.symbol,
          cumulativeCVD: 0,
          lastCandleTimestamp: 0,
          fundingRate: 0,
          openInterest: 0,
          lastPrice: 0,
          accLiqLong: 0, accLiqShort: 0,
          countLiqLong: 0, countLiqShort: 0,
          maxLiqLong: 0, maxLiqShort: 0,
        });
        const initialPriority = LOW_PRIORITY_SYMBOLS.has(s.symbol) ? 1 : 5;
        this.priorityMap.set(s.symbol, { priority: initialPriority, lastUpdated: 0 });
      }
    }
    console.log(`[${this.providerId}] Loaded ${this.symbols.size} symbols`);
  }

  public async connect(): Promise<void> {
    if (this.connected) return;
    await this.readyPromise;
    this.connected = true;

    // Fetch initial OI for all symbols before starting streams
    console.log(`[${this.providerId}] Fetching initial OI for all symbols...`);
    await this.fetchInitialOI();
    console.log(`[${this.providerId}] Initial OI fetch complete`);

    this.subscribeToBatches();
    this.startAllTickersStream();
    this.startSmartOIPolling();
    console.log(`[${this.providerId}] Connected`);
  }

  private async fetchInitialOI(): Promise<void> {
    const symbols = Array.from(this.symbols);
    const batchSize = 10; // Parallel requests per batch
    const delayBetweenBatches = 600; // ms delay to respect rate limits

    for (let i = 0; i < symbols.length; i += batchSize) {
      const batch = symbols.slice(i, i + batchSize);
      await Promise.all(batch.map(async (symbol) => {
        try {
          const res = await this.axiosInstance.get(BINANCE_FUTURES_OI_API, { params: { symbol } });
          if (res.data?.openInterest) {
            const state = this.marketStates.get(symbol);
            if (state) {
              state.openInterest = parseFloat(res.data.openInterest);
            }
            const p = this.priorityMap.get(symbol);
            if (p) p.lastUpdated = Date.now();
          }
        } catch {
          // Skip failed symbols, they'll be retried in polling
        }
      }));

      if (i + batchSize < symbols.length) {
        await new Promise(r => setTimeout(r, delayBetweenBatches));
      }
    }
  }

  public async disconnect(): Promise<void> {
    this.connected = false;
    this.isPollingOI = false;
    this.reconnectTimers.forEach((t) => clearTimeout(t));
    this.reconnectTimers.clear();
    this.wsList.forEach((ws) => {
      try { ws.terminate(); } catch { }
    });
    this.wsList = [];
    if (this.tickerWs) {
      try { this.tickerWs.terminate(); } catch { }
      this.tickerWs = null;
    }
    console.log(`[${this.providerId}] Disconnected`);
  }

  // --- WEBSOCKETS ---
  private createBatchWS(batch: string[]): WebSocket {
    const streams = batch.map((s) => {
      const sym = s.toLowerCase();
      return `${sym}@kline_${KLINE_INTERVAL}/${sym}@markPrice/${sym}@forceOrder`;
    }).join('/');

    const ws = new WebSocket(`${BINANCE_FUTURES_STREAM_BASE}?streams=${streams}`);
    let closedByUs = false;

    ws.on('message', (data) => this.handleMessage(data.toString()));
    ws.on('error', (err) => {
      this.errorCount++;
      console.error(`[${this.providerId}] WS error:`, err.message);
    });
    ws.on('close', () => {
      if (closedByUs) return;
      const timer = setTimeout(() => {
        if (!this.connected) return;
        const idx = this.wsList.indexOf(ws);
        if (idx !== -1) {
          this.wsList[idx] = this.createBatchWS(batch);
        }
        this.reconnectAttempts++;
        console.log(`[${this.providerId}] Reconnected batch, total attempts: ${this.reconnectAttempts}`);
      }, 3000);
      this.reconnectTimers.add(timer);
    });

    // @ts-ignore - для graceful shutdown
    ws._closeGracefully = () => {
      closedByUs = true;
      try { ws.terminate(); } catch { }
    };

    return ws;
  }

  private subscribeToBatches(): void {
    const symbolsArray = Array.from(this.symbols);
    for (let i = 0; i < symbolsArray.length; i += BATCH_SIZE) {
      this.wsList.push(this.createBatchWS(symbolsArray.slice(i, i + BATCH_SIZE)));
    }
  }

  private handleMessage(raw: string): void {
    if (!this.callback) return;
    try {
      const msg = JSON.parse(raw);
      if (!msg.data) return;
      if (msg.stream.includes('kline')) this.processKline(msg.data);
      else if (msg.stream.includes('markPrice')) this.processMarkPrice(msg.data);
      else if (msg.stream.includes('forceOrder')) this.processLiquidation(msg.data);
    } catch (e) {
      this.errorCount++;
    }
  }

  // --- PROCESSING ---
  private processKline(data: any): void {
    const k = data.k;
    const symbol = k.s;
    const state = this.marketStates.get(symbol);
    if (!state) return;

    const close = parseFloat(k.c);
    const vol = parseFloat(k.v);
    
    // Используем Quote Volume (USDT) напрямую, без усреднения цены
    const quoteVol = parseFloat(k.q);          // Общий объем в $
    const takerBuyQuoteVol = parseFloat(k.Q);  // Объем покупок маркет-ордерами в $

    // Формула: (Покупки - Продажи) = (Покупки - (Всего - Покупки)) = 2*Покупки - Всего
    const deltaUSD = (takerBuyQuoteVol * 2) - quoteVol;

    state.lastPrice = close;
    const candleTimestamp = k.t;
    const isClosed = k.x === true;
    const liveCVD = state.cumulativeCVD + deltaUSD;

    this.emitUpdate(state, {
      price: close,
      isClosed,
      timestamp: candleTimestamp,
      ohlc: {
        open: parseFloat(k.o),
        high: parseFloat(k.h),
        low: parseFloat(k.l),
        close: close,
        volume: vol,
      },
      indicators: {
        cvd: liveCVD,
        candleDelta: deltaUSD,
        fundingRate: state.fundingRate,
        openInterest: state.openInterest,
        liquidationsLong: state.accLiqLong,
        liquidationsShort: state.accLiqShort,
        liqCountLong: state.countLiqLong,
        liqCountShort: state.countLiqShort,
        liqMaxLong: state.maxLiqLong,
        liqMaxShort: state.maxLiqShort,
      }
    });

    if (isClosed) {
      state.cumulativeCVD += deltaUSD;
      state.accLiqLong = 0; state.accLiqShort = 0;
      state.countLiqLong = 0; state.countLiqShort = 0;
      state.maxLiqLong = 0; state.maxLiqShort = 0;
      state.lastCandleTimestamp = candleTimestamp;
    }

    this.lastUpdateTime = Date.now();
    this.messageCount++;
  }

  private processMarkPrice(data: any): void {
    const state = this.marketStates.get(data.s);
    if (state) state.fundingRate = parseFloat(data.r);
  }

  private processLiquidation(data: any): void {
    const o = data.o;
    const symbol = o.s;
    const state = this.marketStates.get(symbol);
    if (!state) return;

    const price = parseFloat(o.p);
    const qty = parseFloat(o.q);
    const amount = price * qty;
    const side = o.S === 'SELL' ? 'LONG' : 'SHORT';

    if (side === 'LONG') {
      state.accLiqLong += amount;
      state.countLiqLong++;
      state.maxLiqLong = Math.max(state.maxLiqLong, amount);
    } else {
      state.accLiqShort += amount;
      state.countLiqShort++;
      state.maxLiqShort = Math.max(state.maxLiqShort, amount);
    }
  }

  private emitUpdate(state: SymbolState, payload: {
    price: number;
    isClosed: boolean;
    timestamp: number;
    ohlc?: MarketDataUpdate['ohlc'];
    indicators: MarketDataUpdate['indicators'];
  }): void {
    if (!this.callback) return;

    const update: MarketDataUpdate = {
      providerId: this.providerId,
      marketType: this.marketType,
      symbol: state.symbol,
      price: payload.price,
      timestamp: payload.timestamp,
      isCandleClosed: payload.isClosed,
      ohlc: payload.ohlc,
      indicators: payload.indicators,
    };

    try {
      this.callback(update);
    } catch (e) {
      this.errorCount++;
    }
  }

  // --- TICKER STREAM для динамического приоритета ---
  private startAllTickersStream(): void {
    this.tickerWs = new WebSocket(`${BINANCE_FUTURES_STREAM_BASE}?streams=!ticker@arr`);

    this.tickerWs.on('message', (data) => {
      try {
        const msg = JSON.parse(data.toString());
        const arr = msg.data;
        if (!Array.isArray(arr)) return;

        for (const t of arr) {
          const sym = t.s;
          if (!this.symbols.has(sym) || LOW_PRIORITY_SYMBOLS.has(sym)) continue;

          const change = Math.abs(parseFloat(t.P)); // Percent change
          const vol = parseFloat(t.q); // Quote volume
          const p = this.priorityMap.get(sym);

          if (p) {
            // Высокий приоритет для волатильных или высоковолумных монет
            p.priority = (change > 3 || vol > 50_000_000) ? 10 : 5;
          }
        }
      } catch { }
    });

    this.tickerWs.on('error', (err) => {
      this.errorCount++;
      console.error(`[${this.providerId}] Ticker WS error:`, err.message);
    });

    this.tickerWs.on('close', () => {
      setTimeout(() => {
        if (this.connected) this.startAllTickersStream();
      }, 5000);
    });
  }

  // --- SMART OI POLLING ---
  private async startSmartOIPolling(): Promise<void> {
    this.isPollingOI = true;

    while (this.isPollingOI && this.connected) {
      const start = Date.now();
      const candidates = this.selectOICandidates();

      if (candidates.length > 0) {
        await Promise.all(candidates.map((sym) => this.fetchOI(sym)));
      }

      const elapsed = Date.now() - start;
      const sleep = Math.max(1000 - elapsed, 100);
      await new Promise((r) => setTimeout(r, sleep));
    }
  }

  private selectOICandidates(): string[] {
    const now = Date.now();
    const result: string[] = [];

    for (const [sym, p] of this.priorityMap.entries()) {
      // Интервалы опроса в зависимости от приоритета
      let interval = 15000; // default: 15 секунд
      if (p.priority === 1) interval = 60000;      // low priority: 1 минута
      else if (p.priority === 10) interval = 2000; // high priority: 2 секунды

      if (now - p.lastUpdated > interval) {
        result.push(sym);
        if (result.length >= MAX_REQ_PER_SEC) break;
      }
    }

    return result;
  }

  private async fetchOI(symbol: string): Promise<void> {
    try {
      const res = await this.axiosInstance.get(BINANCE_FUTURES_OI_API, { params: { symbol } });

      if (res.data?.openInterest) {
        const val = parseFloat(res.data.openInterest);
        const state = this.marketStates.get(symbol);

        if (state) {
          state.openInterest = val;

          const currentCandleTS = Math.floor(Date.now() / 60000) * 60000;

          this.emitUpdate(state, {
            price: state.lastPrice,
            isClosed: false,
            timestamp: currentCandleTS,
            ohlc: undefined,
            indicators: {
              cvd: state.cumulativeCVD,
              candleDelta: 0,
              fundingRate: state.fundingRate,
              openInterest: state.openInterest,
              liquidationsLong: state.accLiqLong,
              liquidationsShort: state.accLiqShort,
              liqCountLong: state.countLiqLong,
              liqCountShort: state.countLiqShort,
              liqMaxLong: state.maxLiqLong,
              liqMaxShort: state.maxLiqShort,
            }
          });

        }

        const p = this.priorityMap.get(symbol);
        if (p) p.lastUpdated = Date.now();
      }
    } catch {
      // Rate limit или сетевая ошибка — молча пропускаем
    }
  }

  // --- PUBLIC API ---
  public async getAvailableSymbols(): Promise<string[]> {
    return Array.from(this.symbols);
  }

  public onPriceUpdate(callback: MarketDataCallback): void {
    this.callback = callback;
  }

  public isConnected(): boolean {
    return this.connected;
  }

  public getHealthStatus(): ProviderHealthStatus {
    return {
      providerId: this.providerId,
      marketType: this.marketType,
      isConnected: this.connected,
      lastUpdateTime: this.lastUpdateTime,
      messageCount: this.messageCount,
      reconnectAttempts: this.reconnectAttempts,
      errorCount: this.errorCount,
    };
  }
}
