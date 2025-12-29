import WebSocket from 'ws';
import axios, { AxiosInstance } from 'axios';
import * as https from 'https';
import PQueue from 'p-queue';

const SYMBOL_MODE = (process.env.SYMBOL_MODE || 'TOP').toUpperCase(); // 'ALL' | 'TOP'
const TOP_SYMBOLS_LIMIT = Number(process.env.TOP_SYMBOLS_LIMIT || 50);

const IO_POLL_BASE_INTERVAL = Number(process.env.IO_POLL_BASE_INTERVAL || 15_000);
const IO_POLL_FAST_INTERVAL = Number(process.env.IO_POLL_MIN_INTERVAL || 5_000);
const IO_POLL_LOWP_INTERVAL = Number(process.env.IO_POLL_MIN_INTERVAL || 60_000);

const SYMBOL_WHITELIST = new Set(
  (process.env.SYMBOL_WHITELIST || '')
    .split(',')
    .map(s => s.trim().toUpperCase())
    .filter(Boolean)
);

const SYMBOL_BLACKLIST = new Set(
  (process.env.SYMBOL_BLACKLIST || '')
    .split(',')
    .map(s => s.trim().toUpperCase())
    .filter(Boolean)
);

// --- КОНФИГУРАЦИЯ ---
const BINANCE_FUTURES_TICKER_24HR_URL = 'https://fapi.binance.com/fapi/v1/ticker/24hr';
const BINANCE_FUTURES_EXCHANGE_INFO_URL = 'https://fapi.binance.com/fapi/v1/exchangeInfo';
const BINANCE_FUTURES_OI_API = 'https://fapi.binance.com/fapi/v1/openInterest';
const BINANCE_FUTURES_STREAM_BASE = 'wss://fstream.binance.com/stream';

const BATCH_SIZE = 30;
const KLINE_INTERVAL = '1m';

// ОПТИМИЗАЦИЯ ПОД 550 МОНЕТ:
// Лимит Binance IP для FAPI ~2400 в минуту. 
// 35 запросов/сек * 60 = 2100. Это безопасно и быстро.
const MAX_REQ_PER_SEC = Number(process.env.MAX_REQ_SEC || 35);

// Увеличили таймаут, чтобы при нагрузке запросы не обрывались
const HTTP_TIMEOUT = 10000;

// Список пар с принудительно низким приоритетом (крупные ликвидные монеты обновляем реже)
const LOW_PRIORITY_SYMBOLS = new Set([
  'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'XRPUSDT', 'DOGEUSDT', 'ADAUSDT',
  'TRXUSDT', 'LINKUSDT', 'AVAXUSDT', 'MATICUSDT', 'DOTUSDT', 'LTCUSDT',
  'USDCUSDT', 'BUSDUSDT', 'EURUSDT'
]);

// OI snapshot с таймстампом для точного сопоставления со свечой
interface OISnapshot {
  ts: number;    // timestamp когда получен OI
  value: number; // значение OI
}

// Максимум истории OI (храним 5 минут = 5-10 записей макс)
const MAX_OI_HISTORY = 10;

interface SymbolState {
  symbol: string;
  cvd: number;           // накопленный CVD (истина из aggTrade)
  candleDelta: number;   // delta ТЕКУЩЕЙ минуты
  lastCandleTimestamp: number;
  fundingRate: number;
  openInterest: number;  // последнее полученное значение OI (live)
  oiHistory: OISnapshot[]; // 🔥 история OI с таймстампами
  lastOITimestamp: number;
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
  queueLag?: number; // Добавим метрику отставания очереди
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
  private axiosInstance: AxiosInstance;

  private callback: MarketDataCallback | null = null;
  private messageCount = 0;
  private errorCount = 0;
  private reconnectAttempts = 0;
  private lastUpdateTime = 0;
  private maxQueueDelay = 0; // Для мониторинга

  // 🔥 Rate limiter для OI polling (предотвращает burst)
  private oiQueue: PQueue | null = null;
  private useRateLimiter: boolean;

  constructor(public marketType: 'futures') {
    this.providerId = `binance-${marketType}-ws`;

    // 🔥 Включение rate limiter через env
    this.useRateLimiter = process.env.USE_RATE_LIMITER === 'true';

    if (this.useRateLimiter) {
      this.oiQueue = new PQueue({
        interval: 1000,
        intervalCap: MAX_REQ_PER_SEC
      });
      console.log(`[${this.providerId}] Rate limiter ENABLED (${MAX_REQ_PER_SEC} req/sec)`);
    }

    // НАСТРОЙКА СЕТИ: Оптимизируем Agent для высокой конкурентности
    this.axiosInstance = axios.create({
      httpsAgent: new https.Agent({
        keepAlive: true,
        // Разрешаем открывать больше сокетов, чем запросов в секунду
        maxSockets: MAX_REQ_PER_SEC + 15,
        maxFreeSockets: MAX_REQ_PER_SEC,
        timeout: 60000 // Таймаут TCP соединения
      }),
      timeout: HTTP_TIMEOUT, // Таймаут ожидания ответа
      // headers: {
      //   'User-Agent': 'NodeCryptoCollector/1.0'
      // }
    });

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
    // 1. Получаем базовую информацию о парах
    const infoRes = await this.axiosInstance.get(BINANCE_FUTURES_EXCHANGE_INFO_URL);
    const data = infoRes.data;
    
    // Фильтруем валидные пары (USDT, PERPETUAL, TRADING)
    let validSymbols = (data.symbols || []).filter((s: any) => 
      s.contractType === 'PERPETUAL' && 
      s.marginAsset === 'USDT' && 
      s.status === 'TRADING'
    );

    // 2. Применяем Whitelist (если задан, оставляем ТОЛЬКО эти монеты)
    if (SYMBOL_WHITELIST.size > 0) {
      validSymbols = validSymbols.filter((s: any) => SYMBOL_WHITELIST.has(s.symbol));
      console.log(`[${this.providerId}] Applied Whitelist: ${validSymbols.length} symbols left`);
    }

    // 3. Применяем Blacklist (исключаем монеты)
    if (SYMBOL_BLACKLIST.size > 0) {
      validSymbols = validSymbols.filter((s: any) => !SYMBOL_BLACKLIST.has(s.symbol));
    }

    // 4. Режим TOP (фильтрация по объему)
    // Если включен TOP и (Whitelist пустой или нужно отфильтровать даже внутри вайтлиста)
    if (SYMBOL_MODE === 'TOP') {
      try {
        console.log(`[${this.providerId}] Fetching 24hr ticker for TOP-${TOP_SYMBOLS_LIMIT} sort...`);
        const tickerRes = await this.axiosInstance.get(BINANCE_FUTURES_TICKER_24HR_URL);
        
        // Создаем карту объемов: symbol -> quoteVolume (USDT turnover)
        const volumeMap = new Map<string, number>();
        for (const t of tickerRes.data) {
          volumeMap.set(t.symbol, parseFloat(t.quoteVolume));
        }

        // Сортируем validSymbols по убыванию объема
        validSymbols.sort((a: any, b: any) => {
          const volA = volumeMap.get(a.symbol) || 0;
          const volB = volumeMap.get(b.symbol) || 0;
          return volB - volA; // descending
        });

        // Отрезаем топ N
        const beforeSlice = validSymbols.length;
        validSymbols = validSymbols.slice(0, TOP_SYMBOLS_LIMIT);
        console.log(`[${this.providerId}] Mode TOP: kept ${validSymbols.length} of ${beforeSlice} symbols`);
        
      } catch (err) {
        console.error(`[${this.providerId}] Failed to fetch ticker for TOP sort, falling back to ALL valid symbols`, err);
        // Не выбрасываем ошибку, чтобы продолжить работу хотя бы с несортированным списком
      }
    }

    // 5. Инициализация состояния
    this.symbols.clear();
    this.marketStates.clear();
    this.priorityMap.clear();

    for (const s of validSymbols) {
      this.symbols.add(s.symbol);
      this.marketStates.set(s.symbol, {
        symbol: s.symbol,
        cvd: 0,
        candleDelta: 0,
        lastCandleTimestamp: 0,
        fundingRate: 0,
        openInterest: 0,
        oiHistory: [],
        lastOITimestamp: 0,
        lastPrice: 0,
        accLiqLong: 0, accLiqShort: 0,
        countLiqLong: 0, countLiqShort: 0,
        maxLiqLong: 0, maxLiqShort: 0,
      });
      
      const initialPriority = LOW_PRIORITY_SYMBOLS.has(s.symbol) ? 1 : 5;
      this.priorityMap.set(s.symbol, { priority: initialPriority, lastUpdated: 0 });
    }
    
    console.log(`[${this.providerId}] Final load: ${this.symbols.size} symbols ready`);
  }

  public async connect(): Promise<void> {
    if (this.connected) return;
    await this.readyPromise;
    this.connected = true;

    // Сначала загружаем OI для всех
    console.log(`[${this.providerId}] Fetching initial OI for all symbols...`);
    await this.fetchInitialOI();
    console.log(`[${this.providerId}] Initial OI fetch complete`);

    this.subscribeToBatches();
    this.startAllTickersStream();
    this.startSmartOIPolling();
    console.log(`[${this.providerId}] Connected. Max Rate: ${MAX_REQ_PER_SEC} req/s`);
  }

  private async fetchInitialOI(): Promise<void> {
    const symbols = Array.from(this.symbols);
    const batchSize = 10;
    const delayBetweenBatches = 600;

    for (let i = 0; i < symbols.length; i += batchSize) {
      if (!this.connected) break;
      const batch = symbols.slice(i, i + batchSize);
      await Promise.all(batch.map(async (symbol) => {
        try {
          const res = await this.axiosInstance.get(BINANCE_FUTURES_OI_API, { params: { symbol } });
          if (res.data?.openInterest) {
            const state = this.marketStates.get(symbol);
            if (state) state.openInterest = parseFloat(res.data.openInterest);
            const p = this.priorityMap.get(symbol);
            if (p) p.lastUpdated = Date.now();
          }
        } catch { }
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
      return `${sym}@kline_${KLINE_INTERVAL}/${sym}@markPrice/${sym}@forceOrder/${sym}@aggTrade`;
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
        // 🔥 FIX: удаляем таймер из Set при выполнении во избежание утечки памяти
        this.reconnectTimers.delete(timer);
        
        if (!this.connected) return;
        const idx = this.wsList.indexOf(ws);
        if (idx !== -1) {
          this.wsList[idx] = this.createBatchWS(batch);
        }
        this.reconnectAttempts++;
        // console.log(`[${this.providerId}] Reconnected batch`);
      }, 3000);
      this.reconnectTimers.add(timer);
    });

    // @ts-ignore
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
      else if (msg.stream.includes('aggTrade')) this.processAggTrade(msg.data);
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
    const candleTimestamp = k.t;
    const candleCloseTs = k.T; // 🔥 точный timestamp закрытия свечи
    const isClosed = k.x === true;

    // lastPrice обновляется в processAggTrade для точности
    if (state.lastPrice === 0) state.lastPrice = close;

    // 🔥 OI SNAPSHOT: выбираем последний OI с timestamp ≤ candle close
    const oiToEmit = isClosed
      ? this.getOIAtTimestamp(state, candleCloseTs)
      : state.openInterest;

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
        cvd: state.cvd,
        candleDelta: state.candleDelta,
        fundingRate: state.fundingRate,
        openInterest: oiToEmit, // ← точный OI на момент close
        liquidationsLong: state.accLiqLong,
        liquidationsShort: state.accLiqShort,
        liqCountLong: state.countLiqLong,
        liqCountShort: state.countLiqShort,
        liqMaxLong: state.maxLiqLong,
        liqMaxShort: state.maxLiqShort,
      }
    });

    // RESET ТОЛЬКО НА CLOSE (FIX RACE CONDITION)
    if (isClosed) {
      // ⚠️ candleDelta сбрасывается в aggTrade
      state.accLiqLong = 0; state.accLiqShort = 0;
      state.countLiqLong = 0; state.countLiqShort = 0;
      state.maxLiqLong = 0; state.maxLiqShort = 0;
      
      // FAILSAFE: На случай если kline пришел значительно позже aggTrade
      if (candleTimestamp > state.lastCandleTimestamp) {
        state.lastCandleTimestamp = candleTimestamp;
        state.candleDelta = 0;
      }
    }

    this.lastUpdateTime = Date.now();
    this.messageCount++;
  }

  // 🔥 Выбираем последний OI с timestamp ≤ targetTs
  private getOIAtTimestamp(state: SymbolState, targetTs: number): number {
    // Ищем последний OI, который был получен ДО или В момент закрытия свечи
    let bestOI = state.openInterest; // fallback на текущий
    let bestTs = 0;

    for (const snap of state.oiHistory) {
      if (snap.ts <= targetTs && snap.ts > bestTs) {
        bestOI = snap.value;
        bestTs = snap.ts;
      }
    }

    return bestOI;
  }

  // 🔥 ИСТИНА BINANCE: CVD из aggTrade
  private processAggTrade(data: any): void {
    const state = this.marketStates.get(data.s);
    if (!state) return;

    const price = parseFloat(data.p);
    const qty = parseFloat(data.q);
    const usd = price * qty;
    const delta = data.m ? -usd : usd;
    
    // Получаем начало минуты, к которой относится этот трейд
    const tradeCandleStart = Math.floor(data.T / 60000) * 60000;

    // Инициализация (первый запуск)
    if (state.lastCandleTimestamp === 0) {
      state.lastCandleTimestamp = tradeCandleStart;
    }

    // СЦЕНАРИЙ 1: Пришел трейд из НОВОЙ свечи (мы шагнули в будущее)
    if (tradeCandleStart > state.lastCandleTimestamp) {
      state.candleDelta = 0; // Сбрасываем дельту
      state.lastCandleTimestamp = tradeCandleStart; // Обновляем время
      state.candleDelta += delta; // Записываем первый трейд новой свечи
    }
    // СЦЕНАРИЙ 2: Трейд относится к ТЕКУЩЕЙ отслеживаемой свече
    else if (tradeCandleStart === state.lastCandleTimestamp) {
      state.candleDelta += delta;
    }
    // СЦЕНАРИЙ 3: Запоздалый пакет -> Игнорируем для дельты

    state.cvd += delta;
    state.lastPrice = price;
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

    // 🔥 FIX: Фильтруем старые ликвидации (Latency issue)
    const liqTime = o.T || data.E; 
    // Если ликвидация из прошлого (до начала текущей свечи) — игнорируем
    if (state.lastCandleTimestamp > 0 && liqTime < state.lastCandleTimestamp) {
      return;
    }

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
    try {
      this.callback({
        providerId: this.providerId,
        marketType: this.marketType,
        symbol: state.symbol,
        price: payload.price,
        timestamp: payload.timestamp,
        isCandleClosed: payload.isClosed,
        ohlc: payload.ohlc,
        indicators: payload.indicators,
      });
    } catch (e) {
      this.errorCount++;
    }
  }

  // --- TICKER STREAM ---
  private startAllTickersStream(): void {
    this.tickerWs = new WebSocket(`${BINANCE_FUTURES_STREAM_BASE}?streams=!ticker@arr`);

    this.tickerWs.on('message', (data) => {
      try {
        const msg = JSON.parse(data.toString());
        const arr = msg.data;
        if (!Array.isArray(arr)) return;

        for (const t of arr) {
          const sym = t.s;
          // Игнорируем low priority, они всегда обновляются раз в минуту
          if (!this.symbols.has(sym) || LOW_PRIORITY_SYMBOLS.has(sym)) continue;

          const change = Math.abs(parseFloat(t.P));
          const vol = parseFloat(t.q);
          const p = this.priorityMap.get(sym);

          if (p) {
            // Если волатильность > 3% или объем > 50M - повышаем приоритет до 2 сек
            p.priority = (change > 3 || vol > 50_000_000) ? 10 : 5;
          }
        }
      } catch { }
    });

    this.tickerWs.on('error', (err) => {
      // console.error(`[${this.providerId}] Ticker WS error`);
      this.errorCount++;
    });

    this.tickerWs.on('close', () => {
      setTimeout(() => {
        if (this.connected) this.startAllTickersStream();
      }, 5000);
    });
  }

  // --- SMART OI POLLING (ИСПРАВЛЕННАЯ ЛОГИКА) ---
  private async startSmartOIPolling(): Promise<void> {
    this.isPollingOI = true;

    while (this.isPollingOI && this.connected) {
      const start = Date.now();

      // 1. Выбираем кандидатов (сортировка по срочности)
      const candidates = this.selectOICandidates();

      // 2. Делаем запросы с rate limiting или параллельно
      if (candidates.length > 0) {
        if (this.useRateLimiter && this.oiQueue) {
          // 🔥 Rate-limited: ≤35 req/sec гарантировано
          await Promise.all(
            candidates.map(sym => this.oiQueue!.add(() => this.fetchOI(sym)))
          );
        } else {
          // Старый режим: burst
          await Promise.all(candidates.map((sym) => this.fetchOI(sym)));
        }
      }

      const elapsed = Date.now() - start;
      // Стараемся держать ритм, но не спамить
      const sleep = Math.max(1000 - elapsed, 100);
      await new Promise((r) => setTimeout(r, sleep));
    }
  }

  // КЛЮЧЕВОЙ МЕТОД: Устраняет проблему "застывания" монет в конце списка
  private selectOICandidates(): string[] {
    const now = Date.now();
    const candidates: { symbol: string; urgency: number }[] = [];

    for (const [sym, p] of this.priorityMap.entries()) {
      let interval = IO_POLL_BASE_INTERVAL; // Стандарт (priority 5)
      if (p.priority === 1) interval = IO_POLL_LOWP_INTERVAL;
      else if (p.priority === 10) interval = IO_POLL_FAST_INTERVAL;

      const elapsed = now - p.lastUpdated;

      if (elapsed > interval) {
        // Чем больше времени прошло сверх интервала, тем выше urgency
        candidates.push({
          symbol: sym,
          urgency: elapsed - interval
        });
      }
    }

    // Сортировка по убыванию urgency (самые "голодные" идут первыми)
    candidates.sort((a, b) => b.urgency - a.urgency);

    if (candidates.length > 0) {
      this.maxQueueDelay = candidates[0].urgency;
    }

    // Берем топ N задач, чтобы не превысить лимиты
    return candidates
      .slice(0, MAX_REQ_PER_SEC)
      .map(c => c.symbol);
  }

  private async fetchOI(symbol: string): Promise<void> {
    try {
      const res = await this.axiosInstance.get(
        BINANCE_FUTURES_OI_API,
        { params: { symbol } }
      );

      if (res.data?.openInterest) {
        const val = parseFloat(res.data.openInterest);
        // 🔥 Используем Binance server time, не Date.now()
        const serverTs = res.data.time ?? Date.now();
        const state = this.marketStates.get(symbol);
        const p = this.priorityMap.get(symbol);

        if (state) {
          state.openInterest = val;
          state.lastOITimestamp = serverTs;

          // 🔥 Добавляем в историю с СЕРВЕРНЫМ таймстампом
          state.oiHistory.push({ ts: serverTs, value: val });

          // Ограничиваем размер истории
          if (state.oiHistory.length > MAX_OI_HISTORY) {
            state.oiHistory.shift();
          }
        }

        // Обновляем lastUpdated ТОЛЬКО при успехе
        if (p) p.lastUpdated = Date.now();
      }
    } catch (err: any) {

      // Игнорируем сетевые ошибки, монета останется с высоким urgency и обновится в след. цикле
      if (axios.isAxiosError(err) && (err.code === 'ECONNABORTED' || err.code === 'ETIMEDOUT')) {
        // Можно включить, если нужно отлаживать сеть
        // console.warn(`[OI] Timeout ${symbol}`);
      } else {
        console.error(`[ERROR] fetchOI ${symbol} ${err.message}`);
      }
      this.errorCount++;
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
      queueLag: this.maxQueueDelay, // Показывает, насколько самая старая монета отстала от графика (мс)
    };
  }
}