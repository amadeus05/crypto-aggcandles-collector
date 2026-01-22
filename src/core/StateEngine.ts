import { SmartCandleRow } from '../domain/types';
import { AnyMarketEvent, TradeEvent, LiquidationEvent, KlineEvent } from '../domain/events';

interface SymbolState {
  symbol: string;
  current: SmartCandleRow;        // Текущая (Live)
  previous: SmartCandleRow | null;// Предыдущая (Grace period)

  // Глобальные аккумуляторы
  globalCvd: number;              // Накапливаемый CVD за все время жизни бота

  // Вспомогательные данные для инициализации
  lastKnownOI: number;
  lastKnownFunding: number;
}

export class StateEngine {
  private states = new Map<string, SymbolState>();

  // Настройки таймингов
  private readonly GRACE_PERIOD_MS = 2000; // Окно для soft-updates
  private finalizeIntervalId: NodeJS.Timeout;

  constructor(
    private onStreamUpdate: (row: SmartCandleRow) => void,
    private onPersistUpdate: (row: SmartCandleRow) => void
  ) {
    // Хард-финализация закрытых 1m свечей, чтобы downstream (5m/15m) получал ровно 1 финальный апдейт.
    this.finalizeIntervalId = setInterval(() => this.finalizeExpired(), 250);
  }

  public shutdown() {
    clearInterval(this.finalizeIntervalId);
  }

  public processEvent(e: AnyMarketEvent) {
    let state = this.states.get(e.symbol);
    if (!state) {
      state = this.createState(e.symbol, e.ts);
      this.states.set(e.symbol, state);
    }

    // 1. OI & Funding Updates
    if (e.type === 'oi') {
      this.handleOI(state, e.ts, e.openInterest);
      return;
    }
    if (e.type === 'funding') {
      state.lastKnownFunding = e.rate;
      state.current.funding = e.rate; // Funding всегда актуален для текущей
      this.onStreamUpdate(state.current);
      return;
    }

    // 2. Routing Events (Trade, Liquidation, Kline)
    const currentStart = state.current.ts;
    const currentEnd = currentStart + 60000;

    // A) Событие относится к текущей свече
    if (e.ts >= currentStart && e.ts < currentEnd) {
      this.applyEventToCandle(state, state.current, e);

      // Смена минуты ТОЛЬКО по флагу kline.isClosed
      if (e.type === 'kline' && e.isClosed) {
        this.rotateCandle(state);
      } else {
        this.onStreamUpdate(state.current);
      }
    }

    // B) Событие относится к предыдущей свече (Grace Period)
    else if (state.previous && e.ts >= state.previous.ts && e.ts < (state.previous.ts + 60000)) {
      this.handleLateEvent(state, state.previous, e);
    }

    // C) Событие из будущего (e.ts >= currentEnd)
    else if (e.ts >= currentEnd) {
      // Игнорируем трейды из будущего, пока не придет kline closed для текущей.
      // Это предотвращает создание "дырок" и рассинхрон OHLC.
      // Как только придет kline closed, мы переключимся, и следующие трейды пойдут нормально.
      return;
    }
  }

  private handleOI(state: SymbolState, ts: number, val: number) {
    state.lastKnownOI = val;

    // Если OI пришел для пред. свечи в Grace period
    if (state.previous && !state.previous.isFinalized) {
      const prevEnd = state.previous.ts + 60000;
      // Если TS события попадает в диапазон предыдущей свечи (плюс небольшой люфт)
      if (ts < prevEnd + this.GRACE_PERIOD_MS) {
        state.previous.oi = val;
        this.onPersistUpdate(state.previous);
      }
    }

    // Текущую обновляем всегда
    state.current.oi = val;
    this.onStreamUpdate(state.current);
  }

  private handleLateEvent(state: SymbolState, prev: SmartCandleRow, e: AnyMarketEvent) {
    // 1. Проверка Hard Finalization
    if (prev.isFinalized) return;

    // 2. Проверка времени (Hard Stop по системному времени)
    // Если с момента закрытия прошло больше GRACE_PERIOD_MS, финализируем и выходим
    const timeSinceClose = Date.now() - (prev.ts + 60000);
    if (timeSinceClose > this.GRACE_PERIOD_MS) {
      prev.isFinalized = true;
      this.onPersistUpdate(prev);
      this.onStreamUpdate(prev);
      state.previous = null;
      return;
    }

    // 3. Применяем изменения
    this.applyEventToCandle(state, prev, e);

    // 4. Отправляем апдейт в БД и Стрим
    this.onPersistUpdate(prev);
    this.onStreamUpdate(prev);
  }

  private rotateCandle(state: SymbolState) {
    // Текущая становится предыдущей
    state.current.isClosed = true; // Soft close
    state.previous = state.current;

    // Первичная запись в БД (insert/upsert)
    this.onPersistUpdate(state.previous);

    // Подготовка новой свечи
    const nextTs = state.previous.ts + 60000;

    // CVD наследуется (Continuous)
    // Funding и OI берутся последние известные
    state.current = this.createCandle(
      state.symbol,
      nextTs,
      state.previous.c,
      state.globalCvd, // Наследуем накопленный итог
      state.lastKnownOI,
      state.lastKnownFunding
    );

    this.onStreamUpdate(state.current);
  }

  private applyEventToCandle(state: SymbolState, row: SmartCandleRow, e: AnyMarketEvent) {
    if (row.isFinalized) return; // Защита от записи в замороженную свечу

    switch (e.type) {
      case 'trade':
        this.mergeTrade(state, row, e as TradeEvent);
        break;
      case 'liquidation':
        this.mergeLiquidation(row, e as LiquidationEvent);
        break;
      case 'kline':
        this.mergeKline(row, e as KlineEvent);
        break;
    }
  }

  private mergeTrade(state: SymbolState, row: SmartCandleRow, e: TradeEvent) {
    const quoteVal = e.price * e.qty; // USDT Volume
    const baseVal = e.qty;            // Contract Volume

    // CVD считаем в деньгах (USDT Delta)
    const delta = e.isBuyerMaker ? -quoteVal : quoteVal;

    row.v += baseVal;        // Base asset volume
    row.quote_v += quoteVal; // Quote asset volume

    // Обновляем Delta свечи
    row.delta += delta;

    // Обновляем Global CVD стейта
    if (row === state.current) {
      state.globalCvd += delta;
      row.cvd = state.globalCvd;
    } else {
      // Late update для previous: CVD — накопительная метрика, поэтому нужно сдвинуть и глобальный CVD,
      // иначе последующие свечи будут иметь неконсистентный baseline.
      state.globalCvd += delta;
      row.cvd += delta;
      // Текущая свеча хранит абсолютный CVD, поэтому её тоже нужно сдвинуть.
      state.current.cvd = state.globalCvd;
    }

    row.last_price = e.price;
    row.c = e.price;
    // НЕ обновляем H/L здесь - kline closed будет авторитетным источником
  }

  private mergeLiquidation(row: SmartCandleRow, e: LiquidationEvent) {
    const val = e.price * e.qty;
    // TODO(алго/качество данных):
    // 1) Сейчас агрегация ликвидаций идёт только в notional (USDT). Можно параллельно копить qty (контракты),
    //    чтобы потом строить нормализованные фичи и сравнивать разные инструменты.
    // 2) Если понадобятся стратегии "по уровню цены", имеет смысл хранить компактный профайл:
    //    например histogram по price buckets или top-N крупнейших ликвидаций за минуту (с price).
    // 3) Подумать про дедуп/защиту от повторов от Binance (orderId отсутствует в текущем событии),
    //    если будет замечено задвоение в потоке.
    if (e.side === 'LONG') {
      row.liquidations.long += val;
      row.liquidations.countLong++;
      row.liquidations.maxLong = Math.max(row.liquidations.maxLong, val);
    } else {
      row.liquidations.short += val;
      row.liquidations.countShort++;
      row.liquidations.maxShort = Math.max(row.liquidations.maxShort, val);
    }
  }

  private mergeKline(row: SmartCandleRow, e: KlineEvent) {
    // Принимаем ТОЛЬКО closed klines для финальных OHLC данных
    if (!e.isClosed) return;

    // Kline closed - авторитетный источник OHLC и Volume
    row.o = e.open;
    row.h = e.high;
    row.l = e.low;
    row.c = e.close;
    row.v = e.volume; // Base volume от Binance (точнее чем наш счётчик)
    row.last_price = e.close;
  }

  private createState(symbol: string, startTs: number): SymbolState {
    // Выравниваем время к началу минуты
    const alignedTs = Math.floor(startTs / 60000) * 60000;
    return {
      symbol,
      current: this.createCandle(symbol, alignedTs, 0, 0, 0, 0),
      previous: null,
      globalCvd: 0,
      lastKnownOI: 0,
      lastKnownFunding: 0
    };
  }

  private createCandle(
    symbol: string,
    ts: number,
    open: number,
    startCvd: number,
    oi: number,
    funding: number
  ): SmartCandleRow {
    return {
      symbol,
      ts,
      o: open, h: open, l: open, c: open,
      v: 0,       // Base Vol reset
      quote_v: 0, // Quote Vol reset
      delta: 0,   // Delta reset
      cvd: startCvd, // CVD continues
      oi,
      funding,
      liquidations: {
        long: 0, short: 0, countLong: 0, countShort: 0, maxLong: 0, maxShort: 0
      },
      last_price: open,
      isClosed: false,
      isFinalized: false
    };
  }

  private finalizeExpired() {
    const now = Date.now();
    for (const state of this.states.values()) {
      const prev = state.previous;
      if (!prev || prev.isFinalized) continue;

      const closeAt = prev.ts + 60000;
      if (now - closeAt > this.GRACE_PERIOD_MS) {
        prev.isFinalized = true;
        this.onPersistUpdate(prev);
        this.onStreamUpdate(prev);
        state.previous = null;
      }
    }
  }
}