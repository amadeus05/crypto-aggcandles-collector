# Streaming Documentation

This microservice provides real-time cryptocurrency candle data via Socket.IO.

## Connection

Connected clients can subscribe to specific timeframes or symbols.

**Endpoint:** `http://localhost:<PORT>` (Default: 3000)

## Rooms & Subscriptions

### 1. Live Candles (`live` or `tf:live`)
Receive every update for current candles.
- **Global Room:** `live` (receives ALL updates for ALL symbols and timeframes)
- **TF Room:** `1m:live`, `5m:live`, `15m:live`
- **Subscription:** `socket.emit('subscribe', 'live')` or `socket.emit('subscribe', '1m:live')`
- **Use case:** Real-time UI charts, scalping bots.

### 2. Closed Candles (`closed` or `tf:closed`)
Receive only finalized candles.
- **Global Room:** `closed` (receives ALL closed candles for ALL symbols and timeframes)
- **TF Room:** `1m:closed`, `5m:closed`, `15m:closed`
- **Subscription:** `socket.emit('subscribe', 'closed')` or `socket.emit('subscribe', '5m:closed')`
- **Guarantee:** `row.isFinalized === true`
- **Use case:** ML models, backtesting, database synchronization.

### 3. Legacy Subscriptions (Backward Compatibility)
Receive all updates for a timeframe (live + finalized).
- **Room:** `1m`, `5m`, `15m`
- **Subscription:** `socket.emit('subscribe', '1m')`
- **Behavior:** No double events. Safe for generic storage or simple UIs.

### 4. Symbol Filter
Receive updates for a specific symbol.

- **Legacy (All updates):**
  - **Subscription:** `socket.emit('subscribe:symbol', 'BTCUSDT')`
- **Live Stream:**
  - **Subscription:** `socket.emit('subscribe:symbol', 'BTCUSDT:live')`
- **Closed Only (Guaranteed):**
  - **Subscription:** `socket.emit('subscribe:symbol', 'BTCUSDT:closed')`

## Events

### `candle`
Sent whenever a candle update occurs.

**Payload Structure:**
```typescript
{
  symbol: string;      // e.g., "BTCUSDT"
  tf: string;          // "1m", "5m", or "15m"
  ts: number;          // Opening timestamp (UTC ms)
  
  // OHLC
  o: number;           // Open price
  h: number;           // High price
  l: number;           // Low price
  c: number;           // Close price
  
  // Volumes
  v: number;           // Base Volume (Contracts/Coins)
  quote_v: number;     // Quote Volume (USDT)
  
  // Delta & CVD
  delta: number;       // Volume Delta for this candle (USDT)
  cvd: number;         // Cumulative Volume Delta (Total)
  
  // Meta
  oi: number;          // Open Interest
  funding: number;     // Funding Rate
  
  // Liquidations (aggregated for this candle)
  liquidations: {
    long: number;      // Total liquidation value for longs (USDT)
    short: number;     // Total liquidation value for shorts (USDT)
    countLong: number; // Count of long liquidation events
    countShort: number;// Count of short liquidation events
    maxLong: number;   // Max single long liquidation
    maxShort: number;  // Max single short liquidation
  };
  
  last_price: number;  // Last known price
  
  // Status Flags
  isClosed: boolean;    // Soft-close (Binance kline closed)
  isFinalized: boolean; // Hard-close (Data frozen, used for :closed streams)
}
```

## Client Examples (JS)

### Basic Live Update
```javascript
const socket = io('http://localhost:3000');

socket.emit('subscribe', '1m:live');

socket.on('candle', (data) => {
  console.log('Live Candle:', data.symbol, data.c);
});
```

### Closed Candles Only (For Strategy)
```javascript
const socket = io('http://localhost:3000');

// Subscribe only to closed (finalized) candles
socket.emit('subscribe', '5m:closed');

socket.on('candle', (data) => {
  if (data.isFinalized) {
    console.log('New closed candle for strategy:', data.tf, data.c);
  }
});
```

---

> [!TIP]
> **Performance Optimization**
> For high-frequency trading (UI), use `:live`. 
> For data analysis and machine learning, always use `:closed` to avoid processing intermediate updates.
