import { SmartCandleRow } from '../domain/types';
import { CONFIG } from '../config';

export class CandleAggregator {
  private buffer = new Map<string, SmartCandleRow>();
  
  constructor(private next: (row: SmartCandleRow) => void) {}

  public push(row: SmartCandleRow) {
    if (CONFIG.AGGREGATION.INTERVAL === '1m') {
      this.next(row);
      return;
    }

    // Logic for 5m aggregation (bucket calculation)
    const bucketSize = 5 * 60000;
    const bucketTs = Math.floor(row.ts / bucketSize) * bucketSize;
    const key = `${row.symbol}:${bucketTs}`;
    
    let agg = this.buffer.get(key);
    if (!agg) {
      // Flush previous bucket for this symbol if needed (simplified)
      // In production, use a time-wheel or periodic flush to ensure old buckets are emitted
      agg = { ...row, ts: bucketTs };
      this.buffer.set(key, agg);
    } else {
      // Merge logic (High/Low/Vol sum/Liq sum)
      agg.h = Math.max(agg.h, row.h);
      agg.l = Math.min(agg.l, row.l);
      agg.c = row.c;
      agg.v += row.v;
      agg.delta += row.delta;
      agg.cvd = row.cvd; // Always latest
      agg.oi = row.oi;   // Always latest
      // Merge liquidations...
    }
    
    // Check if this 1m candle is the last in the 5m bucket
    const isBucketEnd = (row.ts + 60000) >= (bucketTs + bucketSize);
    if (isBucketEnd) {
      this.next(agg);
      this.buffer.delete(key);
    }
  }
}