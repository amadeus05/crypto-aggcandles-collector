import { SmartCandleRow } from '../domain/types';

type TF = 5 | 15;

interface Bucket {
    row: SmartCandleRow;
    cvdStart: number;      // CVD в начале TF
    cvdEnd: number;        // CVD в конце TF (обновляется)
    lastUpdate: number;
}

export class TimeframeAggregator {
    private buckets = new Map<string, Bucket>();
    private watermarks = new Map<string, number>();  // symbol -> lastFinalizedBucketTs
    private readonly GRACE_MS = 2000;
    private flushIntervalId: NodeJS.Timeout;
    private cleanupIntervalId: NodeJS.Timeout;

    constructor(
        private readonly tf: TF,
        private readonly onEmit: (row: SmartCandleRow) => void
    ) {
        // Проверка на flush каждую секунду
        this.flushIntervalId = setInterval(() => this.flushExpired(), 1000);
        // Очистка стейлых watermarks каждый час
        this.cleanupIntervalId = setInterval(() => this.cleanupWatermarks(), 3600_000);
    }

    push(min1: SmartCandleRow) {
        // Только финализированные 1m свечи
        if (!min1.isFinalized) return;

        const bucketTs = this.align(min1.ts);
        const symbol = min1.symbol;
        const key = `${symbol}:${bucketTs}`;

        // Watermark защита: не обрабатывать свечи для уже финализированных buckets
        const lastFinalized = this.watermarks.get(symbol) ?? 0;
        if (bucketTs <= lastFinalized) {
            // Эта минута относится к уже финализированному bucket — игнорируем
            return;
        }

        let bucket = this.buckets.get(key);

        if (!bucket) {
            // Создаём новый bucket
            const cvdStart = min1.cvd - min1.delta;  // CVD в начале минуты
            bucket = {
                row: this.createFrom(min1, bucketTs),
                cvdStart,
                cvdEnd: min1.cvd,
                lastUpdate: Date.now()
            };
            this.buckets.set(key, bucket);
        } else {
            // Мержим в существующий bucket
            this.merge(bucket, min1);
            bucket.lastUpdate = Date.now();
        }
    }

    private flushExpired() {
        const now = Date.now();
        const tfMs = this.tf * 60_000;

        for (const [key, bucket] of this.buckets) {
            if (now >= bucket.row.ts + tfMs + this.GRACE_MS) {
                // Финализируем bucket
                bucket.row.isFinalized = true;
                bucket.row.cvd = bucket.cvdEnd;  // Финальный CVD

                this.onEmit(bucket.row);

                // Обновляем watermark
                this.watermarks.set(bucket.row.symbol, bucket.row.ts);

                this.buckets.delete(key);
            }
        }
    }

    private cleanupWatermarks() {
        // Удаляем watermarks старше 1 часа
        const cutoff = Date.now() - 3600_000;
        const tfMs = this.tf * 60_000;

        for (const [symbol, ts] of this.watermarks) {
            if (ts + tfMs < cutoff) {
                this.watermarks.delete(symbol);
            }
        }
    }

    private align(ts: number): number {
        const tfMs = this.tf * 60_000;
        return Math.floor(ts / tfMs) * tfMs;
    }

    private createFrom(src: SmartCandleRow, ts: number): SmartCandleRow {
        return {
            symbol: src.symbol,
            ts,
            o: src.o,
            h: src.h,
            l: src.l,
            c: src.c,
            v: src.v,
            quote_v: src.quote_v,
            delta: src.delta,
            cvd: 0,  // Будет установлен при finalize
            oi: src.oi,
            funding: src.funding,
            liquidations: {
                long: src.liquidations.long,
                short: src.liquidations.short,
                countLong: src.liquidations.countLong,
                countShort: src.liquidations.countShort,
                maxLong: src.liquidations.maxLong || -Infinity,
                maxShort: src.liquidations.maxShort || -Infinity
            },
            last_price: src.last_price,
            isClosed: false,
            isFinalized: false
        };
    }

    private merge(bucket: Bucket, src: SmartCandleRow) {
        const dst = bucket.row;

        dst.h = Math.max(dst.h, src.h);
        dst.l = Math.min(dst.l, src.l);
        dst.c = src.c;
        dst.v += src.v;
        dst.quote_v += src.quote_v;
        dst.delta += src.delta;

        // CVD: обновляем только конечную точку
        bucket.cvdEnd = src.cvd;

        // OI / Funding: последние известные
        dst.oi = src.oi;
        dst.funding = src.funding;
        dst.last_price = src.last_price;

        // Liquidations merge
        dst.liquidations.long += src.liquidations.long;
        dst.liquidations.short += src.liquidations.short;
        dst.liquidations.countLong += src.liquidations.countLong;
        dst.liquidations.countShort += src.liquidations.countShort;
        dst.liquidations.maxLong = Math.max(
            dst.liquidations.maxLong === -Infinity ? 0 : dst.liquidations.maxLong,
            src.liquidations.maxLong
        );
        dst.liquidations.maxShort = Math.max(
            dst.liquidations.maxShort === -Infinity ? 0 : dst.liquidations.maxShort,
            src.liquidations.maxShort
        );
    }

    shutdown() {
        clearInterval(this.flushIntervalId);
        clearInterval(this.cleanupIntervalId);

        // Финализируем все оставшиеся buckets
        for (const [key, bucket] of this.buckets) {
            bucket.row.isFinalized = true;
            bucket.row.cvd = bucket.cvdEnd;
            this.onEmit(bucket.row);
        }

        this.buckets.clear();
        this.watermarks.clear();
    }
}
