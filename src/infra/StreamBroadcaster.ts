import { Server as SocketIOServer } from 'socket.io';
import { Server as HttpServer } from 'http';
import { SmartCandleRow } from '../domain/types';

export type Timeframe = '1m' | '5m' | '15m';

export class StreamBroadcaster {
  private io: SocketIOServer;

  constructor(httpServer: HttpServer) {
    this.io = new SocketIOServer(httpServer, {
      cors: {
        origin: "*",
        methods: ["GET", "POST"]
      }
    });

    this.io.on('connection', (socket) => {
      console.log(`[Stream] Client connected: ${socket.id}`);

      // Подписка на таймфрейм
      socket.on('subscribe', (input: string | string[]) => {
        const topics = Array.isArray(input) ? input : [input];
        for (const topic of topics) {
          // Backward compatibility: '1m' joins ONLY '1m' room (gets all updates)
          if (['1m', '5m', '15m'].includes(topic)) {
            socket.join(topic);
            console.log(`[Stream] ${socket.id} joined legacy room: ${topic}`);
            continue;
          }

          // Global rooms: 'live' or 'closed'
          if (['live', 'closed'].includes(topic)) {
            socket.join(topic);
            console.log(`[Stream] ${socket.id} joined global ${topic} room`);
            continue;
          }

          // New format: '1m:live' or '1m:closed'
          const [tf, type] = topic.split(':');
          if (['1m', '5m', '15m'].includes(tf) && ['live', 'closed'].includes(type)) {
            socket.join(topic);
            console.log(`[Stream] ${socket.id} joined ${type} room: ${topic}`);
          }
        }
      });

      // Отписка от таймфрейма
      socket.on('unsubscribe', (input: string | string[]) => {
        const topics = Array.isArray(input) ? input : [input];
        for (const topic of topics) {
          socket.leave(topic);
          if (['1m', '5m', '15m'].includes(topic)) {
            socket.leave(`${topic}:live`);
          }
          console.log(`[Stream] ${socket.id} unsubscribed from ${topic}`);
        }
      });

      // Подписка на символ
      socket.on('subscribe:symbol', (pattern: string) => {
        // pattern can be 'BTCUSDT', 'BTCUSDT:live', 'BTCUSDT:closed'
        const parts = pattern.split(':');
        const symbol = parts[0];
        const type = parts[1]; // undefined, 'live', or 'closed'

        if (!type) {
          // Legacy symbol room
          socket.join(`symbol:${symbol}`);
          console.log(`[Stream] ${socket.id} joined legacy symbol room: ${symbol}`);
        } else if (['live', 'closed'].includes(type)) {
          socket.join(`symbol:${symbol}:${type}`);
          console.log(`[Stream] ${socket.id} joined ${type} symbol room: ${symbol}`);
        }
      });

      socket.on('unsubscribe:symbol', (pattern: string) => {
        const parts = pattern.split(':');
        const symbol = parts[0];
        const type = parts[1];

        if (!type) {
          socket.leave(`symbol:${symbol}`);
        } else {
          socket.leave(`symbol:${symbol}:${type}`);
        }
      });

      socket.on('disconnect', () => {
        // console.log(`[Stream] Client disconnected: ${socket.id}`);
      });
    });
  }

  /**
   * Отправляет свечу в соответствующие комнаты на основе таймфрейма и состояния
   */
  public broadcastTF(row: SmartCandleRow, tf: Timeframe) {
    const payload = { ...row, tf };
    const { symbol } = row;

    // 1. LEGACY ROOMS: Всегда получают всё (совместимость)
    this.io.to(tf).emit('candle', payload);
    this.io.to(`symbol:${symbol}`).emit('candle', payload);

    // 2. LIVE ROOMS: Всегда получают всё (детальный стрим)
    this.io.to('live').emit('candle', payload); // Global live
    this.io.to(`${tf}:live`).emit('candle', payload);
    this.io.to(`symbol:${symbol}:live`).emit('candle', payload);

    // 3. CLOSED ROOMS: Только если данные финализированы (для стратегий)
    if (row.isFinalized) {
      this.io.to('closed').emit('candle', payload); // Global closed
      this.io.to(`${tf}:closed`).emit('candle', payload);
      this.io.to(`symbol:${symbol}:closed`).emit('candle', payload);
    }
  }

  /**
   * @deprecated Используйте broadcastTF для соблюдения семантики стримов.
   * Данный метод обходит фильтры live/closed.
   */
  public broadcast(row: SmartCandleRow) {
    console.warn('[Stream] Warning: broadcast() is deprecated. Use broadcastTF() instead.');
    this.io.emit('candle', row);
  }

  public shutdown() {
    this.io.close();
  }
}
