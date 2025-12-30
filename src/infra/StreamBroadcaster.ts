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
      socket.on('subscribe', (tf: Timeframe | Timeframe[]) => {
        const tfs = Array.isArray(tf) ? tf : [tf];
        for (const t of tfs) {
          if (['1m', '5m', '15m'].includes(t)) {
            socket.join(t);
            console.log(`[Stream] ${socket.id} subscribed to ${t}`);
          }
        }
      });

      // Отписка от таймфрейма
      socket.on('unsubscribe', (tf: Timeframe | Timeframe[]) => {
        const tfs = Array.isArray(tf) ? tf : [tf];
        for (const t of tfs) {
          socket.leave(t);
          console.log(`[Stream] ${socket.id} unsubscribed from ${t}`);
        }
      });

      // Подписка на конкретный символ (опционально)
      socket.on('subscribe:symbol', (symbol: string) => {
        socket.join(`symbol:${symbol}`);
      });

      socket.on('unsubscribe:symbol', (symbol: string) => {
        socket.leave(`symbol:${symbol}`);
      });

      socket.on('disconnect', () => {
        // console.log(`[Stream] Client disconnected: ${socket.id}`);
      });
    });
  }

  /**
   * Отправляет свечу в room конкретного таймфрейма
   */
  public broadcastTF(row: SmartCandleRow, tf: Timeframe) {
    this.io.to(tf).emit('candle', { ...row, tf });
    // Также в room символа (если кто-то подписан)
    this.io.to(`symbol:${row.symbol}`).emit('candle', { ...row, tf });
  }

  /**
   * Отправляет всем (для backwards compatibility или live-ticks)
   */
  public broadcast(row: SmartCandleRow) {
    this.io.emit('candle', row);
  }

  public shutdown() {
    this.io.close();
  }
}
