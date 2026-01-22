import { StreamBroadcaster, Timeframe } from './infra/StreamBroadcaster';
import { SmartCandleRow } from './domain/types';
import { createServer } from 'http';
import { io as ClientIO } from 'socket.io-client';

async function test() {
    const server = createServer();
    const broadcaster = new StreamBroadcaster(server);
    server.listen(4001);

    const clientLegacy = ClientIO('http://localhost:4001');
    const clientClosedSymbol = ClientIO('http://localhost:4001');

    let legacyCount = 0;
    let closedSymbolCount = 0;

    clientLegacy.on('connect', () => {
        clientLegacy.emit('subscribe', '1m');
    });

    clientClosedSymbol.on('connect', () => {
        clientClosedSymbol.emit('subscribe:symbol', 'BTCUSDT:closed');
    });

    clientLegacy.on('candle', () => legacyCount++);
    clientClosedSymbol.on('candle', () => closedSymbolCount++);

    const mockCandle = (isFinalized: boolean): SmartCandleRow => ({
        symbol: 'BTCUSDT',
        ts: Date.now(),
        o: 50000, h: 51000, l: 49000, c: 50500,
        v: 10, quote_v: 500000,
        delta: 1, cvd: 100,
        oi: 1000, funding: 0.01,
        liquidations: { long: 0, short: 0, countLong: 0, countShort: 0, maxLong: 0, maxShort: 0 },
        last_price: 50500,
        isClosed: isFinalized,
        isFinalized: isFinalized
    });

    // Wait for connections
    await new Promise(r => setTimeout(r, 1000));

    console.log('--- TEST 1: Live Update (isFinalized: false) ---');
    broadcaster.broadcastTF(mockCandle(false), '1m');
    await new Promise(r => setTimeout(r, 500));
    console.log(`Legacy count: ${legacyCount} (expected 1, NO DUPLICATE)`);
    console.log(`Closed Symbol count: ${closedSymbolCount} (expected 0, NO LEAK)`);

    console.log('\n--- TEST 2: Finalized Update (isFinalized: true) ---');
    broadcaster.broadcastTF(mockCandle(true), '1m');
    await new Promise(r => setTimeout(r, 500));
    console.log(`Legacy count: ${legacyCount} (expected 2)`);
    console.log(`Closed Symbol count: ${closedSymbolCount} (expected 1)`);

    // Cleanup
    clientLegacy.close();
    clientClosedSymbol.close();
    broadcaster.shutdown();
    server.close();

    if (legacyCount === 2 && closedSymbolCount === 1) {
        console.log('\nFIX VERIFICATION SUCCESSFUL!');
        process.exit(0);
    } else {
        console.error('\nFIX VERIFICATION FAILED!');
        process.exit(1);
    }
}

test();
