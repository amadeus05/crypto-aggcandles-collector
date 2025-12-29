import axios from 'axios';
import { CONFIG } from '../config';

export class MarketUniverse {
  private symbols: string[] = [];
  
  public async initialize(): Promise<string[]> {
    console.log('[Universe] Initializing market universe...');
    
    // 1. Fetch Basic Info
    const infoRes = await axios.get('https://fapi.binance.com/fapi/v1/exchangeInfo');
    let valid = (infoRes.data.symbols || []).filter((s: any) => 
      s.contractType === 'PERPETUAL' && 
      s.marginAsset === 'USDT' && 
      s.status === 'TRADING'
    );

    // 2. Whitelist / Blacklist
    const whitelist = new Set(CONFIG.EXCHANGE.WHITELIST);
    const blacklist = new Set(CONFIG.EXCHANGE.BLACKLIST);

    if (whitelist.size > 0) {
      valid = valid.filter((s: any) => whitelist.has(s.symbol));
    }
    if (blacklist.size > 0) {
      valid = valid.filter((s: any) => !blacklist.has(s.symbol));
    }

    // 3. Sort by Volume if TOP mode
    if (CONFIG.EXCHANGE.SYMBOL_MODE === 'TOP') {
      try {
        const tickerRes = await axios.get('https://fapi.binance.com/fapi/v1/ticker/24hr');
        
        // Explicit Generic Type <string, number> fixes the arithmetic error
        const volMap = new Map<string, number>(
          tickerRes.data.map((t: any) => [t.symbol, parseFloat(t.quoteVolume)])
        );
        
        valid.sort((a: any, b: any) => {
          const volA = volMap.get(a.symbol) ?? 0;
          const volB = volMap.get(b.symbol) ?? 0;
          return volB - volA;
        });

        valid = valid.slice(0, CONFIG.EXCHANGE.TOP_LIMIT);
      } catch (e) {
        console.warn('[Universe] Failed to sort by volume, using unsorted list');
      }
    }

    this.symbols = valid.map((s: any) => s.symbol);
    console.log(`[Universe] Finalized: ${this.symbols.length} symbols (Hash: ${this.getHash().slice(0, 8)})`);
    return this.symbols;
  }

  public getSymbols(): string[] {
    return this.symbols;
  }

  // Для проверки консистентности (можно писать в логи)
  public getHash(): string {
    return Buffer.from(this.symbols.join(',')).toString('base64');
  }
}