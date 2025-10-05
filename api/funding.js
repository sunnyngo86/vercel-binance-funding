const ccxt = require('ccxt');

module.exports = async (req, res) => {
  const BINANCE_API_KEY = process.env.BINANCE_API_KEY;
  const BINANCE_API_SECRET = process.env.BINANCE_API_SECRET;
  const PHEMEX_API_KEY = process.env.PHEMEX_API_KEY;
  const PHEMEX_API_SECRET = process.env.PHEMEX_API_SECRET;
  const BYBIT_API_KEY = process.env.BYBIT_API_KEY;
  const BYBIT_API_SECRET = process.env.BYBIT_API_SECRET;
  const MEXC_API_KEY = process.env.MEXC_API_KEY;
  const MEXC_API_SECRET = process.env.MEXC_API_SECRET;

  const result = [];
  const equityOverview = {};

  function toSGTime(ts) {
    return new Date(ts).toLocaleString('en-SG', { timeZone: 'Asia/Singapore' });
  }

  // helper: return current price, unrealized PnL, and position value
  async function getPnLAndValue(exchange, pos) {
    try {
      const ticker = await exchange.fetchTicker(pos.symbol);
      const currentPrice = ticker.last || 0;
      const avgPrice = pos.entryPrice || pos.entry_price || 0;
      const amount = Math.abs(pos.contracts || pos.positionAmt || 0);
      const side = pos.side || (pos.contracts > 0 ? 'long' : 'short');

      let pnl = (currentPrice - avgPrice) * amount;
      if (side.toLowerCase().includes('short')) {
        pnl = (avgPrice - currentPrice) * amount;
      }

      const positionValue = amount * currentPrice;
      return { unrealizedPnl: pnl, positionValue, currentPrice };
    } catch (err) {
      console.error(`❌ Failed to fetch ticker for ${pos.symbol}:`, err.message);
      return { unrealizedPnl: 0, positionValue: 0, currentPrice: 0 };
    }
  }

  try {
    const now = Date.now();
    const oneDayAgo = now - 24.001 * 60 * 60 * 1000;

    // === BINANCE ===
    const binance = new ccxt.binance({
      apiKey: BINANCE_API_KEY,
      secret: BINANCE_API_SECRET,
      enableRateLimit: true,
      options: { defaultType: 'future' },
    });

    await binance.loadMarkets();
    const openBinance = (await binance.fetchPositions()).filter(p => p.contracts && p.contracts > 0);

    for (const pos of openBinance) {
      const symbol = pos.symbol;
      const cleanSymbol = symbol.replace('/USDT:USDT', '');
      let seen = new Set();
      let allFunding = [];
      let startTime = oneDayAgo;
      const endTime = now;

      while (startTime < endTime) {
        const data = await binance.fetchFundingHistory(symbol, startTime, 1000, {
          incomeType: 'FUNDING_FEE',
          startTime,
          endTime,
        });
        if (!data?.length) break;
        for (const f of data) {
          const key = `${f.timestamp}-${f.amount}`;
          if (!seen.has(key)) {
            seen.add(key);
            allFunding.push(f);
          }
        }
        const last = data[data.length - 1].timestamp;
        if (last <= startTime) break;
        startTime = last + 1;
        await new Promise(r => setTimeout(r, binance.rateLimit));
      }

      const total = allFunding.reduce((sum, f) => sum + parseFloat(f.amount), 0);
      const { unrealizedPnl, positionValue, currentPrice } = await getPnLAndValue(binance, pos);

      result.push({
        source: 'binance',
        symbol: cleanSymbol,
        currentPrice,
        positionSize: pos.contracts,
        positionValue,
        unrealizedPnl,
        count: allFunding.length,
        totalFunding: total,
        startTime: toSGTime(oneDayAgo),
        endTime: toSGTime(now),
      });
    }

    const binanceFutures = await binance.fetchBalance({ type: 'future' });
    const binanceFunding = await binance.fetchBalance({ type: 'funding' });
    const binanceFuturesEquity = parseFloat(binanceFutures.info.totalMarginBalance || 0);
    const binanceFundingEquity = parseFloat(binanceFunding.free?.USDT || 0);
    equityOverview.binance = {
      futures: binanceFuturesEquity,
      funding: binanceFundingEquity,
      total: binanceFuturesEquity + binanceFundingEquity,
    };

    // === PHEMEX ===
    const phemex = new ccxt.phemex({
      apiKey: PHEMEX_API_KEY,
      secret: PHEMEX_API_SECRET,
      enableRateLimit: true,
      options: { defaultType: 'swap' },
    });

    await phemex.loadMarkets();
    const openPhemex = (await phemex.fetch_positions()).filter(p => p.contracts && p.contracts > 0);

    for (const pos of openPhemex) {
      const symbol = pos.symbol;
      const cleanSymbol = symbol.replace('/USDT:USDT', '');
      let offset = 0;
      const limit = 200;
      let seen = new Set();
      let allFunding = [];

      while (offset < 1000) {
        const data = await phemex.fetchFundingHistory(symbol, undefined, limit, { limit, offset });
        if (!data?.length) break;
        for (const f of data) {
          if (f.timestamp >= oneDayAgo && f.timestamp <= now) {
            const key = `${f.timestamp}-${f.amount}`;
            if (!seen.has(key)) {
              seen.add(key);
              allFunding.push(f);
            }
          }
        }
        if (data.length < limit) break;
        offset += limit;
        await new Promise(r => setTimeout(r, phemex.rateLimit));
      }

      const total = allFunding.reduce((sum, f) => sum + parseFloat(f.amount) * -1, 0);
      const { unrealizedPnl, positionValue, currentPrice } = await getPnLAndValue(phemex, pos);

      result.push({
        source: 'phemex',
        symbol: cleanSymbol,
        currentPrice,
        positionSize: pos.contracts,
        positionValue,
        unrealizedPnl,
        count: allFunding.length,
        totalFunding: total,
        startTime: toSGTime(oneDayAgo),
        endTime: toSGTime(now),
      });
    }

    const phemexBalance = await phemex.fetchBalance();
    const phemexEquity = parseFloat(phemexBalance.info.data.account.accountBalanceRv || 0);
    const phemexUnrealized = result.filter(r => r.source === 'phemex')
      .reduce((sum, r) => sum + (r.unrealizedPnl || 0), 0);
    equityOverview.phemex = {
      futures: phemexEquity,
      funding: 0,
      total: phemexEquity + phemexUnrealized,
    };

    // === BYBIT ===
    const bybit = new ccxt.bybit({
      apiKey: BYBIT_API_KEY,
      secret: BYBIT_API_SECRET,
      enableRateLimit: true,
      options: { defaultType: 'swap' },
    });

    await bybit.loadMarkets();
    const openBybit = (await bybit.fetchPositions()).filter(p => p.contracts && p.contracts > 0);

    for (const pos of openBybit) {
      const symbol = pos.symbol;
      const cleanSymbol = symbol.replace('/USDT:USDT', '');
      let seen = new Set();
      let allFunding = [];
      let currentStart = oneDayAgo;
      const currentEnd = now;

      while (currentStart < currentEnd) {
        const fundings = await bybit.fetchFundingHistory(symbol, currentStart, 100, {
          startTime: currentStart,
          endTime: currentEnd,
        });
        if (!fundings?.length) break;
        for (const f of fundings) {
          const key = `${f.timestamp}-${f.amount}`;
          if (!seen.has(key)) {
            seen.add(key);
            allFunding.push(f);
          }
        }
        const lastTs = fundings.at(-1).timestamp;
        if (lastTs <= currentStart) break;
        currentStart = lastTs + 1;
        await new Promise(r => setTimeout(r, 500));
      }

      const total = allFunding.reduce(
        (sum, f) => sum + parseFloat(f.info?.execFee || f.amount || 0) * -1,
        0
      );
      const { unrealizedPnl, positionValue, currentPrice } = await getPnLAndValue(bybit, pos);

      result.push({
        source: 'bybit',
        symbol: cleanSymbol,
        currentPrice,
        positionSize: pos.contracts,
        positionValue,
        unrealizedPnl,
        count: allFunding.length,
        totalFunding: total,
        startTime: toSGTime(oneDayAgo),
        endTime: toSGTime(now),
      });
    }

    const bybitFutures = await bybit.fetchBalance({ type: 'swap' });
    const bybitFunding = await bybit.fetchBalance({ type: 'funding' });
    const bybitFuturesEquity = parseFloat(bybitFutures.info.result.list?.[0]?.totalEquity || 0);
    const bybitFundingEquity = parseFloat(
      bybitFunding.info.result.balance?.find(b => b.coin === 'USD')?.walletBalance || 0
    );
    equityOverview.bybit = {
      futures: bybitFuturesEquity,
      funding: bybitFundingEquity,
      total: bybitFuturesEquity + bybitFundingEquity,
    };

    // === MEXC ===
    const mexc = new ccxt.mexc({
      apiKey: MEXC_API_KEY,
      secret: MEXC_API_SECRET,
      enableRateLimit: true,
      options: { defaultType: 'swap' },
    });

    await mexc.loadMarkets();
    const openMexc = (await mexc.fetch_positions()).filter(p => p.contracts && p.contracts > 0);

    for (const pos of openMexc) {
      const symbol = pos.symbol;
      const cleanSymbol = symbol.replace('/USDT:USDT', '');
      let page = 1;
      let seen = new Set();
      let allFunding = [];

      while (true) {
        const data = await mexc.fetchFundingHistory(symbol, undefined, 100, {
          page_num: page,
          page_size: 100,
        });
        if (!data?.length) break;
        for (const f of data) {
          if (f.timestamp >= oneDayAgo && f.timestamp <= now) {
            const key = `${f.timestamp}-${f.amount}`;
            if (!seen.has(key)) {
              seen.add(key);
              allFunding.push(f);
            }
          }
        }
        if (data.length < 100) break;
        page++;
        await new Promise(r => setTimeout(r, mexc.rateLimit));
      }

      const total = allFunding.reduce((sum, f) => sum + parseFloat(f.amount), 0);
      const { unrealizedPnl, positionValue, currentPrice } = await getPnLAndValue(mexc, pos);

      result.push({
        source: 'mexc',
        symbol: cleanSymbol,
        currentPrice,
        positionSize: pos.contracts,
        positionValue,
        unrealizedPnl,
        count: allFunding.length,
        totalFunding: total,
        startTime: toSGTime(oneDayAgo),
        endTime: toSGTime(now),
      });
    }

    const mexcBalance = await mexc.fetchBalance();
    const mexcUSDT = mexcBalance.info.data.find(c => c.currency === 'USDT');
    const mexcEquity = parseFloat(mexcUSDT?.equity || 0);
    equityOverview.mexc = {
      futures: mexcEquity,
      funding: 0,
      total: mexcEquity,
    };

    // === TOTAL EQUITY ===
    const totalEquity = Object.values(equityOverview).reduce(
      (sum, ex) => sum + (ex.total || 0),
      0
    );

    res.status(200).json({ success: true, result, equityOverview, totalEquity });
  } catch (e) {
    console.error('❌ Funding API error:', e);
    res.status(500).json({ error: e.message });
  }
};
