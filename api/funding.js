const ccxt = require('ccxt');

const FUNDING_WINDOW_MS = 120.01 * 60 * 60 * 1000; // 5 天
const FUNDING_PAGE_LIMIT = 100;
const FUNDING_MAX_PAGES = 8; // 5天/8h = 15 条，多翻几页留 buffer
const NEGATIVE_FUNDING_SIGN = new Set(['phemex', 'bybit']);
const COINS = ['USDT', 'USDC'];

const toSGTime = (ts) =>
  new Date(ts).toLocaleString('en-SG', { timeZone: 'Asia/Singapore' });

const cleanSymbol = (s) => {
  if (!s) return s;
  let out = s;
  if (out.includes('/')) out = out.split('/')[0];
  if (out.includes(':')) out = out.split(':')[0];
  return out;
};

const convertMexcOrderSide = (code) => {
  if (code === '1' || code === 1 || code === '3' || code === 3) return 'buy';
  if (code === '2' || code === 2 || code === '4' || code === 4) return 'sell';
  return code;
};

const num = (v) => parseFloat(v || 0) || 0;

// 把 CCXT/网络错误归类成人能看懂的简短描述
// 返回 { type, message } —— type 用于前端配色，message 是给人看的解释
function classifyError(err, exchangeName) {
  const name = err?.constructor?.name || err?.name || '';
  const raw = String(err?.message || err || '').slice(0, 200);
  const ex = exchangeName ? exchangeName.toUpperCase() : 'Exchange';

  // CCXT 的错误类名很有用，优先按类名判断
  if (/AuthenticationError|PermissionDenied/i.test(name) ||
      /api[\s_-]?key|signature|invalid.*key|unauthorized|permission|passphrase|apikey/i.test(raw)) {
    return { type: 'auth', message: `${ex} 认证失败 (API key 过期/无效/权限不足)` };
  }
  if (/RateLimitExceeded|DDoSProtection/i.test(name) ||
      /rate limit|too many|too much|429|frequenc/i.test(raw)) {
    return { type: 'ratelimit', message: `${ex} 请求过于频繁 (rate limit / too many requests)` };
  }
  if (/RequestTimeout|ETIMEDOUT|ENOTFOUND|ECONNRESET|NetworkError|ExchangeNotAvailable/i.test(name) ||
      /timeout|timed out|network|ENOTFOUND|ECONNRESET|getaddrinfo|socket hang/i.test(raw)) {
    return { type: 'network', message: `${ex} 网络超时/无法连接` };
  }
  if (/InvalidNonce/i.test(name) || /nonce|timestamp|recv.?window|time.*sync/i.test(raw)) {
    return { type: 'time', message: `${ex} 时间戳/nonce 错误 (服务器时间不同步)` };
  }
  if (/AccountSuspended|AccountNotEnabled/i.test(name) || /suspend|frozen|disabled|not enabled/i.test(raw)) {
    return { type: 'account', message: `${ex} 账户被冻结/未启用` };
  }
  if (/ExchangeError/i.test(name)) {
    return { type: 'exchange', message: `${ex} 交易所返回错误: ${raw}` };
  }
  // 兜底：原始信息
  return { type: 'unknown', message: `${ex}: ${raw}` };
}

const emptyWallet = () => ({
  futures: { USDT: 0, USDC: 0 },
  spot: { USDT: 0, USDC: 0 },
  funding: { USDT: 0, USDC: 0 },
  total: 0,
});

const sumWallet = (w) => {
  let t = 0;
  for (const bucket of ['futures', 'spot', 'funding']) {
    for (const coin of COINS) t += w[bucket][coin] || 0;
  }
  return t;
};

function buildExchanges() {
  return {
    binance: new ccxt.binance({
      apiKey: process.env.BINANCE_API_KEY,
      secret: process.env.BINANCE_API_SECRET,
      enableRateLimit: true,
      options: { defaultType: 'future', warnOnFetchOpenOrdersWithoutSymbol: false },
    }),
    phemex: new ccxt.phemex({
      apiKey: process.env.PHEMEX_API_KEY,
      secret: process.env.PHEMEX_API_SECRET,
      enableRateLimit: true,
      options: { defaultType: 'swap' },
    }),
    bybit: new ccxt.bybit({
      apiKey: process.env.BYBIT_API_KEY,
      secret: process.env.BYBIT_API_SECRET,
      enableRateLimit: true,
      options: { defaultType: 'swap' },
    }),
    mexc: new ccxt.mexc({
      apiKey: process.env.MEXC_API_KEY,
      secret: process.env.MEXC_API_SECRET,
      enableRateLimit: true,
      options: { defaultType: 'swap' },
    }),
    aster: new ccxt.aster({
      // ⚠️ Aster 是 DEX，CCXT v4.5.52+ 用 V3：要 L1 钱包私钥（0x + 64 hex = 66 字符）
      // 兼容多个变量名：优先 ASTER_PRIVATE_KEY，其次你之前存的 ASTER_API_SECRET
      privateKey: process.env.ASTER_PRIVATE_KEY || process.env.ASTER_API_SECRET,
      enableRateLimit: true,
      // ⚠️ CCXT aster 只支持 'swap' / 'spot'，不支持 'future'（传 future 会让 fetchBalance 崩）
      options: {
        defaultType: 'swap',
        warnOnFetchOpenOrdersWithoutSymbol: false,
      },
    }),
    bitget: new ccxt.bitget({
      apiKey: process.env.BITGET_API_KEY,
      secret: process.env.BITGET_API_SECRET,
      // ⚠️ Bitget 强制要求第三个凭证：passphrase（创建 API key 时自己设的）
      // CCXT 里字段名叫 password。Vercel 需新增 BITGET_API_PASSPHRASE
      password: process.env.BITGET_API_PASSPHRASE,
      enableRateLimit: true,
      options: { defaultType: 'swap' },
    }),
  };
}

// ---------- 余额 ----------
async function fetchBinanceEquity(ex) {
  const w = emptyWallet();
  const [umBal, spotBal, fundingBal] = await Promise.all([
    ex.fetchBalance({ type: 'future' }).catch(() => ({})),
    ex.fetchBalance({ type: 'spot' }).catch(() => ({})),
    ex.fetchBalance({ type: 'funding' }).catch(() => ({})),
  ]);

  const umAssets = umBal?.info?.assets || [];
  for (const a of umAssets) {
    if (a.asset === 'USDT') w.futures.USDT = num(a.marginBalance);
    if (a.asset === 'USDC') w.futures.USDC = num(a.marginBalance);
  }
  if (!w.futures.USDT && !w.futures.USDC) {
    w.futures.USDT = num(umBal?.info?.totalMarginBalance);
  }

  w.spot.USDT = num(spotBal?.total?.USDT);
  w.spot.USDC = num(spotBal?.total?.USDC);
  w.funding.USDT = num(fundingBal?.total?.USDT || fundingBal?.free?.USDT);
  w.funding.USDC = num(fundingBal?.total?.USDC || fundingBal?.free?.USDC);

  w.total = sumWallet(w);
  return w;
}

async function fetchPhemexEquity(ex) {
  const w = emptyWallet();
  const [usdtSwap, usdcSwap, spot] = await Promise.all([
    ex.fetchBalance({ type: 'swap', code: 'USDT' }).catch(() => ({})),
    ex.fetchBalance({ type: 'swap', code: 'USDC' }).catch(() => ({})),
    ex.fetchBalance({ type: 'spot' }).catch(() => ({})),
  ]);

  const parsePhemex = (bal, fallbackCoin) => {
    let v = num(bal?.info?.data?.account?.accountBalanceRv);
    if (!v) {
      const ev = bal?.info?.data?.account?.accountBalanceEv;
      if (ev) v = num(ev) / 1e8;
    }
    if (!v) v = num(bal?.total?.[fallbackCoin]);
    return v;
  };

  w.futures.USDT = parsePhemex(usdtSwap, 'USDT');
  w.futures.USDC = parsePhemex(usdcSwap, 'USDC');
  w.spot.USDT = num(spot?.total?.USDT);
  w.spot.USDC = num(spot?.total?.USDC);

  w.total = sumWallet(w);
  return w;
}

async function fetchBybitEquity(ex) {
  const w = emptyWallet();
  const [unified, fund] = await Promise.all([
    ex.fetchBalance({ type: 'unified' }).catch(() =>
      ex.fetchBalance({ type: 'swap' }).catch(() => ({}))
    ),
    ex.fetchBalance({ type: 'funding' }).catch(() => ({})),
  ]);

  const coinList = unified?.info?.result?.list?.[0]?.coin || [];
  for (const c of coinList) {
    if (c.coin === 'USDT') w.futures.USDT = num(c.equity || c.walletBalance);
    if (c.coin === 'USDC') w.futures.USDC = num(c.equity || c.walletBalance);
  }
  if (!w.futures.USDT && !w.futures.USDC) {
    w.futures.USDT = num(unified?.info?.result?.list?.[0]?.totalEquity);
  }

  const fundList = fund?.info?.result?.balance || [];
  for (const b of fundList) {
    if (b.coin === 'USDT') w.funding.USDT = num(b.walletBalance);
    if (b.coin === 'USDC') w.funding.USDC = num(b.walletBalance);
  }

  w.total = sumWallet(w);
  return w;
}

async function fetchMexcEquity(ex) {
  const w = emptyWallet();
  const [swapBal, spotBal] = await Promise.all([
    ex.fetchBalance({ type: 'swap' }).catch(() => ({})),
    ex.fetchBalance({ type: 'spot' }).catch(() => ({})),
  ]);

  const dataArr = swapBal?.info?.data;
  if (Array.isArray(dataArr)) {
    for (const c of dataArr) {
      if (c.currency === 'USDT') w.futures.USDT = num(c.equity);
      if (c.currency === 'USDC') w.futures.USDC = num(c.equity);
    }
  } else if (dataArr && typeof dataArr === 'object') {
    w.futures.USDT = num(dataArr.equity || dataArr.availableBalance);
  }
  if (!w.futures.USDT && !w.futures.USDC) {
    w.futures.USDT = num(swapBal?.total?.USDT);
    w.futures.USDC = num(swapBal?.total?.USDC);
  }

  w.spot.USDT = num(spotBal?.total?.USDT);
  w.spot.USDC = num(spotBal?.total?.USDC);

  w.total = sumWallet(w);
  return w;
}

// Aster V3 (DEX)：CCXT 只支持 type='swap'/'spot'，不支持 'future'
// swap 余额响应是数组: [{ asset, balance, crossWalletBalance, availableBalance, ... }]
// CCXT parseBalance 会把它转成统一格式，total = balance 字段
async function fetchAsterEquity(ex) {
  const w = emptyWallet();
  const [swapBal, spotBal] = await Promise.all([
    ex.fetchBalance({ type: 'swap' }).catch((e) => {
      console.error('❌ aster swap balance:', e.message);
      return {};
    }),
    ex.fetchBalance({ type: 'spot' }).catch(() => ({})),
  ]);

  // 调试：确认结构（正常后可删）
  console.log('🔍 aster swap total:', JSON.stringify(swapBal?.total || {}));

  // 优先用原始 info 数组里的 balance / crossWalletBalance（更接近"权益"）
  const infoArr = Array.isArray(swapBal?.info) ? swapBal.info : null;
  if (infoArr) {
    for (const b of infoArr) {
      // balance = 钱包余额, crossWalletBalance 通常等于或接近 balance
      const val = num(b.balance || b.crossWalletBalance || b.availableBalance);
      if (b.asset === 'USDT') w.futures.USDT = val;
      if (b.asset === 'USDC') w.futures.USDC = val;
    }
  }
  // fallback: CCXT 统一字段 total
  if (!w.futures.USDT && !w.futures.USDC) {
    w.futures.USDT = num(swapBal?.total?.USDT);
    w.futures.USDC = num(swapBal?.total?.USDC);
  }

  // spot 余额（V3 sapi）
  w.spot.USDT = num(spotBal?.total?.USDT);
  w.spot.USDC = num(spotBal?.total?.USDC);

  w.total = sumWallet(w);
  return w;
}

// Bitget V2 Mix：swap 余额 data[] 数组，每项 { marginCoin, accountEquity, usdtEquity, ... }
// USDT-M 和 USDC-M 是不同 productType，分别查询
async function fetchBitgetEquity(ex) {
  const w = emptyWallet();
  const [usdtSwap, usdcSwap, spotBal] = await Promise.all([
    // 默认 productType = USDT-FUTURES
    ex.fetchBalance({ type: 'swap' }).catch((e) => {
      console.error('❌ bitget USDT swap balance:', e.message);
      return {};
    }),
    // USDC 本位合约要显式传 productType
    ex.fetchBalance({ type: 'swap', productType: 'USDC-FUTURES' }).catch(() => ({})),
    ex.fetchBalance({ type: 'spot' }).catch(() => ({})),
  ]);

  // 调试：首次部署确认结构（正常后可删）
  console.log('🔍 bitget swap total:', JSON.stringify(usdtSwap?.total || {}));

  // 原始 info.data[] 里 accountEquity 是该保证金币种的账户权益（含未实现盈亏）
  const parseMixList = (bal, coin) => {
    const list = bal?.info?.data;
    if (Array.isArray(list)) {
      const entry = list.find((d) => d.marginCoin === coin);
      if (entry) return num(entry.accountEquity || entry.usdtEquity || entry.available);
    }
    return 0;
  };

  w.futures.USDT = parseMixList(usdtSwap, 'USDT');
  w.futures.USDC = parseMixList(usdcSwap, 'USDC');

  // fallback: CCXT 统一字段
  if (!w.futures.USDT) w.futures.USDT = num(usdtSwap?.total?.USDT);
  if (!w.futures.USDC) w.futures.USDC = num(usdcSwap?.total?.USDC);

  w.spot.USDT = num(spotBal?.total?.USDT);
  w.spot.USDC = num(spotBal?.total?.USDC);

  w.total = sumWallet(w);
  return w;
}

const BALANCE_FETCHERS = {
  binance: fetchBinanceEquity,
  phemex: fetchPhemexEquity,
  bybit: fetchBybitEquity,
  mexc: fetchMexcEquity,
  aster: fetchAsterEquity,
  bitget: fetchBitgetEquity,
};

// ---------- Ticker ----------
async function buildTickerCache(exchange, symbols) {
  if (!symbols.length) return {};
  try {
    const tickers = await exchange.fetchTickers(symbols);
    return tickers || {};
  } catch {
    const entries = await Promise.all(
      symbols.map(async (s) => {
        try { return [s, await exchange.fetchTicker(s)]; }
        catch { return [s, null]; }
      })
    );
    return Object.fromEntries(entries.filter(([, t]) => t));
  }
}

function computePnL(pos, ticker, positionSize) {
  const currentPrice = ticker?.last || 0;
  const avgPrice = pos.entryPrice || pos.entry_price || 0;
  const amount = Math.abs(positionSize || pos.contracts || pos.positionAmt || 0);
  const side = pos.side || (pos.contracts > 0 ? 'long' : 'short');

  let pnl = (currentPrice - avgPrice) * amount;
  if (String(side).toLowerCase().includes('short')) {
    pnl = (avgPrice - currentPrice) * amount;
  }
  return { unrealizedPnl: pnl, positionValue: currentPrice * amount, currentPrice, side };
}

// ---------- Funding ----------
async function fetchFundingWindow(exchange, symbol, sinceMs, nowMs) {
  const seen = new Set();
  const all = [];
  let start = sinceMs;

  for (let i = 0; i < FUNDING_MAX_PAGES; i++) {
    let page;
    try { page = await exchange.fetchFundingHistory(symbol, start, FUNDING_PAGE_LIMIT); }
    catch { break; }
    if (!page?.length) break;

    for (const f of page) {
      if (f.timestamp >= sinceMs && f.timestamp <= nowMs) {
        const key = `${f.timestamp}-${f.amount}`;
        if (!seen.has(key)) { seen.add(key); all.push(f); }
      }
    }
    const last = page[page.length - 1]?.timestamp;
    if (!last || last <= start || page.length < FUNDING_PAGE_LIMIT) break;
    start = last + 1;
  }
  all.sort((a, b) => b.timestamp - a.timestamp);
  return all;
}

// ---------- 持仓 ----------
async function processExchangePositions(name, exchange, nowMs, sinceMs) {
  let positions;
  try {
    positions = (name === 'phemex' || name === 'mexc')
      ? await exchange.fetch_positions()
      : await exchange.fetchPositions();
  } catch (err) {
    console.error(`❌ ${name} positions:`, err.message);
    throw err; // 抛给上层，由主流程归类成友好错误信息
  }

  const open = positions.filter((p) => p.contracts && p.contracts > 0);
  if (!open.length) return [];

  const symbols = [...new Set(open.map((p) => p.symbol))];
  const tickerCache = await buildTickerCache(exchange, symbols);
  const signFlip = NEGATIVE_FUNDING_SIGN.has(name) ? -1 : 1;

  const rows = await Promise.all(open.map(async (pos) => {
    const allFunding = await fetchFundingWindow(exchange, pos.symbol, sinceMs, nowMs);
    const totalFunding = allFunding.reduce((s, f) => s + num(f.amount), 0) * signFlip;

    let positionSize = pos.contracts;
    if (name === 'mexc') {
      const market = exchange.markets[pos.symbol];
      const contractSize = market?.contractSize || 1;
      positionSize = (pos.contracts || 0) * contractSize;
    }

    const ticker = tickerCache[pos.symbol];
    const { unrealizedPnl, positionValue, currentPrice, side } = computePnL(pos, ticker, positionSize);

    return {
      source: name,
      symbol: cleanSymbol(pos.symbol),
      rawSymbol: pos.symbol,
      side,
      currentPrice,
      entryPrice: pos.entryPrice || pos.entry_price || 0,
      positionSize,
      positionValue,
      unrealizedPnl,
      count: allFunding.length,
      totalFunding,
      // 纯金额数组（向后兼容现有显示）
      fundingRecords: allFunding.map((f) => num(f.amount) * signFlip),
      // 带时间戳的明细（供前端按 8h/16h/1d/3d/5d 窗口筛选重算）
      fundingDetail: allFunding.map((f) => ({
        ts: f.timestamp,
        amount: num(f.amount) * signFlip,
      })),
      startTime: toSGTime(sinceMs),
      endTime: toSGTime(nowMs),
      windowMs: FUNDING_WINDOW_MS,
      serverNow: nowMs,
    };
  }));

  return rows;
}

// ---------- 订单 ----------
function formatOrder(o, name, exchange) {
  const triggerPrice = num(
    o.triggerPrice || o.stopPrice ||
    o.info?.stopPrice || o.info?.triggerPrice || 0
  );
  const limitPrice = num(o.price);
  const orderType = String(o.type || o.info?.type || 'LIMIT').toUpperCase();

  let kind = 'LIMIT';
  if (/TAKE_PROFIT|TAKEPROFIT/.test(orderType)) kind = 'TP';
  else if (/STOP/.test(orderType)) kind = 'SL';
  else if (triggerPrice && !limitPrice) kind = 'TRIGGER';

  const displayPrice = (kind === 'LIMIT')
    ? (limitPrice || triggerPrice)
    : (triggerPrice || limitPrice);

  // MEXC 合约 amount 是张数 (contracts)；需要 × contractSize 转成币数
  // 保持和 position.positionSize 的换算一致（见 processExchangePositions）
  let amount = num(o.amount || o.info?.origQty || o.info?.quantity || 0);
  if (name === 'mexc' && exchange && o.symbol) {
    const market = exchange.markets?.[o.symbol];
    const contractSize = market?.contractSize;
    if (contractSize && contractSize !== 1) {
      amount = amount * contractSize;
    }
  }

  return {
    exchange: name,
    symbol: cleanSymbol(o.symbol),
    side: name === 'mexc' ? convertMexcOrderSide(o.side) : o.side,
    price: displayPrice,
    triggerPrice,
    limitPrice,
    amount,
    kind,
    orderType,
  };
}

async function processExchangeOrders(name, exchange, positionRows) {
  const results = [];
  try {
    if (name === 'binance') {
      // Binance：条件单 (SL/TP) 需要额外用 stop:true 拉取
      const posSymbols = [...new Set(
        positionRows.filter((p) => p.source === name)
          .map((p) => p.rawSymbol || `${p.symbol}/USDT:USDT`)
      )];

      const perSymbol = await Promise.all(posSymbols.map(async (s) => {
        const [normal, triggers] = await Promise.all([
          exchange.fetchOpenOrders(s).catch(() => []),
          exchange.fetchOpenOrders(s, undefined, undefined, { stop: true }).catch(() => []),
        ]);
        return [...normal, ...triggers];
      }));
      results.push(...perSymbol.flat().map((o) => formatOrder(o, name, exchange)));
    } else if (name === 'phemex' || name === 'aster') {
      // Phemex / Aster(V3)：逐 symbol 查询；V3 OpenOrders 已含 TP/SL，不需 stop:true
      const posSymbols = [...new Set(
        positionRows.filter((p) => p.source === name)
          .map((p) => p.rawSymbol || `${p.symbol}/USDT:USDT`)
      )];
      const perSymbol = await Promise.all(
        posSymbols.map((s) => exchange.fetchOpenOrders(s).catch(() => []))
      );
      results.push(...perSymbol.flat().map((o) => formatOrder(o, name, exchange)));
    } else {
      const openOrders = await exchange.fetchOpenOrders().catch(() => []);
      results.push(...openOrders.map((o) => formatOrder(o, name, exchange)));
    }
  } catch (err) {
    console.error(`❌ ${name} orders:`, err.message);
  }

  // 过滤无效订单（价格和数量都为 0 → 已成交或无效记录）
  return results.filter((o) => (o.price > 0 || o.triggerPrice > 0) && o.amount > 0);
}

function dedupeOrders(orders) {
  const seen = new Set();
  return orders.filter((o) => {
    const key = `${o.exchange}-${o.symbol}-${o.side}-${o.price}-${o.triggerPrice}-${o.amount}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

// ---------- 对冲健康度（3 类） ----------
// Class A: 无 TP/SL 挂单保护
// Class B: 资费 funding 亏损
// Class C: 多空 size 或 orders 数量不对齐（不含无 TP/SL 的重复警告）
function analyzeHedges(result) {
  const bySymbol = {};
  for (const r of result) {
    if (!bySymbol[r.symbol]) bySymbol[r.symbol] = { long: [], short: [] };
    if (r.side === 'long') bySymbol[r.symbol].long.push(r);
    else if (r.side === 'short') bySymbol[r.symbol].short.push(r);
  }

  const noProtection = []; // Class A
  const fundingLoss = []; // Class B
  const misaligned = [];  // Class C

  for (const [symbol, sides] of Object.entries(bySymbol)) {
    const longSize = sides.long.reduce((s, r) => s + Math.abs(r.positionSize), 0);
    const shortSize = sides.short.reduce((s, r) => s + Math.abs(r.positionSize), 0);
    const longOrderCount = sides.long.reduce((s, r) => s + (r.tpSlClose?.length || 0), 0);
    const shortOrderCount = sides.short.reduce((s, r) => s + (r.tpSlClose?.length || 0), 0);
    const totalOrderCount = longOrderCount + shortOrderCount;
    const netFunding =
      sides.long.reduce((s, r) => s + r.totalFunding, 0) +
      sides.short.reduce((s, r) => s + r.totalFunding, 0);

    const allEntries = [...sides.long, ...sides.short];
    const hasAnyOrder = allEntries.some((r) => r.tpSlClose?.length);

    // Class A: 无 TP/SL 挂单保护
    if (!hasAnyOrder && allEntries.length > 0) {
      noProtection.push({
        symbol, longSize, shortSize, netFunding,
        longCount: sides.long.length, shortCount: sides.short.length,
      });
    }

    // Class B: funding 亏损
    if (netFunding < -0.5) {
      fundingLoss.push({
        symbol, longSize, shortSize, netFunding,
        longFunding: sides.long.reduce((s, r) => s + r.totalFunding, 0),
        shortFunding: sides.short.reduce((s, r) => s + r.totalFunding, 0),
      });
    }

    // Class C: size 或 order 不对齐
    // 注意：不再限制"仅当有 TP/SL 保护时才检查"
    // ETH 这种无 TP/SL + Size 不对齐应同时在两个分类里显示
    {
      const hasPair = sides.long.length && sides.short.length;
      const problems = [];

      // 裸露敞口（单边）
      if (!hasPair && allEntries.length > 0) {
        if (sides.long.length) problems.push(`裸多单，无对冲空头`);
        else problems.push(`裸空单，无对冲多头`);
      }

      if (hasPair) {
        // Size 对齐检查：任何绝对差 > 浮点容差都报出来
        const absDiff = Math.abs(longSize - shortSize);
        const sizeDiffPct = absDiff / Math.max(longSize, shortSize, 1);
        if (absDiff > 1e-8) {
          const dp = (longSize < 1 || shortSize < 1) ? 4 : (longSize < 100 ? 2 : 0);
          const lFmt = longSize.toLocaleString('en-US', { minimumFractionDigits: dp, maximumFractionDigits: dp });
          const sFmt = shortSize.toLocaleString('en-US', { minimumFractionDigits: dp, maximumFractionDigits: dp });
          problems.push(
            `Size 不对齐: L=${lFmt} vs S=${sFmt} (Δ${(sizeDiffPct * 100).toFixed(3)}%)`
          );
        }

        // Order 数量对齐检查：只在至少有一边挂单时检查（双方都没挂单不算 "数量不一致"）
        if (hasAnyOrder && longOrderCount !== shortOrderCount) {
          problems.push(
            `Order 数量不对齐: long=${longOrderCount} 单 vs short=${shortOrderCount} 单`
          );
        }
      }

      if (problems.length) {
        misaligned.push({
          symbol, longSize, shortSize,
          longOrderCount, shortOrderCount, totalOrderCount,
          netFunding, problems,
        });
      }
    }
  }

  // 排序：funding 亏损按绝对值大排前，misaligned 按 size 差异大排前
  fundingLoss.sort((a, b) => a.netFunding - b.netFunding);
  misaligned.sort((a, b) => {
    const aDiff = Math.abs(a.longSize - a.shortSize) / Math.max(a.longSize, a.shortSize, 1);
    const bDiff = Math.abs(b.longSize - b.shortSize) / Math.max(b.longSize, b.shortSize, 1);
    return bDiff - aDiff;
  });
  noProtection.sort((a, b) => b.longSize + b.shortSize - (a.longSize + a.shortSize));

  return { noProtection, fundingLoss, misaligned };
}

// ---------- 主 ----------
module.exports = async (req, res) => {
  const t0 = Date.now();
  const exchanges = buildExchanges();
  const nowMs = Date.now();
  const sinceMs = nowMs - FUNDING_WINDOW_MS;
  const exchangeList = Object.entries(exchanges);

  try {
    // 每个交易所独立处理：任一失败只 skip 它自己，记录错误，不影响其他
    const exchangeStatus = {}; // { name: { ok, error, errorType } }

    const perExchangePromises = exchangeList.map(async ([name, exchange]) => {
      // Step 1: loadMarkets —— 失败则整个交易所 skip（后续都依赖 markets）
      try {
        await exchange.loadMarkets();
      } catch (err) {
        const { type, message } = classifyError(err, name);
        console.error(`❌ ${name} loadMarkets:`, err.message);
        exchangeStatus[name] = { ok: false, error: message, errorType: type };
        return { name, equity: emptyWallet(), positions: [], failed: true };
      }

      // Step 2: balance + positions 各自独立容错
      let equityErr = null;
      let posErr = null;

      const [equity, positions] = await Promise.all([
        BALANCE_FETCHERS[name](exchange).catch((err) => {
          equityErr = err;
          console.error(`❌ ${name} balance:`, err.message);
          return emptyWallet();
        }),
        processExchangePositions(name, exchange, nowMs, sinceMs).catch((err) => {
          posErr = err;
          console.error(`❌ ${name} positions:`, err.message);
          return [];
        }),
      ]);

      // 记录状态：balance 或 positions 任一出错就标记（balance 更关键，优先报它）
      const firstErr = equityErr || posErr;
      if (firstErr) {
        const { type, message } = classifyError(firstErr, name);
        exchangeStatus[name] = { ok: false, error: message, errorType: type };
      } else {
        exchangeStatus[name] = { ok: true, error: null, errorType: null };
      }

      return { name, equity, positions, failed: false };
    });

    const perExchange = await Promise.all(perExchangePromises);

    const equityOverview = {};
    const result = [];
    for (const { name, equity, positions } of perExchange) {
      equityOverview[name] = equity;
      result.push(...positions);
    }

    const phemexUnrealized = result
      .filter((r) => r.source === 'phemex')
      .reduce((s, r) => s + (r.unrealizedPnl || 0), 0);
    if (equityOverview.phemex) {
      equityOverview.phemex.total += phemexUnrealized;
      equityOverview.phemex.unrealizedPnl = phemexUnrealized;
    }

    // Orders —— 每个交易所独立容错，失败不影响其他
    const orderPromises = exchangeList.map(async ([name, exchange]) => {
      // loadMarkets 已失败的交易所直接跳过订单查询
      if (exchangeStatus[name] && !exchangeStatus[name].ok && exchangeStatus[name].errorType) {
        // 已经有错误记录，但 orders 失败不覆盖更重要的 balance 错误
      }
      try {
        return await processExchangeOrders(name, exchange, result);
      } catch (err) {
        console.error(`❌ ${name} orders:`, err.message);
        return [];
      }
    });
    const ordersPerExchange = await Promise.all(orderPromises);
    const dedupedOrders = dedupeOrders(ordersPerExchange.flat());

    const orderIndex = new Map();
    for (const o of dedupedOrders) {
      const key = `${o.exchange}|${o.symbol.toUpperCase()}`;
      if (!orderIndex.has(key)) orderIndex.set(key, []);
      orderIndex.get(key).push({
        side: o.side,
        price: o.price,
        triggerPrice: o.triggerPrice,
        limitPrice: o.limitPrice,
        amount: o.amount,
        kind: o.kind,
        orderType: o.orderType,
      });
    }
    for (const pos of result) {
      const key = `${pos.source}|${pos.symbol.toUpperCase()}`;
      const related = orderIndex.get(key);
      if (related?.length) pos.tpSlClose = related;
    }

    // 健康分析放在 orders 挂完之后，才能拿到 order count
    const hedgeHealth = analyzeHedges(result);

    const totalEquity = Object.values(equityOverview).reduce(
      (s, ex) => s + (ex.total || 0), 0
    );

    // 汇总失败的交易所列表（给前端显示）
    const failedExchanges = Object.entries(exchangeStatus)
      .filter(([, s]) => !s.ok)
      .map(([name, s]) => ({ name, error: s.error, errorType: s.errorType }));

    const elapsed = Date.now() - t0;
    const okCount = Object.values(exchangeStatus).filter((s) => s.ok).length;
    console.log(
      `✅ ${elapsed}ms | exchanges ok=${okCount}/${exchangeList.length} | pos=${result.length}` +
      (failedExchanges.length ? ` | failed: ${failedExchanges.map(f => f.name).join(',')}` : '')
    );

    res.status(200).json({
      success: true,
      result,
      equityOverview,
      totalEquity,
      hedgeHealth,
      exchangeStatus,      // 每个交易所 { ok, error, errorType }
      failedExchanges,     // 仅失败的，方便前端直接显示
      serverNow: nowMs,    // 服务器抓取时刻，前端按此算时间窗口
      windowMs: FUNDING_WINDOW_MS, // 数据覆盖的最大窗口 (5天)
      elapsedMs: elapsed,
    });
  } catch (e) {
    // 这里只会捕获非交易所级的意外错误（如代码 bug）
    console.error('❌ Fatal Error:', e);
    res.status(500).json({ success: false, error: classifyError(e).message, raw: e.message });
  }
};
