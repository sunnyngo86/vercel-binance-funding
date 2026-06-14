// Funding Dashboard Service Worker
// 只做最简 passthrough，满足 PWA 可安装要求，不缓存数据（交易数据要实时）
self.addEventListener('install', (e) => self.skipWaiting());
self.addEventListener('activate', (e) => self.clients.claim());
self.addEventListener('fetch', (e) => {
  // 纯透传，永不缓存 —— 确保每次都是最新的余额/持仓数据
  e.respondWith(
    fetch(e.request).catch(() => new Response('offline', { status: 503 }))
  );
});
