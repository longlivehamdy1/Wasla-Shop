// Wasla service worker: يخلّي الموقع قابل للتثبيت كتطبيق. مفيش كاش للـ API أو للبيانات.
const SHELL = 'wasla-shell-v1';
self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', e => e.waitUntil(self.clients.claim()));
self.addEventListener('fetch', e => {
  const req = e.request;
  if (req.method !== 'GET' || req.mode !== 'navigate') return;
  e.respondWith(
    fetch(req).then(res => {
      const copy = res.clone();
      caches.open(SHELL).then(c => c.put('/__shell', copy)).catch(() => {});
      return res;
    }).catch(() => caches.match('/__shell').then(r => r || new Response('أنت أوفلاين حالياً. افتح الاتصال وحاول تاني.', { status: 503, headers: { 'Content-Type': 'text/plain; charset=utf-8' } })))
  );
});
