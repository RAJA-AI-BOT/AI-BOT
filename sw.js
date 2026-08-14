
const CACHE_VERSION = 'raja-ai-pwa-v4';

self.addEventListener('install', () => {
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys()
      .then(keys => Promise.all(keys.filter(k => k !== CACHE_VERSION).map(k => caches.delete(k))))
      .then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', (event) => {
  if (event.request.method !== 'GET') return;

  event.respondWith(
    fetch(event.request).catch(() =>
      new Response(
        'RAJA AI is temporarily offline. Please reconnect and reopen the app.',
        { status: 503, headers: { 'Content-Type': 'text/plain; charset=utf-8' } }
      )
    )
  );
});
