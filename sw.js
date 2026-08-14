const CACHE_VERSION = 'raja-ai-pwa-v5-app-features';

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

self.addEventListener('message', (event) => {
  const data = event.data || {};
  if (data.type !== 'RAJA_SHOW_NOTIFICATION') return;

  const payload = data.payload || {};
  const title = String(payload.title || 'RAJA AI');
  const options = Object.assign({
    icon: '/raja-ai-icon-192.png',
    badge: '/raja-ai-icon-192.png',
    tag: 'raja-ai-alert',
    data: { url: '/' }
  }, payload.options || {});

  event.waitUntil(self.registration.showNotification(title, options));
});

self.addEventListener('notificationclick', (event) => {
  event.notification.close();
  const targetUrl = (event.notification.data && event.notification.data.url) || '/';

  event.waitUntil(
    clients.matchAll({ type: 'window', includeUncontrolled: true }).then((clientList) => {
      for (const client of clientList) {
        if ('focus' in client) {
          try { client.navigate(targetUrl); } catch (_) {}
          return client.focus();
        }
      }
      if (clients.openWindow) return clients.openWindow(targetUrl);
      return undefined;
    })
  );
});
