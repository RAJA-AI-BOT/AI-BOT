const CACHE_VERSION = 'raja-ai-pwa-v6-offline-control';

const APP_SHELL = [
  '/',
  '/manifest.json',
  '/raja-ai-icon-192.png',
  '/raja-ai-icon-512.png'
];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_VERSION)
      .then(cache => Promise.allSettled(
        APP_SHELL.map(url => cache.add(url))
      ))
      .then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys()
      .then(keys =>
        Promise.all(
          keys
            .filter(k => k !== CACHE_VERSION)
            .map(k => caches.delete(k))
        )
      )
      .then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', (event) => {
  const request = event.request;

  if (request.method !== 'GET') return;

  const url = new URL(request.url);

  if (url.origin !== self.location.origin) return;

  // Never cache authenticated/history API responses
  const isApi = [
    '/signals/history',
    '/signals/stats',
    '/health',
    '/app-state'
  ].some(path => url.pathname.startsWith(path));

  if (isApi) {
    event.respondWith(
      fetch(request).catch(() =>
        new Response(
          JSON.stringify({
            status: 'error',
            offline: true,
            message: 'RAJA AI is offline.'
          }),
          {
            status: 503,
            headers: {
              'Content-Type': 'application/json; charset=utf-8'
            }
          }
        )
      )
    );

    return;
  }

  // Main app page:
  // Network first, cached app shell if offline
  if (request.mode === 'navigate') {
    event.respondWith(
      fetch(request)
        .then(response => {
          if (response && response.ok) {
            const copy = response.clone();

            caches.open(CACHE_VERSION)
              .then(cache => cache.put('/', copy))
              .catch(() => {});
          }

          return response;
        })
        .catch(async () => {
          const cached = await caches.match('/');

          return cached || new Response(
            'RAJA AI is offline and no saved app shell is available yet.',
            {
              status: 503,
              headers: {
                'Content-Type': 'text/plain; charset=utf-8'
              }
            }
          );
        })
    );

    return;
  }

  // Public PWA files:
  // Use cached version first, refresh cache from network
  if (APP_SHELL.includes(url.pathname)) {
    event.respondWith(
      caches.match(request).then(cached => {
        const network = fetch(request)
          .then(response => {
            if (response && response.ok) {
              caches.open(CACHE_VERSION)
                .then(cache => cache.put(request, response.clone()))
                .catch(() => {});
            }

            return response;
          })
          .catch(() => cached);

        return cached || network;
      })
    );

    return;
  }

  // Everything else remains network-first
  event.respondWith(
    fetch(request).catch(() =>
      new Response(
        'RAJA AI is temporarily offline. Please reconnect to use live data.',
        {
          status: 503,
          headers: {
            'Content-Type': 'text/plain; charset=utf-8'
          }
        }
      )
    )
  );
});


// ======================================================
// RAJA AI SYSTEM NOTIFICATIONS
// ======================================================

self.addEventListener('message', (event) => {
  const data = event.data || {};

  if (data.type !== 'RAJA_SHOW_NOTIFICATION') return;

  const payload = data.payload || {};

  const title = String(
    payload.title || 'RAJA AI'
  );

  const options = Object.assign(
    {
      icon: '/raja-ai-icon-192.png',
      badge: '/raja-ai-icon-192.png',

      tag: 'raja-ai-alert',

      data: {
        url: '/'
      }
    },

    payload.options || {}
  );

  event.waitUntil(
    self.registration.showNotification(
      title,
      options
    )
  );
});


// ======================================================
// OPEN APP WHEN NOTIFICATION IS CLICKED
// ======================================================

self.addEventListener('notificationclick', (event) => {

  event.notification.close();

  const targetUrl =
    (event.notification.data &&
     event.notification.data.url)
      || '/';

  event.waitUntil(

    clients
      .matchAll({
        type: 'window',
        includeUncontrolled: true
      })

      .then((clientList) => {

        for (const client of clientList) {

          if ('focus' in client) {

            try {
              client.navigate(targetUrl);
            } catch (_) {}

            return client.focus();
          }

        }

        if (clients.openWindow) {
          return clients.openWindow(targetUrl);
        }

        return undefined;
      })
  );
});
