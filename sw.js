const CACHE_VERSION = 'raja-ai-pwa-v7-ui-refresh';

const APP_SHELL = [
  '/',
  '/manifest.json',
  '/raja-ai-icon-192.png',
  '/raja-ai-icon-512.png'
];

const API_PATHS = [
  '/signals/history',
  '/signals/stats',
  '/health',
  '/app-state'
];

// ======================================================
// INSTALL
// ======================================================

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_VERSION)
      .then((cache) =>
        Promise.allSettled(
          APP_SHELL.map((url) =>
            cache.add(new Request(url, { cache: 'reload' }))
          )
        )
      )
      .then(() => self.skipWaiting())
  );
});

// ======================================================
// ACTIVATE + REMOVE OLD RAJA AI CACHES
// ======================================================

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys()
      .then((keys) =>
        Promise.all(
          keys
            .filter(
              (key) =>
                key.startsWith('raja-ai-pwa-') &&
                key !== CACHE_VERSION
            )
            .map((key) => caches.delete(key))
        )
      )
      .then(() => self.clients.claim())
  );
});

// ======================================================
// FETCH
// ======================================================

self.addEventListener('fetch', (event) => {
  const request = event.request;

  if (request.method !== 'GET') return;

  const url = new URL(request.url);

  // Do not interfere with third-party requests.
  if (url.origin !== self.location.origin) return;

  const isApi = API_PATHS.some((path) =>
    url.pathname.startsWith(path)
  );

  // ------------------------------------------------------
  // LIVE / AUTHENTICATED API:
  // Always network. Never return stale cached trading data.
  // ------------------------------------------------------
  if (isApi) {
    event.respondWith(
      fetch(request, { cache: 'no-store' }).catch(() =>
        new Response(
          JSON.stringify({
            status: 'error',
            offline: true,
            message: 'RAJA AI is offline.'
          }),
          {
            status: 503,
            headers: {
              'Content-Type': 'application/json; charset=utf-8',
              'Cache-Control': 'no-store'
            }
          }
        )
      )
    );

    return;
  }

  // ------------------------------------------------------
  // MAIN APP PAGE:
  // Network first so new UI/CSS/JS is picked up after deploy.
  // Save the latest successful page only for offline fallback.
  // ------------------------------------------------------
  if (request.mode === 'navigate') {
    event.respondWith(
      fetch(request, { cache: 'no-store' })
        .then((response) => {
          if (response && response.ok) {
            const copy = response.clone();

            caches.open(CACHE_VERSION)
              .then((cache) => cache.put('/', copy))
              .catch(() => {});
          }

          return response;
        })
        .catch(async () => {
          const cached = await caches.match('/');

          return (
            cached ||
            new Response(
              'RAJA AI is offline and no saved app shell is available yet.',
              {
                status: 503,
                headers: {
                  'Content-Type': 'text/plain; charset=utf-8',
                  'Cache-Control': 'no-store'
                }
              }
            )
          );
        })
    );

    return;
  }

  // ------------------------------------------------------
  // PWA STATIC FILES:
  // Serve cache immediately, then refresh in background.
  // ------------------------------------------------------
  if (APP_SHELL.includes(url.pathname)) {
    event.respondWith(
      caches.match(request).then((cached) => {
        const networkPromise = fetch(request, { cache: 'no-cache' })
          .then((response) => {
            if (response && response.ok) {
              caches.open(CACHE_VERSION)
                .then((cache) =>
                  cache.put(request, response.clone())
                )
                .catch(() => {});
            }

            return response;
          })
          .catch(() => cached);

        return cached || networkPromise;
      })
    );

    return;
  }

  // ------------------------------------------------------
  // EVERYTHING ELSE:
  // Network first. No stale trading/application resources.
  // ------------------------------------------------------
  event.respondWith(
    fetch(request).catch(() =>
      new Response(
        'RAJA AI is temporarily offline. Please reconnect to use live data.',
        {
          status: 503,
          headers: {
            'Content-Type': 'text/plain; charset=utf-8',
            'Cache-Control': 'no-store'
          }
        }
      )
    )
  );
});

// ======================================================
// SERVICE-WORKER MESSAGES + SYSTEM NOTIFICATIONS
// ======================================================

self.addEventListener('message', (event) => {
  const data = event.data || {};

  // Optional manual update support from the page.
  if (data.type === 'SKIP_WAITING') {
    event.waitUntil(self.skipWaiting());
    return;
  }

  if (data.type !== 'RAJA_SHOW_NOTIFICATION') return;

  const payload = data.payload || {};
  const title = String(payload.title || 'RAJA AI');

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
    self.registration.showNotification(title, options)
  );
});

// ======================================================
// OPEN / FOCUS APP WHEN NOTIFICATION IS CLICKED
// ======================================================

self.addEventListener('notificationclick', (event) => {
  event.notification.close();

  const targetUrl =
    (event.notification.data &&
      event.notification.data.url) ||
    '/';

  event.waitUntil(
    clients
      .matchAll({
        type: 'window',
        includeUncontrolled: true
      })
      .then(async (clientList) => {
        for (const client of clientList) {
          try {
            if ('navigate' in client) {
              await client.navigate(targetUrl);
            }

            if ('focus' in client) {
              return client.focus();
            }
          } catch (_) {
            // Try the next available client.
          }
        }

        if (clients.openWindow) {
          return clients.openWindow(targetUrl);
        }

        return undefined;
      })
  );
});
