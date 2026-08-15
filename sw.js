const CACHE_VERSION = 'raja-ai-pwa-v8-fast-shell';

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

function isApiPath(pathname) {
  return API_PATHS.some((path) => pathname.startsWith(path));
}

async function putIfUsable(cache, key, response) {
  if (!response || !response.ok) return response;

  try {
    await cache.put(key, response.clone());
  } catch (_) {}

  return response;
}

function offlineJson() {
  return new Response(
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
  );
}

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
// ACTIVATE
// ======================================================

self.addEventListener('activate', (event) => {
  event.waitUntil(
    (async () => {
      const keys = await caches.keys();

      await Promise.all(
        keys
          .filter(
            (key) =>
              key.startsWith('raja-ai-pwa-') &&
              key !== CACHE_VERSION
          )
          .map((key) => caches.delete(key))
      );

      // Navigation preload starts the network request while the
      // service worker boots. Cached UI can still be returned first.
      try {
        if (self.registration.navigationPreload) {
          await self.registration.navigationPreload.enable();
        }
      } catch (_) {}

      await self.clients.claim();
    })()
  );
});

// ======================================================
// FETCH
// ======================================================

self.addEventListener('fetch', (event) => {
  const request = event.request;

  if (request.method !== 'GET') return;

  const url = new URL(request.url);

  // Never intercept third-party resources.
  if (url.origin !== self.location.origin) return;

  // ------------------------------------------------------
  // LIVE / AUTHENTICATED APIs:
  // Always network-only. Never serve stale trading data.
  // ------------------------------------------------------
  if (isApiPath(url.pathname)) {
    event.respondWith(
      fetch(request, { cache: 'no-store' })
        .catch(() => offlineJson())
    );
    return;
  }

  // ------------------------------------------------------
  // NAVIGATION:
  // FAST SHELL FIRST.
  //
  // If '/' is already cached, return it immediately so the UI
  // opens even while Render is waking. In parallel, refresh the
  // cached HTML from the network. That background request also
  // wakes the backend without blocking the visible app.
  // ------------------------------------------------------
  if (request.mode === 'navigate') {
    const refreshPromise = (async () => {
      try {
        const preload = await event.preloadResponse;

        const response =
          preload ||
          await fetch(request, { cache: 'no-store' });

        if (response && response.ok) {
          const cache = await caches.open(CACHE_VERSION);
          await putIfUsable(cache, '/', response);
        }

        return response || null;
      } catch (_) {
        return null;
      }
    })();

    event.waitUntil(
      refreshPromise
        .then(() => undefined)
        .catch(() => undefined)
    );

    event.respondWith(
      (async () => {
        const cache = await caches.open(CACHE_VERSION);
        const cached = await cache.match('/');

        // Repeat opens: instant shell.
        if (cached) return cached;

        // First controlled load: wait for network/preload.
        const network = await refreshPromise;

        if (network) return network;

        return new Response(
          'RAJA AI is offline and no saved app shell is available yet.',
          {
            status: 503,
            headers: {
              'Content-Type': 'text/plain; charset=utf-8',
              'Cache-Control': 'no-store'
            }
          }
        );
      })()
    );

    return;
  }

  // ------------------------------------------------------
  // APP SHELL STATIC FILES:
  // Cache-first + background refresh.
  // ------------------------------------------------------
  if (APP_SHELL.includes(url.pathname)) {
    const refreshPromise = fetch(request, {
      cache: 'no-cache'
    })
      .then(async (response) => {
        if (response && response.ok) {
          const cache = await caches.open(CACHE_VERSION);

          await putIfUsable(
            cache,
            request,
            response
          );
        }

        return response;
      })
      .catch(() => null);

    event.waitUntil(
      refreshPromise
        .then(() => undefined)
        .catch(() => undefined)
    );

    event.respondWith(
      caches.match(request)
        .then(async (cached) => {
          if (cached) return cached;

          const network = await refreshPromise;

          return (
            network ||
            new Response(
              'RAJA AI asset unavailable while offline.',
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
  // EVERYTHING ELSE:
  // Network first.
  // Do not cache live/application responses.
  // ------------------------------------------------------
  event.respondWith(
    fetch(request, {
      cache: 'no-cache'
    })
      .catch(() =>
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
// MESSAGES + SYSTEM NOTIFICATIONS
// ======================================================

self.addEventListener('message', (event) => {
  const data = event.data || {};

  if (data.type === 'SKIP_WAITING') {
    event.waitUntil(
      self.skipWaiting()
    );

    return;
  }

  if (data.type !== 'RAJA_SHOW_NOTIFICATION') {
    return;
  }

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
// OPEN / FOCUS APP WHEN NOTIFICATION IS CLICKED
// ======================================================

self.addEventListener('notificationclick', (event) => {
  event.notification.close();

  const targetUrl =
    (
      event.notification.data &&
      event.notification.data.url
    ) ||
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
          } catch (_) {}
        }

        if (clients.openWindow) {
          return clients.openWindow(targetUrl);
        }

        return undefined;
      })
  );
});
