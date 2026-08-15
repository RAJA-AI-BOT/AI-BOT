const CACHE_VERSION = 'raja-ai-pwa-v9-stable-shell';
const STABLE_SHELL_CACHE = 'raja-ai-pwa-stable-shell';
const ASSET_CACHE = 'raja-ai-pwa-v9-assets';

const STATIC_ASSETS = [
  '/manifest.json',
  '/raja-ai-icon-192.png',
  '/raja-ai-icon-512.png'
];

const API_PREFIXES = [
  '/signals/',
  '/scan',
  '/verify-license',
  '/health',
  '/app-state',
  '/market-news',
  '/admin/',
  '/telegram/'
];

function isApiPath(pathname) {
  return API_PREFIXES.some((path) => pathname.startsWith(path));
}

function offlineJson() {
  return new Response(
    JSON.stringify({
      status: 'error',
      offline: true,
      message: 'RAJA AI backend is waking or temporarily offline.'
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

async function cacheResponse(cacheName, key, response) {
  if (!response || !response.ok) return response;

  try {
    const cache = await caches.open(cacheName);
    await cache.put(key, response.clone());
  } catch (_) {}

  return response;
}

// ======================================================
// INSTALL
// ======================================================

self.addEventListener('install', (event) => {
  event.waitUntil((async () => {
    const assetCache = await caches.open(ASSET_CACHE);

    await Promise.allSettled(
      STATIC_ASSETS.map((url) =>
        assetCache.add(
          new Request(url, {
            cache: 'reload'
          })
        )
      )
    );

    // Best effort only.
    // Render waking ho to installation fail nahi hogi.
    try {
      const response = await fetch('/', {
        cache: 'no-store'
      });

      if (response && response.ok) {
        await cacheResponse(
          STABLE_SHELL_CACHE,
          '/',
          response
        );
      }
    } catch (_) {}

    await self.skipWaiting();
  })());
});

// ======================================================
// ACTIVATE
// ======================================================

self.addEventListener('activate', (event) => {
  event.waitUntil((async () => {
    const keys = await caches.keys();
    const stable = await caches.open(
      STABLE_SHELL_CACHE
    );

    // Previous working dashboard ko preserve karo
    // before deleting old versioned caches.
    const alreadySaved = await stable.match('/');

    if (!alreadySaved) {
      for (const key of keys) {
        if (!key.startsWith('raja-ai-pwa-')) {
          continue;
        }

        if (
          key === STABLE_SHELL_CACHE ||
          key === ASSET_CACHE
        ) {
          continue;
        }

        try {
          const oldCache = await caches.open(key);
          const oldRoot = await oldCache.match('/');

          if (oldRoot) {
            await stable.put(
              '/',
              oldRoot.clone()
            );

            break;
          }
        } catch (_) {}
      }
    }

    await Promise.all(
      keys
        .filter(
          (key) =>
            key.startsWith('raja-ai-pwa-') &&
            key !== STABLE_SHELL_CACHE &&
            key !== ASSET_CACHE &&
            key !== CACHE_VERSION
        )
        .map((key) => caches.delete(key))
    );

    // Browser network request ko SW boot ke sath hi
    // start kar sakta hai.
    try {
      if (
        self.registration.navigationPreload
      ) {
        await self.registration
          .navigationPreload
          .enable();
      }
    } catch (_) {}

    await self.clients.claim();
  })());
});

// ======================================================
// FETCH
// ======================================================

self.addEventListener('fetch', (event) => {
  const request = event.request;

  if (request.method !== 'GET') {
    return;
  }

  const url = new URL(request.url);

  // Third-party resources ko service worker
  // intercept nahi karega.
  if (
    url.origin !== self.location.origin
  ) {
    return;
  }

  // ------------------------------------------------------
  // LIVE / AUTHENTICATED API
  // ------------------------------------------------------

  if (isApiPath(url.pathname)) {
    event.respondWith(
      fetch(request, {
        cache: 'no-store'
      }).catch(() => offlineJson())
    );

    return;
  }

  // ------------------------------------------------------
  // NAVIGATION
  //
  // Returning user:
  // cached dashboard immediately show hoga.
  //
  // Background:
  // Render wake + latest dashboard refresh.
  // ------------------------------------------------------

  if (request.mode === 'navigate') {
    const networkRefresh = (async () => {
      try {
        const preload =
          await event.preloadResponse;

        const response =
          preload ||
          await fetch(request, {
            cache: 'no-store'
          });

        if (
          response &&
          response.ok
        ) {
          await cacheResponse(
            STABLE_SHELL_CACHE,
            '/',
            response
          );
        }

        return response || null;
      } catch (_) {
        return null;
      }
    })();

    event.waitUntil(
      networkRefresh
        .then(() => undefined)
        .catch(() => undefined)
    );

    event.respondWith(
      (async () => {
        const stable =
          await caches.open(
            STABLE_SHELL_CACHE
          );

        const cached =
          await stable.match('/');

        // Repeat opens:
        // dashboard instantly load.
        if (cached) {
          return cached;
        }

        // First ever visit:
        // server response ka wait.
        const network =
          await networkRefresh;

        if (network) {
          return network;
        }

        return new Response(
          `<!doctype html>
<html>
<head>
<meta
  name="viewport"
  content="width=device-width,initial-scale=1"
>
<title>RAJA AI</title>

<style>
html,
body {
  margin: 0;
  min-height: 100%;
  background: #020712;
  color: #fff;
  font-family: system-ui;
}

body {
  display: grid;
  place-items: center;
  min-height: 100vh;
}

.box {
  max-width: 420px;
  margin: 20px;
  padding: 24px;
  border: 1px solid #0ddfc6;
  border-radius: 18px;
  background: #07111f;
  text-align: center;
}

h1 {
  font-size: 22px;
  margin: 0 0 10px;
  color: #4fffe0;
}

p {
  color: #b8c6d8;
  line-height: 1.5;
}
</style>

</head>

<body>

<div class="box">
  <h1>RAJA AI</h1>

  <p>
    The server is waking.
    Reconnect and refresh in a moment.
  </p>
</div>

</body>
</html>`,
          {
            status: 503,
            headers: {
              'Content-Type':
                'text/html; charset=utf-8',

              'Cache-Control':
                'no-store'
            }
          }
        );
      })()
    );

    return;
  }

  // ------------------------------------------------------
  // STATIC PWA ASSETS
  // Cache first + background refresh.
  // ------------------------------------------------------

  if (
    STATIC_ASSETS.includes(
      url.pathname
    )
  ) {
    const refresh =
      fetch(request, {
        cache: 'no-cache'
      })
        .then(async (response) => {
          if (
            response &&
            response.ok
          ) {
            await cacheResponse(
              ASSET_CACHE,
              request,
              response
            );
          }

          return response;
        })
        .catch(() => null);

    event.waitUntil(
      refresh
        .then(() => undefined)
        .catch(() => undefined)
    );

    event.respondWith(
      (async () => {
        const cached =
          await caches.match(request);

        if (cached) {
          return cached;
        }

        const network =
          await refresh;

        return (
          network ||
          new Response(
            'RAJA AI asset unavailable.',
            {
              status: 503,
              headers: {
                'Content-Type':
                  'text/plain; charset=utf-8'
              }
            }
          )
        );
      })()
    );

    return;
  }

  // ------------------------------------------------------
  // EVERYTHING ELSE
  // Network first.
  // ------------------------------------------------------

  event.respondWith(
    fetch(request, {
      cache: 'no-cache'
    }).catch(() =>
      new Response(
        'RAJA AI is temporarily offline. Please reconnect to use live data.',
        {
          status: 503,
          headers: {
            'Content-Type':
              'text/plain; charset=utf-8',

            'Cache-Control':
              'no-store'
          }
        }
      )
    )
  );
});

// ======================================================
// MESSAGES + SYSTEM NOTIFICATIONS
// ======================================================

self.addEventListener(
  'message',
  (event) => {
    const data =
      event.data || {};

    if (
      data.type ===
      'SKIP_WAITING'
    ) {
      event.waitUntil(
        self.skipWaiting()
      );

      return;
    }

    if (
      data.type !==
      'RAJA_SHOW_NOTIFICATION'
    ) {
      return;
    }

    const payload =
      data.payload || {};

    const title =
      String(
        payload.title ||
        'RAJA AI'
      );

    const options =
      Object.assign(
        {
          icon:
            '/raja-ai-icon-192.png',

          badge:
            '/raja-ai-icon-192.png',

          tag:
            'raja-ai-alert',

          data: {
            url: '/'
          }
        },

        payload.options || {}
      );

    event.waitUntil(
      self.registration
        .showNotification(
          title,
          options
        )
    );
  }
);

// ======================================================
// OPEN / FOCUS APP WHEN NOTIFICATION CLICKED
// ======================================================

self.addEventListener(
  'notificationclick',
  (event) => {
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

        .then(
          async (clientList) => {
            for (
              const client
              of clientList
            ) {
              try {
                if (
                  'navigate' in client
                ) {
                  await client.navigate(
                    targetUrl
                  );
                }

                if (
                  'focus' in client
                ) {
                  return client.focus();
                }
              } catch (_) {}
            }

            if (
              clients.openWindow
            ) {
              return clients.openWindow(
                targetUrl
              );
            }

            return undefined;
          }
        )
    );
  }
);
