const CACHE_VERSION = 'raja-ai-pwa-v43-front-broker-pair';
const SHELL_CACHE = `${CACHE_VERSION}-shell`;
const ASSET_CACHE = `${CACHE_VERSION}-assets`;

const STATIC_ASSETS = [
  '/manifest.json',
  '/raja-ai-icon-192-v3.png',
  '/raja-ai-icon-512-v3.png',
  '/raja-splash-logo.png'
];

const API_PREFIXES = [
  '/app-version',
  '/health',
  '/verify-license',
  '/logout-license',
  '/user/',
  '/signals/',
  '/track-signal',
  '/scan',
  '/scan-batch',
  '/batch-scan',
  '/side-auto-signals',
  '/chart-scan',
  '/market-news',
  '/otc-fallback-config',
  '/forex-otc-fallback-data',
  '/quotex-bridge/',
  '/telegram/',
  '/admin/'
];

self.addEventListener('install', event => {
  event.waitUntil((async () => {
    self.skipWaiting();

    const cache = await caches.open(ASSET_CACHE);

    await Promise.allSettled(
      STATIC_ASSETS.map(async url => {
        const response = await fetch(url, {
          cache: 'reload'
        });

        if (response?.ok) {
          await cache.put(
            url,
            response.clone()
          );
        }
      })
    );
  })());
});

self.addEventListener('activate', event => {
  event.waitUntil((async () => {
    const keep = new Set([
      SHELL_CACHE,
      ASSET_CACHE
    ]);

    const keys = await caches.keys();

    await Promise.all(
      keys.map(key =>
        /^raja-ai-pwa-/i.test(key) && !keep.has(key)
          ? caches.delete(key)
          : Promise.resolve(false)
      )
    );

    await self.clients.claim();
  })());
});

self.addEventListener('message', event => {
  if (
    event.data &&
    event.data.type === 'SKIP_WAITING'
  ) {
    self.skipWaiting();
  }
});

function isApiPath(pathname) {
  return API_PREFIXES.some(
    prefix =>
      pathname === prefix ||
      pathname.startsWith(prefix)
  );
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
        'Content-Type': 'application/json'
      }
    }
  );
}

async function networkFirstNavigation(request) {
  const cache = await caches.open(SHELL_CACHE);
  const pathname = new URL(request.url).pathname;

  let shellKey = '/';

  if (pathname.startsWith('/live-scanner')) {
    shellKey = '/live-scanner';
  } else if (pathname.startsWith('/chart-scanner')) {
    shellKey = '/chart-scanner';
  }

  try {
    const response = await fetch(request, {
      cache: 'no-store'
    });

    if (response?.ok) {
      await cache.put(
        shellKey,
        response.clone()
      );
    }

    return response;
  } catch (_) {
    return (
      await cache.match(shellKey)
    ) || Response.error();
  }
}

async function networkFirstAsset(request) {
  const cache = await caches.open(ASSET_CACHE);

  try {
    const response = await fetch(request, {
      cache: 'no-store'
    });

    if (response?.ok) {
      await cache.put(
        request,
        response.clone()
      );
    }

    return response;
  } catch (_) {
    return (
      await cache.match(request)
    ) || Response.error();
  }
}

self.addEventListener('fetch', event => {
  const request = event.request;

  if (request.method !== 'GET') {
    return;
  }

  const url = new URL(request.url);

  if (url.origin !== self.location.origin) {
    return;
  }

  if (request.mode === 'navigate') {
    event.respondWith(
      networkFirstNavigation(request)
    );
    return;
  }

  if (isApiPath(url.pathname)) {
    event.respondWith(
      fetch(request, {
        cache: 'no-store'
      }).catch(offlineJson)
    );
    return;
  }

  event.respondWith(
    networkFirstAsset(request)
  );
});
