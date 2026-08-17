const CACHE_VERSION = 'raja-ai-pwa-v20-native-capture';
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
  '/verify-license',
  '/logout-license',
  '/user/',
  '/signals/',
  '/scan',
  '/batch-scan',
  '/market-news',
  '/otc-fallback-config',
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

        if (response && response.ok) {
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
      keys.map(key => {
        if (
          /^raja-ai-pwa-/i.test(key) &&
          !keep.has(key)
        ) {
          return caches.delete(key);
        }

        return Promise.resolve(false);
      })
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

async function offlineJson() {
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

  try {
    const response = await fetch(
      request,
      {
        cache: 'no-store'
      }
    );

    if (
      response &&
      response.ok
    ) {
      /*
        Normalize navigation variants like:
        ?raja_install=1
        build query tokens
        temporary query parameters

        into one stable cached app shell.
      */
      await cache.put(
        '/',
        response.clone()
      );
    }

    return response;

  } catch (_) {

    return (
      await cache.match('/')
    ) || Response.error();
  }
}

async function networkFirstAsset(request) {
  const cache = await caches.open(ASSET_CACHE);

  try {
    const response = await fetch(
      request,
      {
        cache: 'no-store'
      }
    );

    if (
      response &&
      response.ok
    ) {
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

  if (
    request.method !== 'GET'
  ) {
    return;
  }

  const url = new URL(request.url);

  if (
    url.origin !== self.location.origin
  ) {
    return;
  }

  /*
    HTML / app navigation:
    always latest network version first.
  */
  if (
    request.mode === 'navigate'
  ) {
    event.respondWith(
      networkFirstNavigation(request)
    );

    return;
  }

  /*
    API requests:
    never use stale cached API data.
  */
  if (
    isApiPath(url.pathname)
  ) {
    event.respondWith(
      fetch(
        request,
        {
          cache: 'no-store'
        }
      ).catch(offlineJson)
    );

    return;
  }

  /*
    Important PWA/update/icon files:
    always network-first.
  */
  if (
    url.pathname === '/sw.js' ||
    url.pathname === '/manifest.json' ||
    url.pathname === '/app-version' ||
    /^\/raja-ai-icon-/.test(url.pathname) ||
    url.pathname === '/raja-splash-logo.png'
  ) {
    event.respondWith(
      networkFirstAsset(request)
    );

    return;
  }

  /*
    Other same-origin assets:
    network first, cached fallback.
  */
  event.respondWith(
    networkFirstAsset(request)
  );
});
