const CACHE = 't2d-pulse-v4';
const PREFIX = 't2d-pulse-';
const CORE = ['/', '/offline.html', '/pulse.css', '/pulse.js', '/manifest.webmanifest', '/icons/t2d-pulse-192.png'];
const MAX_ENTRIES = 100;

self.addEventListener('install', event => {
  event.waitUntil(caches.open(CACHE).then(cache => cache.addAll(CORE)).then(() => self.skipWaiting()));
});
self.addEventListener('activate', event => {
  event.waitUntil(caches.keys().then(keys => Promise.all(
    keys.filter(key => key.startsWith(PREFIX) && key !== CACHE).map(key => caches.delete(key))
  )).then(() => self.clients.claim()));
});

// Serialize cache writes so the limit holds during parallel image requests.
let cacheWrites = Promise.resolve();
function remember(key, response) {
  cacheWrites = cacheWrites.catch(() => {}).then(async () => {
    const cache = await caches.open(CACHE);
    await cache.put(key, response);
    const keys = await cache.keys();
    const removable = keys.filter(request => !CORE.includes(new URL(request.url).pathname));
    for (const request of removable.slice(0, Math.max(0, keys.length - MAX_ENTRIES))) await cache.delete(request);
  });
  return cacheWrites;
}

self.addEventListener('fetch', event => {
  const request = event.request;
  const url = new URL(request.url);
  if (request.method !== 'GET' || url.origin !== self.location.origin) return;
  if (request.mode !== 'navigate' && !['style', 'script', 'image', 'manifest'].includes(request.destination)
      && !['/pulse.json', '/build-status.json'].includes(url.pathname)) return;
  const key = url.pathname === '/' || url.pathname === '/index.html' ? new URL('/', url).href : request.url;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 5000);
  const network = fetch(request, {cache: 'no-cache', signal: controller.signal}).then(response => {
    if (response.status >= 500) throw new Error('Server temporarily unavailable');
    return response;
  }).finally(() => clearTimeout(timeout));
  // Attach waitUntil synchronously, before the event dispatch completes.
  event.waitUntil(network.then(response => response.ok ? remember(key, response.clone()) : null).catch(() => {}));
  event.respondWith(network.catch(async () => {
    const cache = await caches.open(CACHE);
    const cached = await cache.match(key);
    if (cached) return cached;
    if (request.mode === 'navigate') return (await cache.match('/offline.html')) || Response.error();
    return Response.error();
  }));
});
