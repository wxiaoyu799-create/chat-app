const CACHE_NAME = 'chat-app-shell-v2';
const SHELL_FILES = [
  '/',
  '/manifest.json',
  '/icon-192.png',
  '/icon-512.png',
];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => cache.addAll(SHELL_FILES))
  );
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(
        keys.filter((key) => key !== CACHE_NAME).map((key) => caches.delete(key))
      )
    )
  );
  self.clients.claim();
});

// 只缓存静态外壳资源（HTML/图标/manifest），不拦截 WebSocket 连接，
// 保证聊天始终是实时的。
//
// 缓存策略：网络优先，缓存兜底。
// 这个项目改动很频繁，如果用"缓存优先"，联网状态下也会先给用户上一次缓存下来的旧版本，
// 导致每次部署后，其他设备总是"差一个版本"，得手动刷新两次才能追上——
// 之前"页面头部背景还是黑色"这个问题就是这么来的。
// 改成网络优先后：只要联网，永远优先请求最新内容；请求失败（比如离线）才退回缓存版本兜底，
// 这样只要用户联网就一定能看到最新部署的内容，缓存只在真正离线时才派上用场。
self.addEventListener('fetch', (event) => {
  const url = new URL(event.request.url);
  if (event.request.method !== 'GET' || url.pathname.startsWith('/ws')) {
    return;
  }
  event.respondWith(
    fetch(event.request)
      .then((response) => {
        if (response && response.status === 200) {
          const clone = response.clone();
          caches.open(CACHE_NAME).then((cache) => cache.put(event.request, clone));
        }
        return response;
      })
      .catch(() => caches.match(event.request))
  );
});
