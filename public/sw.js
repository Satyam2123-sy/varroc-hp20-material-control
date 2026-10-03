const CACHE='varroc-hp20-shell-v32';
const APP=['/','/index.html'];
self.addEventListener('install',e=>e.waitUntil(caches.open(CACHE).then(c=>c.addAll(APP)).then(()=>self.skipWaiting())));
self.addEventListener('activate',e=>e.waitUntil(caches.keys().then(keys=>Promise.all(keys.filter(k=>k.startsWith('varroc-hp20-shell-')&&k!==CACHE).map(k=>caches.delete(k)))).then(()=>self.clients.claim())));
self.addEventListener('fetch',e=>{
  const u=new URL(e.request.url);
  if(e.request.method!=='GET' || u.pathname.startsWith('/socket.io/')) return;
  if(u.pathname==='/' || u.pathname==='/index.html'){
    e.respondWith(fetch(e.request,{cache:'no-store'}).then(r=>{
      if(r.ok){const c=r.clone();caches.open(CACHE).then(x=>x.put('/index.html',c));}
      return r;
    }).catch(()=>caches.match('/index.html')));
    return;
  }
  e.respondWith(caches.match(e.request).then(c=>c||fetch(e.request)));
});
