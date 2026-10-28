// Servidor do Aventura Infinita: serve o site E o multiplayer (WebSocket) na mesma porta.
// Mesmo protocolo do worker.js (Cloudflare), então o mp-shim.js funciona sem mudanças.
// Uso: coloque esta pasta (servidor-node) dentro da pasta do site e rode:  npm install && npm start
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const SITE = path.resolve(process.env.SITE_DIR || path.join(__dirname, '..'));
const HAS_SITE = fs.existsSync(path.join(SITE, 'index.html')); // sem site ao lado, só roda o multiplayer
const ALLOWED = (process.env.ALLOWED_ORIGINS || '').split(',').map(s => s.trim()).filter(Boolean);
const MAX_PEERS = 40, MAX_MSG = 700, MIN_GAP_MS = 60;

const MIME = {
  '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.css': 'text/css',
  '.json': 'application/json', '.webmanifest': 'application/manifest+json', '.png': 'image/png',
  '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.svg': 'image/svg+xml', '.txt': 'text/plain; charset=utf-8',
  '.ico': 'image/x-icon'
};

const server = http.createServer((req, res) => {
  let p;
  try { p = decodeURIComponent(new URL(req.url, 'http://x').pathname); } catch { res.writeHead(400); return res.end(); }

  // Endereço do multiplayer = o próprio servidor (sem precisar editar mp-config.js)
  if (p === '/mp-config.js') {
    const proto = (req.headers['x-forwarded-proto'] || 'http').split(',')[0].trim();
    const wsProto = proto === 'https' ? 'wss' : 'ws';
    res.writeHead(200, { 'Content-Type': MIME['.js'], 'Cache-Control': 'no-cache' });
    return res.end('window.MP_SERVER = "' + wsProto + '://' + req.headers.host + '";');
  }
  if (p === '/saude') { res.writeHead(200); return res.end('ok'); }

  if (!HAS_SITE) { res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' }); return res.end('Aventura Infinita multiplayer: ok'); }
  if (p.endsWith('/')) p += 'index.html';
  const file = path.join(SITE, p);
  if (!file.startsWith(SITE + path.sep) || file.includes(path.sep + 'servidor-node' + path.sep)
      || file.includes(path.sep + 'servidor-multiplayer' + path.sep)) { res.writeHead(404); return res.end('Not found'); }
  fs.readFile(file, (err, data) => {
    if (err) { res.writeHead(404); return res.end('Not found'); }
    const ext = path.extname(file).toLowerCase();
    const h = { 'Content-Type': MIME[ext] || 'application/octet-stream' };
    if (path.basename(file) === 'sw.js') h['Cache-Control'] = 'no-cache';
    res.writeHead(200, h); res.end(data);
  });
});

// ---- Multiplayer ----
const rooms = new Map(); // nome -> Map(ws -> {id, p, at})
const wss = new WebSocketServer({ noServer: true, maxPayload: MAX_MSG });

server.on('upgrade', (req, socket, head) => {
  const u = new URL(req.url, 'http://x');
  const origin = req.headers.origin || '';
  const room = (u.searchParams.get('room') || '').slice(0, 80);
  const bad = u.pathname !== '/ws' || !/^[\w.\-]{1,80}$/.test(room)
    || (ALLOWED.length && !ALLOWED.includes(origin))
    || (rooms.get(room) && rooms.get(room).size >= MAX_PEERS);
  if (bad) { socket.write('HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n'); return socket.destroy(); }
  wss.handleUpgrade(req, socket, head, ws => join(ws, room));
});

function broadcast(room, from, obj) {
  const s = JSON.stringify(obj);
  for (const w of room.keys()) if (w !== from && w.readyState === 1) { try { w.send(s); } catch {} }
}

function join(ws, name) {
  let room = rooms.get(name);
  if (!room) { room = new Map(); rooms.set(name, room); }
  const id = Math.random().toString(36).slice(2, 10);
  const st = { id, p: null, at: 0 };
  const peers = [];
  for (const a of room.values()) if (a.p) peers.push({ peer: a.id, presence: a.p });
  room.set(ws, st);
  ws.send(JSON.stringify({ t: 'init', me: id, peers }));

  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', (buf, isBinary) => {
    if (isBinary) return;
    const msg = buf.toString();
    if (msg.length > MAX_MSG) return;
    const now = Date.now();
    if (now - st.at < MIN_GAP_MS) return;
    let m; try { m = JSON.parse(msg); } catch { return; }
    if (!m || m.t !== 'p' || typeof m.d !== 'object' || m.d === null || Array.isArray(m.d)) return;
    const first = st.p === null;
    st.p = m.d; st.at = now;
    broadcast(room, ws, { t: first ? 'join' : 'p', peer: id, presence: m.d, d: m.d });
  });
  const left = () => {
    if (!room.delete(ws)) return;
    broadcast(room, ws, { t: 'left', peer: id });
    if (room.size === 0) rooms.delete(name);
  };
  ws.on('close', left); ws.on('error', left);
}

// derruba conexões mortas (celular que perdeu sinal)
setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) { ws.terminate(); continue; }
    ws.isAlive = false; try { ws.ping(); } catch {}
  }
}, 30000);

server.listen(PORT, () => console.log('Aventura Infinita no ar: http://localhost:' + PORT + '  (site: ' + SITE + ')'));
