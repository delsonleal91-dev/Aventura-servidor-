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
const MAX_PEERS = 40;
const MAX_PRESENCE = 24000;   // presença (posição, roupa, chat, nome...)
const MAX_EVENT = 130000;     // eventos (voz, skin, construções, golpes)
const MAX_MSG = MAX_EVENT;    // limite duro do WebSocket
const MIN_GAP_MS = 60;
// Eventos: { t:'e', k:<tipo>, d:<dados>, to?:<id do jogador> }
//   vs   voz ao vivo (pedacos de audio) -> todos     (ate ~40 por segundo)
//   vz   voz (clipe curto, antigo)   -> todos        (max 1 a cada 700 ms)
//   skin skin importada (PNG base64) -> todos, guardada para quem entrar depois
//   ob   construções do jogador      -> todos, guardada para quem entrar depois
//   hit  golpe/flecha em jogador     -> só o alvo (to), dano limitado
const EVENT_RULES = {
  vz:   { gap: 700,  sticky: false, dm: false },
  vs:   { gap: 0, max: 60, sticky: false, dm: false },   // voz ao vivo: pedacos de ~80 ms (ate 60 por segundo)
  skin: { gap: 1500, sticky: true,  dm: false },
  ob:   { gap: 800,  sticky: true,  dm: false },
  hit:  { gap: 120,  sticky: false, dm: true  },
  ko:   { gap: 1000, sticky: false, dm: false }
};

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
  const st = { id, p: null, at: 0, ev: {}, sticky: {} };
  const peers = [];
  for (const a of room.values()) if (a.p) peers.push({ peer: a.id, presence: a.p });
  room.set(ws, st);
  ws.send(JSON.stringify({ t: 'init', me: id, peers }));
  // quem entra agora recebe a skin e as construções que os outros já publicaram
  for (const a of room.values()) {
    if (a === st) continue;
    for (const k in a.sticky) { try { ws.send(a.sticky[k]); } catch {} }
  }

  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', (buf, isBinary) => {
    if (isBinary) return;
    const msg = buf.toString();
    if (msg.length > MAX_MSG) return;
    const now = Date.now();
    let m; try { m = JSON.parse(msg); } catch { return; }
    if (!m || typeof m !== 'object') return;

    // ---- eventos (voz, skin, construções, golpes) ----
    if (m.t === 'e') {
      const rule = EVENT_RULES[m.k];
      if (!rule || typeof m.d !== 'object' || m.d === null || Array.isArray(m.d)) return;
      if (rule.max) {            // limite por segundo (aceita rajadas curtas)
        const w = st.win || (st.win = {});
        const b = w[m.k] || (w[m.k] = { t: now, n: 0 });
        if (now - b.t >= 1000) { b.t = now; b.n = 0; }
        if (++b.n > rule.max) return;
      } else {
        if (now - (st.ev[m.k] || 0) < rule.gap) return;
        st.ev[m.k] = now;
      }
      if (m.k === 'vs' && (typeof m.d.d !== 'string' || m.d.d.length > 12000)) return;
      if (m.k === 'hit') {
        let dmg = Number(m.d.dmg);
        if (!Number.isFinite(dmg)) return;
        m.d = { dmg: Math.max(1, Math.min(60, Math.round(dmg))), w: m.d.w === 'a' ? 'a' : 'm' };
      }
      if (m.k === 'ko') m.d = { by: typeof m.d.by === 'string' ? m.d.by.slice(0, 16) : '' };
      const out = JSON.stringify({ t: 'e', k: m.k, peer: id, d: m.d });
      if (rule.dm) {            // mensagem direta: só o alvo, e só se estiver na mesma sala
        const to = String(m.to || '');
        for (const [w, a] of room) if (a.id === to && w.readyState === 1) { try { w.send(out); } catch {} }
        return;
      }
      if (rule.sticky) st.sticky[m.k] = out;
      broadcast(room, ws, JSON.parse(out));
      return;
    }

    // ---- presença ----
    if (msg.length > MAX_PRESENCE) return;
    if (now - st.at < MIN_GAP_MS) return;
    if (m.t !== 'p' || typeof m.d !== 'object' || m.d === null || Array.isArray(m.d)) return;
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
