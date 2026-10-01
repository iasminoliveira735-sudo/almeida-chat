import express from 'express';
import http from 'http';
import { WebSocketServer } from 'ws';
import crypto from 'crypto';

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

app.use(express.static('public'));
app.get('/health', (_req, res) => res.json({ ok: true, service: 'chat-server' }));

const clients = new Map();
const rooms = new Map();

function send(ws, data) {
  if (ws.readyState === 1) ws.send(JSON.stringify(data));
}

function roomUsers(room) {
  return [...(rooms.get(room) || [])]
    .map(id => clients.get(id))
    .filter(Boolean)
    .map(c => ({ id: c.id, name: c.name }));
}

function broadcast(room, data, exceptId = null) {
  for (const id of rooms.get(room) || []) {
    if (id === exceptId) continue;
    const c = clients.get(id);
    if (c) send(c.ws, data);
  }
}

// Remove a client from its current room, but keep the client connection alive.
function leaveRoom(id) {
  const c = clients.get(id);
  if (!c || !c.room) return;
  const oldRoom = c.room;
  const set = rooms.get(oldRoom);
  if (set) {
    set.delete(id);
    if (!set.size) rooms.delete(oldRoom);
    else broadcast(oldRoom, { type: 'users', users: roomUsers(oldRoom) });
  }
  c.room = null;
}

wss.on('connection', ws => {
  const id = crypto.randomUUID();
  clients.set(id, { id, ws, name: 'Usuário', room: null });
  send(ws, { type: 'connected', id });

  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }

    const c = clients.get(id);
    if (!c) return;

    if (msg.type === 'join') {
      leaveRoom(id);

      c.room = String(msg.room || 'geral').trim().slice(0, 60) || 'geral';
      c.name = String(msg.name || 'Usuário').trim().slice(0, 30) || 'Usuário';

      if (!rooms.has(c.room)) rooms.set(c.room, new Set());
      rooms.get(c.room).add(id);

      send(ws, {
        type: 'joined',
        room: c.room,
        me: { id, name: c.name },
        users: roomUsers(c.room)
      });
      broadcast(c.room, { type: 'users', users: roomUsers(c.room) });
      return;
    }

    if (msg.type === 'message' && c.room) {
      const text = String(msg.text || '').trim().slice(0, 2000);
      if (!text) return;

      const message = {
        type: 'message',
        id: crypto.randomUUID(),
        from: id,
        name: c.name,
        text,
        time: new Date().toISOString()
      };

      send(ws, message);
      broadcast(c.room, message, id);
    }
  });

  ws.on('close', () => {
    leaveRoom(id);
    clients.delete(id);
  });
});

const port = process.env.PORT || 3000;
server.listen(port, () => console.log(`Server running on ${port}`));
