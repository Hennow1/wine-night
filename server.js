const express = require('express');
const http = require('http');
const fs = require('fs');
const { Server } = require('socket.io');
const path = require('path');
const crypto = require('crypto');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

const HOST_PASSWORD = process.env.HOST_PASSWORD || 'winehost';
const STATE_FILE = process.env.STATE_FILE || path.join(__dirname, '.session-state.json');

let state = {
  participants: {},  // id -> { id, name, isHost, socketId, connected }
  wineOwners: [],    // [id, ...] — set once when tasting starts
  ratings: {},       // ownerId -> { raterId -> { score, notes } }
  phase: 'lobby',   // lobby | tasting | revealed
};

// Restore state on startup (survives process crashes; needs a Railway Volume for cross-deploy persistence)
try {
  if (fs.existsSync(STATE_FILE)) {
    const saved = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
    state = saved;
    // Mark everyone as disconnected — they'll reconnect and rejoin
    Object.values(state.participants).forEach(p => { p.connected = false; p.socketId = null; });
    console.log(`Restored state: ${Object.keys(state.participants).length} participants, phase=${state.phase}`);
  }
} catch (e) {
  console.log('No saved state, starting fresh:', e.message);
}

// Persist every 30 seconds
setInterval(() => {
  try { fs.writeFileSync(STATE_FILE, JSON.stringify(state)); } catch (_) {}
}, 30000);

// Returns ratings that a given participant has submitted (sent back on join/rejoin so the client can pre-fill)
function getMyRatings(raterId) {
  const result = {};
  for (const [ownerId, raterMap] of Object.entries(state.ratings)) {
    if (raterMap[raterId]) {
      result[ownerId] = { score: raterMap[raterId].score, notes: raterMap[raterId].notes };
    }
  }
  return result;
}

function broadcastState() {
  io.emit('state', publicState());
}

function publicState() {
  const ratingProgress = {};
  for (const ownerId of state.wineOwners) {
    ratingProgress[ownerId] = Object.keys(state.ratings[ownerId] || {});
  }
  return {
    phase: state.phase,
    participants: Object.values(state.participants).map(p => ({
      id: p.id, name: p.name, isHost: p.isHost, connected: p.connected,
    })),
    wineOwners: state.wineOwners
      .filter(id => state.participants[id])
      .map(id => ({ id, name: state.participants[id].name })),
    ratingProgress,  // ownerId -> [raterId, ...]
    results: state.phase === 'revealed' ? buildResults() : null,
  };
}

function buildResults() {
  return state.wineOwners
    .filter(id => state.participants[id])
    .map(ownerId => {
      const owner = state.participants[ownerId];
      const entries = Object.values(state.ratings[ownerId] || {});
      const scores = entries.map(e => e.score);
      const avg = scores.length ? scores.reduce((a, b) => a + b, 0) / scores.length : 0;
      return {
        owner: { id: ownerId, name: owner.name },
        ratings: entries.map(e => ({
          raterName: state.participants[e.raterId]?.name || 'Unknown',
          score: e.score,
          notes: e.notes,
        })),
        average: Math.round(avg * 10) / 10,
        count: scores.length,
      };
    })
    .sort((a, b) => b.average - a.average);
}

app.use(express.static(path.join(__dirname, 'public')));

io.on('connection', (socket) => {
  socket.on('join', ({ name, password }) => {
    if (password && password !== HOST_PASSWORD) {
      socket.emit('joinError', 'Incorrect host password.');
      return;
    }
    const isHost = password === HOST_PASSWORD;
    const trimmed = (name || '').trim();

    // Reclaim a disconnected participant with the same name — restores their ratings attribution
    const reclaim = Object.values(state.participants).find(
      p => p.name.toLowerCase() === trimmed.toLowerCase() && p.isHost === isHost && !p.connected
    );

    if (reclaim) {
      reclaim.socketId = socket.id;
      reclaim.connected = true;
      socket.data.participantId = reclaim.id;
      socket.emit('joined', { id: reclaim.id, isHost: reclaim.isHost, myRatings: getMyRatings(reclaim.id) });
    } else {
      const id = crypto.randomUUID();
      state.participants[id] = { id, name: trimmed, isHost, socketId: socket.id, connected: true };
      socket.data.participantId = id;
      socket.emit('joined', { id, isHost, myRatings: {} });
    }
    broadcastState();
  });

  // Reconnecting socket restores session without creating a duplicate participant
  socket.on('rejoin', ({ id }) => {
    const existing = state.participants[id];
    if (existing) {
      existing.socketId = socket.id;
      existing.connected = true;
      socket.data.participantId = id;
      socket.emit('joined', { id, isHost: existing.isHost, myRatings: getMyRatings(id) });
    } else {
      socket.emit('sessionLost');
    }
    broadcastState();
  });

  // Host: open tasting — all participants become wine owners simultaneously
  socket.on('startTasting', () => {
    const p = state.participants[socket.data.participantId];
    if (!p?.isHost) return;
    state.wineOwners = Object.keys(state.participants);
    state.phase = 'tasting';
    broadcastState();
  });

  // Host: reveal all scores
  socket.on('reveal', () => {
    const p = state.participants[socket.data.participantId];
    if (!p?.isHost) return;
    state.phase = 'revealed';
    broadcastState();
  });

  // Host: full reset
  socket.on('reset', () => {
    const p = state.participants[socket.data.participantId];
    if (!p?.isHost) return;
    state = { participants: {}, wineOwners: [], ratings: {}, phase: 'lobby' };
    broadcastState();
  });

  // Host: remove a participant
  socket.on('kick', ({ participantId }) => {
    const p = state.participants[socket.data.participantId];
    if (!p?.isHost) return;
    const target = state.participants[participantId];
    if (!target || target.isHost) return;
    const targetSocket = io.sockets.sockets.get(target.socketId);
    if (targetSocket) targetSocket.emit('kicked');
    delete state.participants[participantId];
    state.wineOwners = state.wineOwners.filter(id => id !== participantId);
    broadcastState();
  });

  // Anyone (except the wine's owner) submits or updates a rating
  socket.on('submitRating', ({ ownerId, score, notes }) => {
    const raterId = socket.data.participantId;
    if (!raterId || raterId === ownerId) return;
    if (!state.ratings[ownerId]) state.ratings[ownerId] = {};
    state.ratings[ownerId][raterId] = { raterId, score: Number(score), notes: notes || '' };
    broadcastState();
  });

  // On disconnect: mark as away but keep in roster so ratings stay attributed
  socket.on('disconnect', () => {
    const pid = socket.data.participantId;
    if (!pid || !state.participants[pid]) return;
    state.participants[pid].connected = false;
    state.participants[pid].socketId = null;
    broadcastState();
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Wine Night running on http://localhost:${PORT}`);
  console.log(`Host password: ${HOST_PASSWORD}`);
});
