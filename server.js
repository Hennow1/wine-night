const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const crypto = require('crypto');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

const HOST_PASSWORD = process.env.HOST_PASSWORD || 'winehost';

let state = {
  participants: {},     // id -> { id, name, isHost, socketId }
  tastingQueue: [],     // ordered array of participant IDs (wine owners)
  currentIndex: -1,    // index into tastingQueue, -1 = lobby
  ratings: {},          // ownerId -> { raterId -> { score, notes } }
  phase: 'lobby',      // lobby | tasting | revealed
};

const disconnectTimers = {}; // participantId -> setTimeout handle

function broadcastState() {
  io.emit('state', publicState());
}

function publicState() {
  const participants = Object.values(state.participants);
  const currentOwnerId = state.currentIndex >= 0 ? state.tastingQueue[state.currentIndex] : null;
  const currentOwner = currentOwnerId ? state.participants[currentOwnerId] : null;

  const ratedIds = currentOwnerId
    ? Object.keys(state.ratings[currentOwnerId] || {})
    : [];

  const results = state.phase === 'revealed' ? buildResults() : null;

  // Which participant IDs have already been tasted (or are being tasted)
  const tastedOwnerIds = state.tastingQueue.slice(0, state.currentIndex + 1);
  // Which are still in the queue after current
  const remainingOwnerIds = state.tastingQueue.slice(state.currentIndex + 1);
  // Which haven't been queued at all
  const queuedIds = new Set(state.tastingQueue);
  const unqueuedParticipants = participants.filter(p => !queuedIds.has(p.id));

  return {
    phase: state.phase,
    participants: participants.map(p => ({ id: p.id, name: p.name, isHost: p.isHost })),
    currentIndex: state.currentIndex,
    currentOwner: currentOwner ? { id: currentOwner.id, name: currentOwner.name } : null,
    tastingQueue: state.tastingQueue,
    tastedOwnerIds,
    remainingOwnerIds,
    unqueuedParticipants: unqueuedParticipants.map(p => ({ id: p.id, name: p.name })),
    ratedParticipantIds: ratedIds,
    totalInQueue: state.tastingQueue.length,
    results,
  };
}

function buildResults() {
  return state.tastingQueue.map(ownerId => {
    const owner = state.participants[ownerId];
    const wineRatings = state.ratings[ownerId] || {};
    const entries = Object.values(wineRatings);
    const scores = entries.map(e => e.score);
    const avg = scores.length ? (scores.reduce((a, b) => a + b, 0) / scores.length) : 0;
    return {
      owner: { id: ownerId, name: owner?.name || 'Unknown' },
      ratings: entries.map(e => {
        const p = state.participants[e.raterId];
        return { raterName: p?.name || 'Unknown', score: e.score, notes: e.notes };
      }),
      average: Math.round(avg * 10) / 10,
      count: scores.length,
    };
  }).sort((a, b) => b.average - a.average);
}

app.use(express.static(path.join(__dirname, 'public')));

io.on('connection', (socket) => {
  socket.on('join', ({ name, password }) => {
    if (password && password !== HOST_PASSWORD) {
      socket.emit('joinError', 'Incorrect host password.');
      return;
    }
    const isHost = password === HOST_PASSWORD;
    const id = crypto.randomUUID();
    state.participants[id] = { id, name, isHost, socketId: socket.id };
    socket.data.participantId = id;
    socket.emit('joined', { id, isHost });
    broadcastState();
  });

  // Host: move from lobby to tasting (shows picker)
  socket.on('startTasting', () => {
    const p = state.participants[socket.data.participantId];
    if (!p?.isHost) return;
    state.phase = 'tasting';
    broadcastState();
  });

  // Host: set the next wine owner and advance
  socket.on('selectNext', ({ ownerId }) => {
    const p = state.participants[socket.data.participantId];
    if (!p?.isHost) return;
    // Add to queue if not already there
    if (!state.tastingQueue.includes(ownerId)) {
      state.tastingQueue.push(ownerId);
    }
    // If we haven't started, start now; otherwise move to this person
    state.currentIndex = state.tastingQueue.indexOf(ownerId);
    state.phase = 'tasting';
    broadcastState();
  });

  // Host: go back to previous wine
  socket.on('prevWine', () => {
    const p = state.participants[socket.data.participantId];
    if (!p?.isHost || state.currentIndex <= 0) return;
    state.currentIndex--;
    broadcastState();
  });

  // Host: reveal
  socket.on('reveal', () => {
    const p = state.participants[socket.data.participantId];
    if (!p?.isHost) return;
    state.phase = 'revealed';
    broadcastState();
  });

  // Host: reset
  socket.on('reset', () => {
    const p = state.participants[socket.data.participantId];
    if (!p?.isHost) return;
    state = { participants: {}, tastingQueue: [], currentIndex: -1, ratings: {}, phase: 'lobby' };
    broadcastState();
  });

  // Host: kick a participant
  socket.on('kick', ({ participantId }) => {
    const p = state.participants[socket.data.participantId];
    if (!p?.isHost) return;
    const target = state.participants[participantId];
    if (!target || target.isHost) return;
    if (disconnectTimers[participantId]) {
      clearTimeout(disconnectTimers[participantId]);
      delete disconnectTimers[participantId];
    }
    const targetSocket = io.sockets.sockets.get(target.socketId);
    if (targetSocket) targetSocket.emit('kicked');
    delete state.participants[participantId];
    broadcastState();
  });

  // Submit rating (anyone except the wine's owner)
  socket.on('submitRating', ({ ownerId, score, notes }) => {
    const raterId = socket.data.participantId;
    if (!raterId || raterId === ownerId) return;
    if (!state.ratings[ownerId]) state.ratings[ownerId] = {};
    state.ratings[ownerId][raterId] = { raterId, score: Number(score), notes: notes || '' };
    broadcastState();
  });

  // Reconnecting client restores their participant slot instead of creating a new one
  socket.on('rejoin', ({ id }) => {
    if (disconnectTimers[id]) {
      clearTimeout(disconnectTimers[id]);
      delete disconnectTimers[id];
    }
    const existing = state.participants[id];
    if (existing) {
      existing.socketId = socket.id;
      socket.data.participantId = id;
      socket.emit('joined', { id, isHost: existing.isHost });
    } else {
      socket.emit('sessionLost');
    }
    broadcastState();
  });

  socket.on('disconnect', () => {
    const pid = socket.data.participantId;
    if (!pid) return;
    // Grace period: give 10s to reconnect before removing from state
    disconnectTimers[pid] = setTimeout(() => {
      delete state.participants[pid];
      delete disconnectTimers[pid];
      broadcastState();
    }, 10000);
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Wine Night running on http://localhost:${PORT}`);
  console.log(`Host password: ${HOST_PASSWORD}`);
});
