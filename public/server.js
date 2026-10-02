const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const crypto = require('crypto');
const path = require('path');
const fs = require('fs');

const app = express();
const server = http.createServer(app);
const io = new Server(server);
const PORT = process.env.PORT || 3000;
const DB_FILE = path.join(__dirname, 'rooms.json');

let rooms = {};
try { rooms = JSON.parse(fs.readFileSync(DB_FILE, 'utf8')); } catch (_) { rooms = {}; }

function save() {
  fs.writeFileSync(DB_FILE, JSON.stringify(rooms, null, 2));
}
function makeCode() {
  let c;
  do { c = 'VP3-' + crypto.randomBytes(3).toString('hex').toUpperCase(); } while (rooms[c]);
  return c;
}
function publicRoom(room) {
  return {code:room.code, entries:room.entries, members:room.members, createdAt:room.createdAt};
}

app.use(express.static(path.join(__dirname, 'Public')));
app.get('/health', (_, res) => res.json({ok:true}));

io.on('connection', socket => {
  socket.on('createRoom', (_, cb) => {
    const c = makeCode();
    rooms[c] = {
      code:c, owner:socket.id, entries:[], members:{},
      createdAt:new Date().toISOString()
    };
    rooms[c].members[socket.id] = {role:'owner', name:'Owner'};
    socket.data.roomCode = c;
    socket.join(c);
    save();
    cb({ok:true, room:publicRoom(rooms[c]), role:'owner'});
  });

  socket.on('joinRoom', (payload, cb) => {
    const c = String(payload?.code || '').trim().toUpperCase();
    const room = rooms[c];
    if (!room) return cb({ok:false,error:'Room not found. Check the code.'});
    room.members[socket.id] = {role:'member', name:String(payload?.name || 'Member').slice(0,40)};
    socket.data.roomCode = c;
    socket.join(c);
    save();
    cb({ok:true, room:publicRoom(room), role:'member'});
    io.to(c).emit('roomUpdate', publicRoom(room));
  });

  socket.on('addEntry', (payload, cb) => {
    const c=socket.data.roomCode, room=rooms[c];
    if (!room) return cb?.({ok:false,error:'Join a room first.'});
    const q506=Number(payload?.q506||0), q507=Number(payload?.q507||0);
    if ((!q506 && !q507) || q506<0 || q507<0)
      return cb?.({ok:false,error:'Enter a valid 506 or 507 quantity.'});
    room.entries.push({
      id:crypto.randomUUID(), q506, q507,
      at:new Date().toISOString(),
      by:room.members[socket.id]?.name||'Member'
    });
    save();
    io.to(c).emit('roomUpdate', publicRoom(room));
    cb?.({ok:true});
  });

  socket.on('deleteEntry', (payload, cb) => {
    const c=socket.data.roomCode, room=rooms[c];
    if (!room) return cb?.({ok:false,error:'Join a room first.'});
    room.entries=room.entries.filter(e=>e.id!==payload?.id);
    save();
    io.to(c).emit('roomUpdate', publicRoom(room));
    cb?.({ok:true});
  });

  socket.on('closeRoom', (_, cb) => {
    const c=socket.data.roomCode, room=rooms[c];
    if (!room || room.owner!==socket.id) return cb?.({ok:false,error:'Owner only'});
    io.to(c).emit('roomClosed');
    delete rooms[c];
    save();
    cb?.({ok:true});
  });

  socket.on('disconnect', () => {
    const c=socket.data.roomCode, room=rooms[c];
    if (!room) return;
    delete room.members[socket.id];
    if (room.owner===socket.id) {
      io.to(c).emit('roomClosed');
      delete rooms[c];
    } else {
      io.to(c).emit('roomUpdate', publicRoom(room));
    }
    save();
  });
});

server.listen(PORT, ()=>console.log(`Varroc Live running on port ${PORT}`));
