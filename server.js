const express=require('express');
const http=require('http');
const {Server}=require('socket.io');
const path=require('path');
const fs=require('fs');
const crypto=require('crypto');
const app=express();
const server=http.createServer(app);
const io=new Server(server,{cors:{origin:true,credentials:true},transports:['websocket','polling']});
const PORT=process.env.PORT||3000;
const PUBLIC_DIR=path.join(__dirname,'public');
const DB_FILE=path.join(__dirname,'rooms.json');
let rooms={};
try{if(fs.existsSync(DB_FILE))rooms=JSON.parse(fs.readFileSync(DB_FILE,'utf8'))||{};}catch(_){rooms={};}
function save(){try{fs.writeFileSync(DB_FILE,JSON.stringify(rooms,null,2));}catch(_) {}}
function makeCode(){let c;do{c='VP3-'+crypto.randomBytes(3).toString('hex').toUpperCase();}while(rooms[c]);return c;}
function normalizeCode(x){return String(x||'').trim().toUpperCase();}
function cleanName(x){return String(x||'').trim().replace(/\s+/g,' ').slice(0,80)||'Member';}
function publicRoom(r){return {code:r.code,encrypted:r.encrypted||null,createdAt:r.createdAt};}
app.use(express.static(PUBLIC_DIR));
app.get('/',(_,res)=>res.sendFile(path.join(PUBLIC_DIR,'index.html')));
app.get('/health',(_,res)=>res.json({ok:true,rooms:Object.keys(rooms).length,e2ee:true}));

io.on('connection',socket=>{
  socket.on('createRoom',(p,cb)=>{
    const name=cleanName(p&&p.name), code=makeCode(), ownerToken=crypto.randomBytes(18).toString('hex');
    rooms[code]={code,ownerToken,encrypted:null,members:{},createdAt:new Date().toISOString()};
    rooms[code].members[socket.id]={role:'owner',name};
    socket.data.roomCode=code;socket.data.role='owner';socket.data.name=name;socket.data.ownerToken=ownerToken;save();
    cb&&cb({ok:true,room:publicRoom(rooms[code]),ownerToken});
  });
  socket.on('joinRoom',(p,cb)=>{
    const code=normalizeCode(p&&p.code),room=rooms[code];
    if(!room)return cb&&cb({ok:false,error:'Invalid room code.'});
    socket.join(code);socket.data.roomCode=code;socket.data.role='member';socket.data.name=cleanName(p&&p.name);socket.data.ownerToken='';
    room.members[socket.id]={role:'member',name:socket.data.name};save();
    cb&&cb({ok:true,room:publicRoom(room),role:'member'});
    io.to(code).emit('roomUpdate',publicRoom(room));
  });
  socket.on('restoreRoom',(p,cb)=>{
    const code=normalizeCode(p&&p.code),token=String(p&&p.ownerToken||'');
    if(!code||!token)return cb&&cb({ok:false,error:'Room recovery information missing.'});
    if(rooms[code]){
      const room=rooms[code];
      if(room.ownerToken!==token)return cb&&cb({ok:false,error:'Owner recovery key invalid.'});
      socket.join(code);socket.data.roomCode=code;socket.data.role='owner';socket.data.name=cleanName(p&&p.name);socket.data.ownerToken=token;room.members[socket.id]={role:'owner'};
      // If Render restarted and the room record survived but its encrypted payload
      // is empty, recover the owner's last encrypted snapshot without needing
      // plaintext on the server. Never overwrite an existing ciphertext.
      const snap=p&&p.snapshot;
      if(!room.encrypted && snap && snap.code===code && snap.encrypted && typeof snap.encrypted.data==='string' && typeof snap.encrypted.iv==='string'){
        room.encrypted={iv:snap.encrypted.iv,data:snap.encrypted.data};
      }
      save();
      return cb&&cb({ok:true,room:publicRoom(room),role:'owner',ownerToken:token,recovered:false});
    }
    // Recovery intentionally restores only encrypted ciphertext. Server never needs plaintext.
    const snap=p&&p.snapshot;
    if(!snap||snap.code!==code||!snap.encrypted)return cb&&cb({ok:false,error:'Saved encrypted room snapshot is invalid.'});
    const recovered={code,ownerToken:token,encrypted:snap.encrypted,members:{},createdAt:snap.createdAt||new Date().toISOString()};
    recovered.members[socket.id]={role:'owner'};rooms[code]=recovered;
    socket.join(code);socket.data.roomCode=code;socket.data.role='owner';socket.data.name=cleanName(p&&p.name);socket.data.ownerToken=token;save();
    cb&&cb({ok:true,room:publicRoom(recovered),role:'owner',ownerToken:token,recovered:true});
    io.to(code).emit('roomUpdate',publicRoom(recovered));
  });
  socket.on('rejoinRoom',(p,cb)=>{
    const code=normalizeCode(p&&p.code),room=rooms[code];
    if(!room)return cb&&cb({ok:false,error:'Room no longer exists.'});
    const token=String(p&&p.ownerToken||'');
    const requestedRole=String(p&&p.role||'member');
    let role='member';
    if(room.ownerToken && token===room.ownerToken) role='owner';
    else if(requestedRole==='owner') return cb&&cb({ok:false,error:'Owner recovery key missing or invalid.'});
    socket.join(code);socket.data.roomCode=code;socket.data.role=role;socket.data.name=cleanName(p&&p.name);socket.data.ownerToken=role==='owner'?token:'';
    room.members[socket.id]={role,name:socket.data.name};save();
    cb&&cb({ok:true,room:publicRoom(room),role});
    io.to(code).emit('roomUpdate',publicRoom(room));
  });
  socket.on('syncRoom',(p,cb)=>{
    const code=socket.data.roomCode,room=rooms[code];
    if(!room)return cb&&cb({ok:false,error:'Room not found.'});
    if(socket.data.role!=='owner')return cb&&cb({ok:false,error:'View-only access: only owner can change room data.'});
    const enc=p&&p.encrypted;
    if(!enc||typeof enc.data!=='string'||typeof enc.iv!=='string')return cb&&cb({ok:false,error:'Invalid encrypted room data.'});
    if(enc.data.length>2000000)return cb&&cb({ok:false,error:'Encrypted room data too large.'});
    room.encrypted={iv:enc.iv,data:enc.data};save();
    const out=publicRoom(room);cb&&cb({ok:true,room:out});io.to(code).emit('roomUpdate',out);
  });
  socket.on('closeRoom',(_,cb)=>{
    const code=socket.data.roomCode,room=rooms[code];
    if(!room)return cb&&cb({ok:false,error:'Room not found.'});
    if(socket.data.role!=='owner')return cb&&cb({ok:false,error:'Only the owner can close the room.'});
    io.to(code).emit('roomClosed');delete rooms[code];save();cb&&cb({ok:true});
  });
  socket.on('disconnect',()=>{const code=socket.data.roomCode;if(!code||!rooms[code])return;delete rooms[code].members[socket.id];save();});
});
server.listen(PORT,'0.0.0.0',()=>console.log('Varroc HP20 Material Control E2EE running on port '+PORT));
