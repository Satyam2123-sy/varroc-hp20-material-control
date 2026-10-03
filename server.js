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
function publicRoom(r){
  let data=null;
  try{ data=r.roomData?JSON.parse(JSON.stringify(r.roomData)):null; }catch(_){ data=null; }
  const members={};
  for(const m of Object.values(r.members||{})){
    if(m&&m.name) members[m.name]={name:m.name,role:m.role==='owner'?'owner':'member'};
  }
  if(data){ data.members=members; }
  return {code:r.code,roomData:data,members,createdAt:r.createdAt};
}
app.use(express.static(PUBLIC_DIR));
app.get('/',(_,res)=>res.sendFile(path.join(PUBLIC_DIR,'index.html')));
app.get('/health',(_,res)=>res.json({ok:true,rooms:Object.keys(rooms).length,e2ee:false}));

io.on('connection',socket=>{
  socket.on('createRoom',(p,cb)=>{
    const name=cleanName(p&&p.name), code=makeCode(), ownerToken=crypto.randomBytes(18).toString('hex');
    rooms[code]={code,ownerToken,ownerName:name,roomData:{code,entries:[],breakdowns:[],attendance:[],members:{}},members:{},createdAt:new Date().toISOString()};
    rooms[code].members[socket.id]={role:'owner',name};
    rooms[code].roomData.members={[name]:{name,role:'owner'}};
    socket.join(code);
    socket.data.roomCode=code;socket.data.role='owner';socket.data.name=name;socket.data.ownerToken=ownerToken;save();
    cb&&cb({ok:true,room:publicRoom(rooms[code]),ownerToken});
  });
  socket.on('joinRoom',(p,cb)=>{
    const code=normalizeCode(p&&p.code),room=rooms[code];
    if(!room)return cb&&cb({ok:false,error:'Invalid room code.'});
    socket.join(code);socket.data.roomCode=code;socket.data.role='member';socket.data.name=cleanName(p&&p.name);socket.data.ownerToken='';
    room.members[socket.id]={role:'member',name:socket.data.name};
    room.roomData=room.roomData||{code,entries:[],breakdowns:[],attendance:[],members:{}};
    room.roomData.members=room.roomData.members||{};
    room.roomData.members[socket.id+'-'+Date.now()]={name:socket.data.name,role:'member'};
    save();
    cb&&cb({ok:true,room:publicRoom(room),role:'member'});
    io.to(code).emit('roomUpdate',publicRoom(room));
  });
  socket.on('restoreRoom',(p,cb)=>{
    const code=normalizeCode(p&&p.code),token=String(p&&p.ownerToken||'');
    if(!code||!token)return cb&&cb({ok:false,error:'Room recovery information missing.'});
    if(rooms[code]){
      const room=rooms[code];
      if(room.ownerToken!==token){
        const requestedName=cleanName(p&&p.name);
        if(requestedName!==cleanName(room.ownerName)) return cb&&cb({ok:false,error:'Owner recovery key invalid.'});
      }
      socket.join(code);socket.data.roomCode=code;socket.data.role='owner';socket.data.name=cleanName(p&&p.name);socket.data.ownerToken=token;room.members[socket.id]={role:'owner',name:socket.data.name};room.roomData=room.roomData||{code,entries:[],breakdowns:[],attendance:[],members:{}};room.roomData.members=room.roomData.members||{};room.roomData.members[socket.id]={name:socket.data.name,role:'owner'};save();
      return cb&&cb({ok:true,room:publicRoom(room),role:'owner',ownerToken:token,recovered:false});
    }
    const snap=p&&p.snapshot;
    if(!snap||snap.code!==code||!snap.roomData)return cb&&cb({ok:false,error:'Saved room snapshot is invalid.'});
    const recovered={code,ownerToken:token,ownerName:cleanName(p&&p.name),roomData:snap.roomData,members:{},createdAt:snap.createdAt||new Date().toISOString()};
    recovered.members[socket.id]={role:'owner',name:cleanName(p&&p.name)};rooms[code]=recovered;
    socket.join(code);socket.data.roomCode=code;socket.data.role='owner';socket.data.name=cleanName(p&&p.name);socket.data.ownerToken=token;save();
    cb&&cb({ok:true,room:publicRoom(recovered),role:'owner',ownerToken:token,recovered:true});
    io.to(code).emit('roomUpdate',publicRoom(recovered));
  });
  socket.on('rejoinRoom',(p,cb)=>{
    const code=normalizeCode(p&&p.code),room=rooms[code];
    if(!room)return cb&&cb({ok:false,error:'Room no longer exists.'});
    let token=String(p&&p.ownerToken||'');
    const requestedRole=String(p&&p.role||'member');
    let role='member';
    const requestedName=cleanName(p&&p.name);
    const ownerByName=requestedName===cleanName(room.ownerName||'');
    const ownerInSavedMembers=Object.values((room.roomData&&room.roomData.members)||{}).some(m=>m&&m.role==='owner'&&cleanName(m.name)===requestedName);
    if(room.ownerToken && token===room.ownerToken) role='owner';
    else if(requestedRole==='owner' && (ownerByName||ownerInSavedMembers)){
      role='owner';
      token=room.ownerToken;
    } else if(requestedRole==='owner') return cb&&cb({ok:false,error:'Owner recovery information missing.'});
    socket.join(code);socket.data.roomCode=code;socket.data.role=role;socket.data.name=cleanName(p&&p.name);socket.data.ownerToken=role==='owner'?token:'';
    room.members[socket.id]={role,name:socket.data.name};
    room.roomData=room.roomData||{code,entries:[],breakdowns:[],attendance:[],members:{}};
    room.roomData.members=room.roomData.members||{};
    room.roomData.members[socket.id]={name:socket.data.name,role};
    save();
    cb&&cb({ok:true,room:publicRoom(room),role});
    io.to(code).emit('roomUpdate',publicRoom(room));
  });
  socket.on('addEntry',(entry,cb)=>{
    const code=socket.data.roomCode,room=rooms[code];
    if(!room)return cb&&cb({ok:false,error:'Room not found.'});
    if(socket.data.role!=='owner')return cb&&cb({ok:false,error:'View-only access: only owner can add quantity.'});
    if(!entry||typeof entry!=='object')return cb&&cb({ok:false,error:'Invalid entry.'});
    room.roomData=room.roomData||{code,entries:[],breakdowns:[],attendance:[],members:{}};
    room.roomData.entries=Array.isArray(room.roomData.entries)?room.roomData.entries:[];
    room.roomData.entries.push({id:String(entry.id||Date.now()),at:entry.at||new Date().toISOString(),shift:entry.shift||'',q506:Number(entry.q506)||0,q507:Number(entry.q507)||0,by:cleanName(entry.by||socket.data.name)});
    save();
    const out=publicRoom(room); io.to(code).emit('roomUpdate',out); cb&&cb({ok:true,room:out});
  });
  socket.on('syncRoom',(p,cb)=>{
    const code=socket.data.roomCode,room=rooms[code];
    if(!room)return cb&&cb({ok:false,error:'Room not found.'});
    if(socket.data.role!=='owner')return cb&&cb({ok:false,error:'View-only access: only owner can change room data.'});
    const data=p&&p.room;
    if(!data||typeof data!=='object')return cb&&cb({ok:false,error:'Invalid room data.'});
    if(JSON.stringify(data).length>2000000)return cb&&cb({ok:false,error:'Room data too large.'});
    room.roomData=data; save();
    const out=publicRoom(room); cb&&cb({ok:true,room:out}); io.to(code).emit('roomUpdate',out);
  });
  socket.on('closeRoom',(_,cb)=>{
    const code=socket.data.roomCode,room=rooms[code];
    if(!room)return cb&&cb({ok:false,error:'Room not found.'});
    if(socket.data.role!=='owner')return cb&&cb({ok:false,error:'Only the owner can close the room.'});
    io.to(code).emit('roomClosed');delete rooms[code];save();cb&&cb({ok:true});
  });
  socket.on('disconnect',()=>{const code=socket.data.roomCode;if(!code||!rooms[code])return;delete rooms[code].members[socket.id];save();io.to(code).emit('roomUpdate',publicRoom(rooms[code]));});
});
server.listen(PORT,'0.0.0.0',()=>console.log('Varroc HP20 Material Control running on port '+PORT));
