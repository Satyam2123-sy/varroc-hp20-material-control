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
const BUILD_ID='V36-ACTION-SESSION-FIX';
const PUBLIC_DIR=path.join(__dirname,'public');
const DB_FILE=path.join(__dirname,'rooms.json');
let rooms={};
try{if(fs.existsSync(DB_FILE))rooms=JSON.parse(fs.readFileSync(DB_FILE,'utf8'))||{};}catch(_){rooms={};}
function save(){try{fs.writeFileSync(DB_FILE,JSON.stringify(rooms,null,2));}catch(_) {}}
function makeToken(){return crypto.randomBytes(18).toString('hex');}
function makeCode(){let c;do{c='VP3-'+crypto.randomBytes(3).toString('hex').toUpperCase();}while(rooms[c]);return c;}
function normalizeCode(x){return String(x||'').trim().toUpperCase();}
function cleanName(x){return String(x||'').trim().replace(/\s+/g,' ').slice(0,80)||'Member';}
function ensureData(r){r.roomData=r.roomData||{};r.roomData.code=r.code;r.roomData.entries=Array.isArray(r.roomData.entries)?r.roomData.entries:[];r.roomData.breakdowns=Array.isArray(r.roomData.breakdowns)?r.roomData.breakdowns:[];r.roomData.attendance=Array.isArray(r.roomData.attendance)?r.roomData.attendance:[];return r;}
function publicRoom(r){ensureData(r);const members={};for(const m of Object.values(r.members||{})){if(m&&m.connected&&m.name)members[m.memberToken||m.socketId||m.name]={name:m.name,role:m.role==='owner'?'owner':'member'};}return {code:r.code,roomData:{...r.roomData,members},members,createdAt:r.createdAt,build:BUILD_ID};}
function addSession(r,socket,role,name,memberToken){r.members=r.members||{};r.members[memberToken]={memberToken,socketId:socket.id,role,name,connected:true};socket.data.roomCode=r.code;socket.data.role=role;socket.data.name=name;socket.data.memberToken=memberToken;socket.data.ownerToken=role==='owner'?r.ownerToken:'';socket.join(r.code);}
function broadcast(r){save();io.to(r.code).emit('roomUpdate',publicRoom(r));}
app.use(express.static(PUBLIC_DIR));
app.get('/',(_,res)=>res.sendFile(path.join(PUBLIC_DIR,'index.html')));
app.get('/health',(_,res)=>res.json({ok:true,build:BUILD_ID,rooms:Object.keys(rooms).length,e2ee:false}));
app.get('/build',(_,res)=>res.json({build:BUILD_ID}));
io.on('connection',socket=>{
  socket.on('createRoom',(p,cb)=>{const name=cleanName(p&&p.name),code=makeCode(),ownerToken=makeToken();const r={code,ownerToken,ownerName:name,roomData:{code,entries:[],breakdowns:[],attendance:[]},members:{},createdAt:new Date().toISOString()};rooms[code]=r;addSession(r,socket,'owner',name,ownerToken);save();cb&&cb({ok:true,room:publicRoom(r),ownerToken});broadcast(r);});
  socket.on('joinRoom',(p,cb)=>{const code=normalizeCode(p&&p.code),r=rooms[code];if(!r)return cb&&cb({ok:false,error:'Invalid room code.'});const name=cleanName(p&&p.name),memberToken=makeToken();ensureData(r);addSession(r,socket,'member',name,memberToken);save();cb&&cb({ok:true,room:publicRoom(r),role:'member',memberToken});broadcast(r);});
  socket.on('rejoinRoom',(p,cb)=>{const code=normalizeCode(p&&p.code),r=rooms[code];if(!r)return cb&&cb({ok:false,error:'Room no longer exists.'});ensureData(r);const name=cleanName(p&&p.name);let role='member',token=String(p&&p.ownerToken||'');
    if(token && token===r.ownerToken) role='owner';
    else if(String(p&&p.role||'')==='owner' && name===cleanName(r.ownerName||'')){role='owner';token=r.ownerToken;}
    else {token=String(p&&p.memberToken||'');const old=r.members&&r.members[token];if(!old){const byName=Object.values(r.members||{}).find(m=>m&&m.role==='member'&&cleanName(m.name)===name);if(byName)token=byName.memberToken;}if(!token)token=makeToken();role='member';}
    addSession(r,socket,role,name,token);save();cb&&cb({ok:true,room:publicRoom(r),role,ownerToken:role==='owner'?r.ownerToken:'',memberToken:role==='member'?token:''});broadcast(r);
  });
  socket.on('restoreRoom',(p,cb)=>{const code=normalizeCode(p&&p.code),token=String(p&&p.ownerToken||''),snap=p&&p.snapshot;if(!code||!token)return cb&&cb({ok:false,error:'Recovery information missing.'});let r=rooms[code];if(r && r.ownerToken!==token)return cb&&cb({ok:false,error:'Owner recovery key invalid.'});if(!r){if(!snap||snap.code!==code||!snap.roomData)return cb&&cb({ok:false,error:'Saved room snapshot invalid.'});r={code,ownerToken:token,ownerName:cleanName(p&&p.name),roomData:snap.roomData,members:{},createdAt:snap.createdAt||new Date().toISOString()};rooms[code]=r;}ensureData(r);addSession(r,socket,'owner',cleanName(p&&p.name),r.ownerToken);save();cb&&cb({ok:true,room:publicRoom(r),role:'owner',ownerToken:r.ownerToken,recovered:true});broadcast(r);});
  function ownerRoom(socket,p){
    p=p||{};
    const code=normalizeCode(p.code||socket.data.roomCode);
    const token=String(p.ownerToken||socket.data.ownerToken||'');
    const name=cleanName(p.name||socket.data.name||'');
    if(!code||!token)return null;
    let r=rooms[code];
    if(!r && p.snapshot && p.snapshot.code===code && p.snapshot.roomData){
      r={code,ownerToken:token,ownerName:name,roomData:p.snapshot.roomData,members:{},createdAt:p.snapshot.createdAt||new Date().toISOString()};
      rooms[code]=r;
    }
    if(!r || r.ownerToken!==token)return null;
    ensureData(r); addSession(r,socket,'owner',name,r.ownerToken); return r;
  }
  socket.on('addEntry',(p,cb)=>{
    p=p||{}; const entry=p.entry||p; const r=ownerRoom(socket,p);
    if(!r)return cb&&cb({ok:false,error:'Owner session not connected. Reconnect the room and try again.'});
    r.roomData.entries.push({id:String(entry&&entry.id||makeToken()),at:entry&&entry.at||new Date().toISOString(),shift:entry&&entry.shift||'',q506:Number(entry&&entry.q506)||0,q507:Number(entry&&entry.q507)||0,by:cleanName(entry&&entry.by||socket.data.name)});
    const out=publicRoom(r);broadcast(r);cb&&cb({ok:true,room:out});
  });
  function ownerMutation(socket,p,cb,fn){const r=ownerRoom(socket,p);if(!r)return cb&&cb({ok:false,error:'Owner session not connected. Reconnect the room and try again.'});fn(r);const out=publicRoom(r);broadcast(r);cb&&cb({ok:true,room:out});}
  socket.on('addBreakdown',(x,cb)=>ownerMutation(socket,x,cb,r=>{r.roomData.breakdowns.push({id:String(x&&x.id||makeToken()),at:x&&x.at||new Date().toISOString(),shift:x&&x.shift||'',text:String(x&&x.text||'').slice(0,1000),by:cleanName(x&&x.by||socket.data.name)});}));
  socket.on('addAttendance',(x,cb)=>ownerMutation(socket,x,cb,r=>{r.roomData.attendance.push({id:String(x&&x.id||makeToken()),at:x&&x.at||new Date().toISOString(),shift:x&&x.shift||'',name:cleanName(x&&x.name),by:cleanName(x&&x.by||socket.data.name)});}));
  socket.on('deleteEntry',(p,cb)=>ownerMutation(socket,p,cb,r=>{const id=p&&p.id!==undefined?p.id:p;r.roomData.entries=r.roomData.entries.filter(x=>x.id!==id);}));
  socket.on('deleteBreakdown',(p,cb)=>ownerMutation(socket,p,cb,r=>{const id=p&&p.id!==undefined?p.id:p;r.roomData.breakdowns=r.roomData.breakdowns.filter(x=>x.id!==id);}));
  socket.on('deleteAttendance',(p,cb)=>ownerMutation(socket,p,cb,r=>{const id=p&&p.id!==undefined?p.id:p;r.roomData.attendance=r.roomData.attendance.filter(x=>x.id!==id);}));
  socket.on('syncRoom',(p,cb)=>ownerMutation(socket,p,cb,r=>{const d=p&&p.room;if(d&&typeof d==='object'){r.roomData={...d,code:r.code,members:undefined};ensureData(r);}}));
  socket.on('closeRoom',(_,cb)=>{const r=rooms[socket.data.roomCode];if(!r)return cb&&cb({ok:false,error:'Room not found.'});if(socket.data.role!=='owner')return cb&&cb({ok:false,error:'Only owner can close the room.'});io.to(r.code).emit('roomClosed');delete rooms[r.code];save();cb&&cb({ok:true});});
  socket.on('disconnect',()=>{const code=socket.data.roomCode,r=rooms[code];if(!r)return;const token=socket.data.memberToken;if(token&&r.members&&r.members[token])r.members[token].connected=false;save();io.to(code).emit('roomUpdate',publicRoom(r));});
});
server.listen(PORT,'0.0.0.0',()=>console.log('Varroc HP20 Material Control running on port '+PORT));
