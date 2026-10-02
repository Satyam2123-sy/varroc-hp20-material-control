const express=require('express');
const http=require('http');
const {Server}=require('socket.io');
const crypto=require('crypto');
const path=require('path');
const fs=require('fs');
const app=express();
const server=http.createServer(app);
const io=new Server(server,{transports:['websocket','polling'],cors:{origin:true,credentials:true}});
const PORT=process.env.PORT||3000;
const PUBLIC_DIR=path.join(__dirname,'public');
const DB_FILE=path.join(__dirname,'rooms.json');
let rooms={};
try{if(fs.existsSync(DB_FILE)) rooms=JSON.parse(fs.readFileSync(DB_FILE,'utf8'))||{};}catch(_){rooms={};}
function save(){try{fs.writeFileSync(DB_FILE,JSON.stringify(rooms,null,2));}catch(_) {}}
function normalizeCode(c){return String(c||'').trim().toUpperCase();}
function makeCode(){let c;do{c='VP3-'+crypto.randomBytes(3).toString('hex').toUpperCase();}while(rooms[c]);return c;}
function publicRoom(r){return {code:r.code,entries:r.entries||[],members:r.members||{},createdAt:r.createdAt};}
function shape(r){r.entries=Array.isArray(r.entries)?r.entries:[];r.members=r.members&&typeof r.members==='object'?r.members:{};return r;}
app.use(express.static(PUBLIC_DIR));
app.get('/health',(_,res)=>res.json({ok:true,rooms:Object.keys(rooms).length}));
app.get('/',(_,res)=>res.sendFile(path.join(PUBLIC_DIR,'index.html')));
io.on('connection',socket=>{
 socket.on('createRoom',(_,cb)=>{
  const code=makeCode();const room={code,entries:[],members:{},createdAt:new Date().toISOString()};
  room.members[socket.id]={name:'SATYAM',role:'owner'};rooms[code]=room;socket.data.roomCode=code;socket.data.role='owner';save();
  cb&&cb({ok:true,room:publicRoom(room)});io.to(code).emit('roomUpdate',publicRoom(room));
 });
 socket.on('joinRoom',(p,cb)=>{
  const code=normalizeCode(p&&p.code),room=rooms[code];if(!room)return cb&&cb({ok:false,error:'Invalid room code.'});shape(room);socket.join(code);socket.data.roomCode=code;socket.data.role='member';room.members[socket.id]={name:String(p&&p.name||'Member').slice(0,40),role:'member'};save();cb&&cb({ok:true,room:publicRoom(room)});io.to(code).emit('roomUpdate',publicRoom(room));
 });
 socket.on('rejoinRoom',(p,cb)=>{
  const code=normalizeCode(p&&p.code),room=rooms[code];if(!room)return cb&&cb({ok:false,error:'Room no longer exists.'});shape(room);
  const owner=p&&p.name==='SATYAM';socket.join(code);socket.data.roomCode=code;socket.data.role=owner?'owner':'member';room.members[socket.id]={name:owner?'SATYAM':String(p&&p.name||'Member').slice(0,40),role:owner?'owner':'member'};save();cb&&cb({ok:true,room:publicRoom(room)});io.to(code).emit('roomUpdate',publicRoom(room));
 });
 socket.on('addEntry',(p,cb)=>{
  const code=socket.data.roomCode,room=rooms[code];if(!room)return cb&&cb({ok:false,error:'Room not found.'});
  const q506=Number(p&&p.q506||0),q507=Number(p&&p.q507||0);if(!Number.isFinite(q506)||!Number.isFinite(q507)||q506<0||q507<0||(q506===0&&q507===0))return cb&&cb({ok:false,error:'Invalid quantity.'});
  const m=room.members[socket.id];const entry={id:crypto.randomBytes(8).toString('hex'),at:new Date().toISOString(),q506,q507,by:m?m.name:(socket.data.role==='owner'?'SATYAM':'Member')};room.entries.push(entry);save();cb&&cb({ok:true,entry});io.to(code).emit('roomUpdate',publicRoom(room));
 });
 socket.on('deleteEntry',(p,cb)=>{const code=socket.data.roomCode,room=rooms[code];if(!room)return cb&&cb({ok:false,error:'Room not found.'});room.entries=room.entries.filter(e=>e.id!==String(p&&p.id||''));save();cb&&cb({ok:true});io.to(code).emit('roomUpdate',publicRoom(room));});
 socket.on('closeRoom',(_,cb)=>{const code=socket.data.roomCode,room=rooms[code];if(!room)return cb&&cb({ok:false,error:'Room not found.'});if(socket.data.role!=='owner')return cb&&cb({ok:false,error:'Only the owner can close the room.'});io.to(code).emit('roomClosed');delete rooms[code];save();cb&&cb({ok:true});});
 socket.on('disconnect',()=>{const code=socket.data.roomCode;if(!code||!rooms[code])return;delete rooms[code].members[socket.id];save();io.to(code).emit('roomUpdate',publicRoom(rooms[code]));});
});
server.listen(PORT,'0.0.0.0',()=>console.log('Varroc HP20 Material Control running on port '+PORT));
