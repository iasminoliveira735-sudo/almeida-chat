import express from 'express';
import http from 'http';
import { WebSocketServer } from 'ws';
import crypto from 'crypto';
const app=express(), server=http.createServer(app), wss=new WebSocketServer({server});
app.use(express.static('public'));
app.get('/health',(_,res)=>res.json({ok:true}));
const clients=new Map(), rooms=new Map();
const send=(ws,x)=>ws.readyState===1&&ws.send(JSON.stringify(x));
const users=r=>[...(rooms.get(r)||[])].map(id=>clients.get(id)).filter(Boolean).map(c=>({id:c.id,name:c.name}));
const broadcast=(r,x,except)=>[...(rooms.get(r)||[])].forEach(id=>{if(id!==except){const c=clients.get(id);if(c)send(c.ws,x)}});
function leave(id){const c=clients.get(id);if(!c)return;const s=rooms.get(c.room);if(s){s.delete(id);if(!s.size)rooms.delete(c.room);else broadcast(c.room,{type:'users',users:users(c.room)})}clients.delete(id)}
wss.on('connection',ws=>{const id=crypto.randomUUID();clients.set(id,{id,ws,name:'Usuário',room:null});send(ws,{type:'connected',id});ws.on('message',raw=>{let m;try{m=JSON.parse(raw)}catch{return}const c=clients.get(id);if(!c)return;if(m.type==='join'){leave(id);c.room=String(m.room||'geral').trim().slice(0,60)||'geral';c.name=String(m.name||'Usuário').trim().slice(0,30)||'Usuário';if(!rooms.has(c.room))rooms.set(c.room,new Set);rooms.get(c.room).add(id);send(ws,{type:'joined',room:c.room,me:{id,name:c.name},users:users(c.room)});broadcast(c.room,{type:'users',users:users(c.room)});return}if(m.type==='message'&&c.room){const text=String(m.text||'').trim().slice(0,2000);if(!text)return;const x={type:'message',id:crypto.randomUUID(),from:id,name:c.name,text,time:new Date().toISOString()};send(ws,x);broadcast(c.room,x,id)}});ws.on('close',()=>leave(id))});
server.listen(process.env.PORT||3000);
