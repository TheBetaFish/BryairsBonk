#!/usr/bin/env node
"use strict";
const http=require("http"),fs=require("fs"),path=require("path");
const {createBot,LOG_LEVELS}=require("./src");
const HOST=process.env.HOST||"127.0.0.1",PORT=Number(process.env.PORT||8787);
const ALLOWED_ORIGINS=new Set((process.env.ALLOWED_ORIGINS||"https://thebetafish.github.io,http://127.0.0.1:8787,http://localhost:8787").split(",").map(x=>x.trim()).filter(Boolean));
let bot=null,connecting=false,connected=false,joined=false,currentRoom="",lastError="";
const clients=new Set(),history=[];const MAX_HISTORY=250;
function safePlayer(p){if(!p||typeof p!=="object")return null;return{id:p.id??p.peerID??null,username:String(p.username||p.userName||p.name||"Player").slice(0,64),team:p.team??null,teamName:p.teamName??null,guest:Boolean(p.guest),host:Boolean(p.host),ready:p.ready??null,level:Number.isFinite(Number(p.level))?Number(p.level):null,ping:Number.isFinite(Number(p.ping))?Number(p.ping):null};}
function getPlayers(){try{return bot?bot.getAllPlayers().map(safePlayer).filter(Boolean):[]}catch{return[]}}
function state(){return{backend:true,connecting,connected,joined,room:currentRoom,players:getPlayers(),lastError,inGame:Boolean(bot&&bot.room&&bot.room.inGame),countdown:bot&&bot.room?bot.room.countdown:false,teamsLocked:Boolean(bot&&bot.room&&bot.room.teamsLocked),mode:bot&&bot.room?bot.room.mode||"": "",engine:bot&&bot.room?bot.room.engine||"": "",framecount:bot&&bot.room&&Number.isFinite(Number(bot.room.framecount))?Number(bot.room.framecount):0}}
function broadcast(type,data={}){const item={type,data,time:new Date().toISOString()};history.push(item);if(history.length>MAX_HISTORY)history.shift();const line="data: "+JSON.stringify(item)+"\n\n";for(const res of clients){try{res.write(line)}catch{clients.delete(res)}}}
function attachEvents(instance){
 for(const name of ["ready","connect","disconnect","JOIN","PLAYER_JOIN","PLAYER_LEAVE","CHAT_MESSAGE","TEAM_CHANGE","HOST_TRANSFER","READY_CHANGE","GAME_START","GAME_END","COUNTDOWN","ABORT_COUNTDOWN","MAP_SWITCH","GAMEMODE_CHANGE","ROOM_SHARE_LINK","TEAMLOCK_TOGGLE"]){
  instance.events.on(name,data=>{
   if(name==="connect")connected=true;if(name==="disconnect"){connected=false;joined=false}if(name==="JOIN")joined=true;
   if(name==="CHAT_MESSAGE"&&data)broadcast("chat",{username:String(data.player?.username||"Player").slice(0,64),message:String(data.message||"").slice(0,500)});
   let detail="";if(name==="PLAYER_JOIN")detail=String(data?.player?.username||"Player joined");else if(name==="PLAYER_LEAVE")detail=String(data?.player?.username||"Player left");else if(name==="COUNTDOWN")detail=String(data?.countdown??"");else if(name==="GAMEMODE_CHANGE")detail=String(data?.mode??"");else if(name==="ROOM_SHARE_LINK")detail=String(data?.url??"");
   broadcast("game-event",{name,detail});broadcast("state",state());
  });
 }
 instance.events.on("PACKET",packet=>{try{instance.autoHandlePacket(packet)}catch(e){broadcast("log",{level:"warn",message:"Packet handler: "+e.message})}});
}
function readBody(req){return new Promise((resolve,reject)=>{let raw="";req.on("data",c=>{raw+=c;if(raw.length>32000){reject(Error("Request body too large"));req.destroy()}});req.on("end",()=>{try{resolve(raw?JSON.parse(raw):{})}catch{reject(Error("Invalid JSON body"))}});req.on("error",reject)})}
function send(res,status,data,type="application/json; charset=utf-8"){res.writeHead(status,{"Content-Type":type,"Cache-Control":"no-store","X-Content-Type-Options":"nosniff","Content-Security-Policy":"default-src 'self'; connect-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self'; base-uri 'none'; frame-ancestors 'none'"});res.end(type.startsWith("application/json")?JSON.stringify(data):data)}
const msg=e=>String(e?.message||e||"Unknown error").slice(0,400);
async function api(req,res,url){
 if(req.method==="GET"&&url.pathname==="/api/status")return send(res,200,state());
 if(req.method!=="POST")return send(res,405,{error:"Method not allowed"});
 let body;try{body=await readBody(req)}catch(e){return send(res,400,{error:msg(e)})}
 if(url.pathname==="/api/join"){
  if(connecting||connected||bot)return send(res,409,{error:"A client is already active. Leave the current session first."});
  const room=String(body.room||"").trim(),username=String(body.username||"BryairClient").trim().slice(0,24),password=String(body.password||"").slice(0,100);
  if(!room)return send(res,400,{error:"Enter a room name or share URL."});if(!username)return send(res,400,{error:"Enter a guest display name."});
  connecting=true;joined=false;currentRoom=room;lastError="";broadcast("log",{level:"info",message:"Starting guest session…"});
  try{
   bot=createBot({account:{username,guest:true},PROTOCOL_VERSION:49,logLevel:LOG_LEVELS.WARN});const instance=bot;attachEvents(instance);
   await instance.init();
   const info=/^https?:\/\//i.test(room)?await instance.getAddressFromUrl(room):await instance.getAddressFromRoomName(room);
   if(!info)throw Error("Room lookup returned no result.");currentRoom=String(info.roomname||room);instance.setAddress(info);
   broadcast("log",{level:"info",message:"Room found. Opening game connection…"});
   await instance.connect();if(!instance.connected)throw Error("Game server connection did not become active.");
   await instance.joinRoom(password?{password}:{});
   connecting=false;broadcast("log",{level:"success",message:"Join request sent; waiting for room state…"});broadcast("state",state());return send(res,200,{ok:true,state:state()});
  }catch(e){lastError=msg(e);connecting=false;connected=Boolean(bot&&bot.connected);broadcast("log",{level:"error",message:"Join failed: "+lastError});try{if(bot){await bot.leaveGame();bot.disconnect()}}catch{}bot=null;connected=false;joined=false;currentRoom="";broadcast("state",state());return send(res,502,{error:lastError,state:state()})}
 }
 if(url.pathname==="/api/ready"){
  if(!bot||!connected||!joined)return send(res,409,{error:"Join a room before changing readiness."});
  if(typeof body.ready!=="boolean")return send(res,400,{error:"ready must be true or false."});
  try{await bot.ready(body.ready);broadcast("log",{level:"info",message:"Ready status sent."});return send(res,200,{ok:true,state:state()})}catch(e){return send(res,502,{error:msg(e)})}
 }
 if(url.pathname==="/api/team"){
  if(!bot||!connected||!joined)return send(res,409,{error:"Join a room before changing teams."});
  const team=Number(body.team);if(!Number.isInteger(team)||team<0||team>5)return send(res,400,{error:"Team must be between 0 and 5."});
  try{await bot.joinTeam(team);broadcast("log",{level:"info",message:"Team change request sent."});return send(res,200,{ok:true,state:state()})}catch(e){return send(res,502,{error:msg(e)})}
 }
 if(url.pathname==="/api/input"){
  if(!bot||!connected||!joined||!bot.room?.inGame)return send(res,409,{error:"Join a room and wait for a match before sending game input."});
  const input=Number(body.input),frame=Number(body.frame),sequence=Number(body.sequence);
  if(!Number.isSafeInteger(input)||input<0||input>65535||!Number.isSafeInteger(frame)||frame<0||!Number.isSafeInteger(sequence)||sequence<0)return send(res,400,{error:"input must be 0–65535 and frame/sequence must be non-negative integers."});
  const receivedAt=Date.now();try{const rawFrame=bot.room.framecount;const hasLiveFrame=typeof rawFrame==="number"&&Number.isFinite(rawFrame)&&rawFrame>0;const base=hasLiveFrame?rawFrame:null;const updatedAt=Number(bot.room.framecountUpdatedAt)||0;const frameAgeMs=updatedAt?Math.max(0,receivedAt-updatedAt):null;const roundStartRaw=Number(bot.room.roundStartTime);const roundStartMs=roundStartRaw>1e12?roundStartRaw:roundStartRaw>1e9?roundStartRaw*1000:0;const roundFrame=roundStartMs>0&&roundStartMs<=receivedAt?Math.floor((receivedAt-roundStartMs)*60/1000):null;let inputFrame=frame;let frameSource="client-clock";if(hasLiveFrame&&frameAgeMs!==null&&frameAgeMs<5000){inputFrame=Math.max(0,base+Math.floor(frameAgeMs*60/1000));frameSource="live-server-frame"}else if(Number.isSafeInteger(roundFrame)&&roundFrame>=0){inputFrame=roundFrame;frameSource="round-start-clock"}bot.sendInput({input,frame:inputFrame,sequence}).catch(e=>broadcast("log",{level:"error",message:"Input forward failed: "+msg(e)}));return send(res,200,{ok:true,frame:inputFrame,serverFrame:base,frameAgeMs,frameSource,roundStartTime:roundStartMs||null,forwardedAt:receivedAt})}catch(e){return send(res,502,{error:msg(e)})}
 }
 if(url.pathname==="/api/chat"){
  if(!bot||!connected||!joined)return send(res,409,{error:"Join a room before sending chat."});
  const message=String(body.message||"").trim().slice(0,300);if(!message)return send(res,400,{error:"Message cannot be empty."});
  try{bot.chat(message);broadcast("log",{level:"info",message:"Chat message submitted."});return send(res,200,{ok:true})}catch(e){return send(res,502,{error:msg(e)})}
 }
 if(url.pathname==="/api/leave"){
  try{if(bot){await bot.leaveGame();bot.disconnect()}}catch(e){broadcast("log",{level:"warn",message:"Leave: "+msg(e)})}
  bot=null;connecting=false;connected=false;joined=false;currentRoom="";broadcast("log",{level:"info",message:"Client session closed."});broadcast("state",state());return send(res,200,{ok:true,state:state()})
 }
 return send(res,404,{error:"Not found"});
}
const server=http.createServer(async(req,res)=>{
 const origin=req.headers.origin;
 if(origin&&ALLOWED_ORIGINS.has(origin)){res.setHeader("Access-Control-Allow-Origin",origin);res.setHeader("Vary","Origin");res.setHeader("Access-Control-Allow-Methods","GET, POST, OPTIONS");res.setHeader("Access-Control-Allow-Headers","Content-Type");}
 if(req.method==="OPTIONS"){res.writeHead(origin&&ALLOWED_ORIGINS.has(origin)?204:403);return res.end();}
 if(origin&&!ALLOWED_ORIGINS.has(origin)){res.writeHead(403,{"Content-Type":"application/json; charset=utf-8"});return res.end(JSON.stringify({error:"Origin is not allowed. Configure ALLOWED_ORIGINS for your client site."}));}
 const url=new URL(req.url,"http://"+HOST+":"+PORT);
 if(req.method==="GET"&&url.pathname==="/")return send(res,200,fs.readFileSync(path.join(__dirname,"client.html"),"utf8"),"text/html; charset=utf-8");
 if(req.method==="GET"&&url.pathname==="/app.js")return send(res,200,fs.readFileSync(path.join(__dirname,"public","app.js"),"utf8"),"text/javascript; charset=utf-8");
 if(req.method==="GET"&&url.pathname==="/api/events"){res.writeHead(200,{"Content-Type":"text/event-stream; charset=utf-8","Cache-Control":"no-cache, no-transform","Connection":"keep-alive","X-Accel-Buffering":"no"});res.write("retry: 2000\n\n");clients.add(res);for(const item of history)res.write("data: "+JSON.stringify(item)+"\n\n");req.on("close",()=>clients.delete(res));return}
 if(url.pathname.startsWith("/api/"))return api(req,res,url);return send(res,404,{error:"Not found"});
});
server.listen(PORT,HOST,()=>{console.log("Bryair's Bonk API listening on "+HOST+":"+PORT);});
function shutdown(){try{if(bot){bot.leaveGame();bot.disconnect()}}catch{}for(const res of clients){try{res.end()}catch{}}server.close(()=>process.exit(0));setTimeout(()=>process.exit(0),1500).unref()}
process.on("SIGINT",shutdown);process.on("SIGTERM",shutdown);
