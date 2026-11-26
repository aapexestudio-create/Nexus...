// Servidor NEXUS: serve index.html + WebSocket (chat, sinalização de chamadas, arquivos). Sem dependências.
const http=require("http"),fs=require("fs"),crypto=require("crypto"),path=require("path");
const PORT=process.env.PORT||3000,MAX=40*1024*1024,users=new Map(),all=new Set();
const srv=http.createServer((q,r)=>fs.readFile(path.join(__dirname,"index.html"),(e,d)=>{if(e){r.writeHead(500);return r.end("index.html não encontrado")}r.writeHead(200,{"Content-Type":"text/html; charset=utf-8"});r.end(d)}));
function frame(op,b){const n=b.length;let h;if(n<126)h=Buffer.from([0x80|op,n]);else if(n<65536){h=Buffer.alloc(4);h[0]=0x80|op;h[1]=126;h.writeUInt16BE(n,2)}else{h=Buffer.alloc(10);h[0]=0x80|op;h[1]=127;h.writeBigUInt64BE(BigInt(n),2)}return Buffer.concat([h,b])}
const send=(s,o)=>{if(!s.destroyed)s.write(frame(1,Buffer.from(JSON.stringify(o))))};
const online=()=>{const n=users.size||1;all.forEach(s=>send(s,{t:"online",n}))};
srv.on("upgrade",(q,s)=>{
 const k=q.headers["sec-websocket-key"];if(!k)return s.destroy();
 s.write("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: "+crypto.createHash("sha1").update(k+"258EAFA5-E914-47DA-95CA-C5AB0DC85B11").digest("base64")+"\r\n\r\n");
 s.setNoDelay(true);all.add(s);let buf=Buffer.alloc(0),parts=[],name="";
 const leave=()=>{if(name){const set=users.get(name);if(set){set.delete(s);if(!set.size)users.delete(name)}}name=""};
 const to=n=>users.get(String(n||"").toLowerCase());
 function handle(d){
  if(d.t==="hello"){leave();const nick=String(d.name||"").trim().slice(0,32);name=nick.toLowerCase();if(!name)return;if(!users.has(name))users.set(name,new Set());users.get(name).add(s);s.nick=nick;online();return}
  if(!name||d.t==="ping")return;
  if(d.t==="chat"){
   if(d.scope==="dm"){const t=to(d.to);if(!t)return send(s,{t:"err",m:d.to+" está offline — a mensagem só chega quando ele estiver online."});t.forEach(x=>send(x,{t:"chat",scope:"dm",from:s.nick,msg:d.msg,file:d.file}))}
   else all.forEach(x=>x!==s&&x.nick&&send(x,{t:"chat",scope:"ch",server:d.server,channel:d.channel,msg:d.msg,file:d.file}))
  }else if(d.t==="sig"){const t=to(d.to);if(t)t.forEach(x=>send(x,{...d,from:s.nick}))}
 }
 s.on("data",d=>{buf=Buffer.concat([buf,d]);
  for(;;){if(buf.length<2)break;const op=buf[0]&15,fin=buf[0]&128,mk=buf[1]&128;let len=buf[1]&127,off=2;
   if(len===126){if(buf.length<4)break;len=buf.readUInt16BE(2);off=4}else if(len===127){if(buf.length<10)break;len=Number(buf.readBigUInt64BE(2));off=10}
   if(len>MAX)return s.destroy();if(mk)off+=4;if(buf.length<off+len)break;
   const p=Buffer.from(buf.subarray(off,off+len));if(mk){const m=buf.subarray(off-4,off);for(let i=0;i<len;i++)p[i]^=m[i&3]}
   buf=buf.subarray(off+len);
   if(op===8){s.end(frame(8,Buffer.alloc(0)));return}
   if(op===9){s.write(frame(10,p));continue}
   if(op===10)continue;
   parts.push(p);if(!fin)continue;
   const txt=Buffer.concat(parts).toString();parts=[];try{handle(JSON.parse(txt))}catch(_){}
  }});
 s.on("close",()=>{all.delete(s);leave();online()});s.on("error",()=>{});
});
srv.listen(PORT,()=>console.log("NEXUS rodando em http://localhost:"+PORT));
