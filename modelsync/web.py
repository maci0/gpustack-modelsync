"""Single-page matrix UI served at / (HTML+CSS) with JS at /app.js.

Security: all GPUStack-derived strings (model paths, node/GPU names) are HTML-
escaped before insertion, no inline event handlers are used (event delegation +
addEventListener), and the page is served under a CSP that forbids inline/foreign
scripts — so an injected `<script>`/`onerror` in a model name cannot execute.

Palette matches GPUStack's UI (UmiJS/Ant Design): light, primary green #54cc98.
ponytail: two strings, no framework. A GPUStack frontend fork would replace it."""

PAGE = """<!doctype html>
<html><head><meta charset="utf-8"><title>GPUStack Model Sync</title>
<style>
 :root{--bg:#f4f5f6;--card:#fff;--fg:#141414;--mut:#8c8c8c;--line:#ededed;
   --acc:#54cc98;--acc-d:#3fb083;--tint:#f3fbf8;--ok:#54cc98;--warn:#fa8c16;--bad:#ff4d4f}
 *{box-sizing:border-box}
 body{background:var(--bg);color:var(--fg);font:14px/1.55 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,system-ui,sans-serif;margin:0;padding:24px}
 header{display:flex;align-items:center;gap:14px;margin-bottom:16px}
 .logo{width:26px;height:26px;border-radius:7px;background:linear-gradient(135deg,#54cc98,#3fb083);display:inline-block}
 h1{font-size:17px;margin:0;font-weight:600;letter-spacing:-.2px}
 .spacer{flex:1}
 select,button{font:inherit;border-radius:8px;border:1px solid var(--line);outline:none}
 select{background:var(--card);color:var(--fg);padding:7px 11px}
 select:focus{border-color:var(--acc)}
 button{background:var(--acc);color:#fff;border:0;padding:9px 18px;cursor:pointer;font-weight:600}
 button:hover{background:var(--acc-d)} button:disabled{opacity:.45;cursor:default}
 .msg{color:var(--mut);font-size:13px;min-height:18px;margin:0 0 12px}
 .stats{display:flex;gap:18px;font-size:12.5px;color:var(--mut)}
 .stats b{color:var(--fg);font-weight:600} .stats .err b{color:var(--bad)} .stats .syn b{color:var(--acc-d)}
 .wrap{background:var(--card);border:1px solid var(--line);border-radius:14px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,.04)}
 table{border-collapse:collapse;width:100%}
 th,td{padding:11px 14px;border-bottom:1px solid var(--line);text-align:center;vertical-align:middle}
 tr:last-child td{border-bottom:0}
 tbody tr:hover{background:#fafcfb}
 th:first-child,td:first-child{text-align:left;max-width:560px}
 thead th{color:var(--mut);font-weight:600;font-size:11.5px;background:#fbfbfb;position:sticky;top:0}
 .nname{font-weight:600;font-size:13px} .nstate{font-size:10px;text-transform:uppercase;letter-spacing:.5px;margin-top:1px}
 .s-ready{color:var(--ok)} .s-bad{color:var(--bad)} .s-warn{color:var(--warn)}
 .gpu{color:#5a6270;font-size:10px;font-weight:600;max-width:150px;margin:2px auto 0;line-height:1.25}
 .nfree{color:var(--mut);font-size:10px}
 .mpath{display:inline-flex;align-items:center;gap:9px}
 .model{font-family:ui-monospace,Menlo,monospace;font-size:12px;word-break:break-all;color:#1f2733}
 .sz{color:var(--mut);font-size:11px;white-space:nowrap}
 .reset{background:#fff;border:1px solid var(--line);color:var(--mut);padding:2px 8px;font-size:12px;border-radius:7px;font-weight:500}
 .reset:hover{border-color:var(--warn);color:var(--warn)}
 .cell{display:inline-flex;flex-direction:column;align-items:center;gap:5px;min-width:78px}
 .cell input{width:17px;height:17px;accent-color:var(--acc);cursor:pointer}
 .bar{position:relative;height:6px;width:70px;background:#eef0f1;border-radius:4px;overflow:hidden}
 .bar>span{position:absolute;inset:0 auto 0 0;background:var(--acc);transition:width .6s}
 .pct{font-size:10px;color:var(--mut)}
 .badge{font-size:11px;font-weight:600;padding:1px 8px;border-radius:20px}
 .b-present{color:var(--acc-d);background:var(--tint)} .b-serving{color:#fff;background:var(--acc)}
 .b-pending{color:var(--warn);background:#fff7e6} .b-ghost{color:var(--mut);background:#f5f5f5}
 .b-err{color:var(--bad);background:#fff2f0}
 .empty{color:var(--mut);padding:34px;text-align:center}
</style></head><body>
<header>
 <span class="logo"></span><h1>Model Sync</h1>
 <div class="stats" id="stats"></div>
 <span class="spacer"></span>
 <label class="msg" style="margin:0">cluster <select id="cluster"></select></label>
 <button id="apply">Apply</button>
</header>
<div class="msg" id="msg"></div>
<div class="wrap"><table><thead><tr id="head"></tr></thead><tbody id="body"></tbody></table></div>
<script src="/app.js"></script>
</body></html>"""

SCRIPT = """
let nodes=[],models=[],dirty=false,statusMap={},polling=false;
const $=id=>document.getElementById(id);
const esc=s=>String(s==null?'':s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const cluster=()=>$('cluster').value;
const vNodes=()=>nodes.filter(n=>String(n.cluster_id)===cluster());
const gib=b=>b?(b/1073741824).toFixed(b>1073741824?0:1)+'G':'0G';

function hdrs(extra){const t=localStorage.getItem('modelsync_token')||'';return {...(t?{'X-Auth-Token':t}:{}),...extra};}
async function api(u,opts){
  let r=await fetch(u,{...opts,headers:hdrs(opts&&opts.headers)});
  if(r.status===401){
    const t=prompt('Orchestrator auth token:')||''; localStorage.setItem('modelsync_token',t);
    r=await fetch(u,{...opts,headers:hdrs(opts&&opts.headers)});
    if(r.status===401)localStorage.removeItem('modelsync_token');  // bad token: don't cache it
  }
  if(!r.ok)throw new Error(r.status);
  return r.json();
}
const jget=u=>api(u);
const jpost=(u,body)=>api(u,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)});
// (end of in-page app; the Tampermonkey build lives in USERSCRIPT below)

async function loadNodes(){
  nodes=await jget('/nodes');
  const cs=[...new Set(nodes.map(n=>n.cluster_id))];
  const sel=$('cluster'),cur=sel.value;
  sel.innerHTML=cs.map(c=>`<option value="${esc(c)}">cluster ${esc(c)}</option>`).join('');
  if(cs.map(String).includes(cur))sel.value=cur;
}
async function loadModels(){ if(dirty)return; models=await jget('/models'); render(); }
function nodeHead(n){
  const cls=n.state==='ready'?'s-ready':(n.unreachable||n.state==='maintenance'?'s-bad':'s-warn');
  const vram=n.vram_total?`<div class="nfree">VRAM ${gib(n.vram_used)}/${gib(n.vram_total)}${n.gpu_util!=null?' · '+esc(n.gpu_util)+'%':''}</div>`:'';
  const gpu=n.gpu_name?`<div class="gpu" title="${esc(n.gpu_name)}">${esc(n.gpu_name)}</div>`:'';
  return `<th><div class="nname">${esc(n.name)}</div><div class="nstate ${cls}">${esc(n.state||'')}</div>`+
    gpu+vram+`<div class="nfree">${n.free_bytes?gib(n.free_bytes)+' free':''}</div></th>`;
}
function render(){
  const ns=vNodes();
  $('head').innerHTML='<th>model</th>'+ns.map(nodeHead).join('');
  if(!models.length){$('body').innerHTML=`<tr><td colspan="${ns.length+1}" class="empty">no models in GPUStack yet</td></tr>`;updateStats();return;}
  $('body').innerHTML=models.map(m=>{
    const cells=ns.map(n=>{
      const on=m.nodes.includes(n.id);
      const cb=`<input type="checkbox" data-m="${esc(m.path)}" data-n="${esc(n.id)}" ${on?'checked':''}>`;
      return `<td><div class="cell" data-cell="${esc(m.path+'@'+n.id)}" data-have="${m.have.includes(n.id)}" data-serving="${(m.serving||[]).includes(n.id)}">${cb}<div class="under"></div></div></td>`;
    }).join('');
    return `<tr><td><span class="mpath"><button class="reset" data-path="${esc(m.path)}" title="recover a stuck/conflicted folder">⟳</button>`+
      `<span class="model">${esc(m.path)}</span><span class="sz">${gib(m.size)}</span></span></td>${cells}</tr>`;
  }).join('');
  paint();
}
async function apply(){
  const plan={};
  document.querySelectorAll('#body input:checked').forEach(c=>{(plan[c.dataset.m]=plan[c.dataset.m]||[]).push(+c.dataset.n);});
  $('msg').textContent='applying…';$('apply').disabled=true;
  const r=await jpost('/plan',{plan});
  const parts=[];
  if(r.added&&r.added.length)   parts.push(`added ${r.added.length} (${r.added.map(esc).join(', ')})`);
  if(r.removed&&r.removed.length)parts.push(`removed ${r.removed.length} — unshared + deregistered (${r.removed.map(esc).join(', ')})`);
  if(r.warnings&&r.warnings.length) parts.push('⚠ '+r.warnings.map(esc).join(' · '));
  $('msg').innerHTML = parts.length ? parts.join(' · ') : 'no changes';
  dirty=false;$('apply').textContent='Apply';$('apply').disabled=false;
  await loadModels(); poll();
}
async function reset(path){
  $('msg').textContent='resetting '+esc(path.split('/').pop())+'…';
  const r=await jpost('/reset',{path});
  $('msg').textContent = r.ok===false ? ('reset: '+esc(r.error||'failed')) : ('reset → '+((r.actions||[]).map(esc).join(' · ')||'no targets'));
  poll();
}
async function poll(){
  if(polling)return;               // no pile-up if /status is slow
  polling=true;
  try{
    const st=await jget('/status'); statusMap={};
    st.forEach(s=>statusMap[s.path+'@'+s.worker_id]=s);
    paint();
  }catch(e){}finally{ polling=false; }
}
function updateStats(){
  const ns=vNodes(), ready=ns.filter(n=>n.state==='ready').length;
  const vals=Object.values(statusMap);
  const syncing=vals.filter(s=>!s.complete&&!s.errors).length, errs=vals.filter(s=>s.errors>0).length;
  $('stats').innerHTML=`<span>models <b>${models.length}</b></span><span>nodes <b>${ready}/${ns.length}</b> ready</span>`+
    `<span class="syn">syncing <b>${syncing}</b></span><span class="err">errors <b>${errs}</b></span>`;
}
function paint(){
  updateStats();
  document.querySelectorAll('[data-cell]').forEach(el=>{
    const under=el.querySelector('.under'), s=statusMap[el.dataset.cell];
    const serving=el.dataset.serving==='true', have=el.dataset.have==='true';
    if(serving){under.innerHTML='<span class="badge b-serving" title="model instance running here">▶ serving</span>';return;}
    if(s){
      if(s.errors>0){under.innerHTML='<span class="badge b-err" title="Syncthing error — ⟳ reset">error</span>';return;}
      if(!s.complete){
        const left=s.need_bytes?` · ${gib(s.need_bytes)} left`:'';
        under.innerHTML=`<div class="bar"><span style="width:${Number(s.completion)||0}%"></span></div><span class="pct">${(Number(s.completion)||0).toFixed(0)}% ${esc(s.state)}${left}</span>`;
        return;
      }
      under.innerHTML = have ? '<span class="badge b-present" title="synced + registered in GPUStack">ready</span>'
        : '<span class="badge b-pending" title="synced; registering in GPUStack">registering…</span>';
      return;
    }
    under.innerHTML = have ? '<span class="badge b-ghost" title="present in GPUStack, not managed here">present</span>' : '';
  });
}
// event delegation (no inline handlers -> CSP-safe)
$('apply').addEventListener('click',apply);
$('cluster').addEventListener('change',render);
$('body').addEventListener('change',e=>{ if(e.target.matches('input[type=checkbox]')){dirty=true;$('apply').textContent='Apply *';} });
$('body').addEventListener('click',e=>{ const b=e.target.closest('.reset'); if(b)reset(b.dataset.path); });

(async()=>{await loadNodes();await loadModels();poll();})();
setInterval(poll,3000);
setInterval(loadModels,10000);
setInterval(loadNodes,10000);
try{ const t=localStorage.getItem('modelsync_token')||''; new EventSource('/events'+(t?'?token='+encodeURIComponent(t):'')).onmessage=()=>{loadNodes();loadModels();}; }catch(e){}
"""


# Tampermonkey userscript: embeds the matrix into the GPUStack dashboard (per
# operator, no fork). Served at GET /userscript.js with placeholders filled in.
# GM_xmlhttpRequest bypasses CORS so it can call the orchestrator cross-origin;
# auth token stored via GM_setValue. Renders inline (no iframe -> not blocked by
# our X-Frame-Options). __API__/__GP_ORIGIN__/__ORCH_HOST__ are substituted server-side.
USERSCRIPT = r"""// ==UserScript==
// @name         GPUStack Model Sync
// @namespace    modelsync
// @version      0.1.0
// @description  Sync model folders across GPUStack nodes, inside the dashboard
// @match        __GP_ORIGIN__/*
// @connect      __ORCH_HOST__
// @grant        GM_xmlhttpRequest
// @grant        GM_setValue
// @grant        GM_getValue
// @downloadURL  __API__/userscript.js
// @updateURL    __API__/userscript.js
// ==/UserScript==
(function(){
'use strict';
const API='__API__';
const esc=s=>String(s==null?'':s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const gib=b=>b?(b/1073741824).toFixed(b>1073741824?0:1)+'G':'0G';
const mk=(t,c,h)=>{const e=document.createElement(t);if(c)e.className=c;if(h!=null)e.innerHTML=h;return e;};
function tok(){let t=GM_getValue('token','');if(!t){t=prompt('Model Sync auth token:')||'';GM_setValue('token',t);}return t;}
function gm(method,path,body){return new Promise((res,rej)=>{GM_xmlhttpRequest({method,url:API+path,
  headers:Object.assign({'X-Auth-Token':tok()},body?{'Content-Type':'application/json'}:{}),
  data:body?JSON.stringify(body):null,timeout:20000,
  onload:r=>{if(r.status===401){GM_setValue('token','');rej(new Error('unauthorized (re-enter token)'));return;}
    if(r.status>=200&&r.status<300){try{res(JSON.parse(r.responseText))}catch(e){rej(e)}}else rej(new Error('HTTP '+r.status));},
  onerror:()=>rej(new Error('network/CORS - check @connect + orchestrator reachable')),ontimeout:()=>rej(new Error('timeout'))});});}

const css=`#msb{position:fixed;right:22px;bottom:22px;z-index:99999;background:#54cc98;color:#fff;border:0;border-radius:24px;padding:11px 18px;font:600 13px system-ui;cursor:pointer;box-shadow:0 4px 14px rgba(0,0,0,.25)}
#msp{position:fixed;top:0;right:0;bottom:0;width:min(980px,96vw);z-index:99999;background:#fff;color:#141414;box-shadow:-4px 0 24px rgba(0,0,0,.25);display:none;flex-direction:column;font:14px system-ui}
#msp.o{display:flex}
.mshd{display:flex;align-items:center;gap:10px;padding:13px 18px;border-bottom:1px solid #ededed}
.mshd b{font-size:15px}.mssp{flex:1}
#msmsg{color:#8c8c8c;font-size:12px;min-height:16px;padding:6px 18px}
#msbody{overflow:auto;padding:0 18px 18px}
.msB{background:#54cc98;color:#fff;border:0;border-radius:7px;padding:8px 15px;font:600 13px system-ui;cursor:pointer}
.msX{background:#eee;color:#333;border:0;border-radius:7px;padding:7px 11px;cursor:pointer}
#mscl{padding:6px 9px;border-radius:6px;border:1px solid #ddd}
table.msT{border-collapse:collapse;width:100%}
.msT th,.msT td{border-bottom:1px solid #ededed;padding:8px 10px;text-align:center;font-size:12px;vertical-align:middle}
.msT th:first-child,.msT td:first-child{text-align:left;max-width:430px;word-break:break-all}
.msT .mp{font-family:ui-monospace,monospace}
.msT .rs{border:1px solid #ddd;background:#fff;border-radius:6px;cursor:pointer;color:#888;padding:1px 6px;margin-right:7px}
.msT .bar{height:6px;width:64px;background:#eef0f1;border-radius:4px;overflow:hidden;display:inline-block;vertical-align:middle}
.msT .bar>i{display:block;height:100%;background:#54cc98}
.bdg{font-size:11px;font-weight:600;padding:1px 7px;border-radius:20px}
.k-ok{color:#3fb083;background:#f3fbf8}.k-pend{color:#fa8c16;background:#fff7e6}.k-err{color:#ff4d4f;background:#fff2f0}.k-gh{color:#999;background:#f5f5f5}`;
document.head.appendChild(mk('style',null,css));

const btn=mk('button',null,'↻ Model Sync');btn.id='msb';document.body.appendChild(btn);
const panel=mk('div');panel.id='msp';
const hd=mk('div','mshd');
const cl=mk('select');cl.id='mscl';
const apply=mk('button','msB','Apply');const close=mk('button','msX','Close');
hd.appendChild(mk('b',null,'Model Sync'));hd.appendChild(mk('span','mssp'));hd.appendChild(cl);hd.appendChild(apply);hd.appendChild(close);
const msg=mk('div',null,'');msg.id='msmsg';
const body=mk('div');body.id='msbody';
panel.appendChild(hd);panel.appendChild(msg);panel.appendChild(body);document.body.appendChild(panel);

let nodes=[],models=[],dirty=false,stat={},timer=null,polling=false;
const vn=()=>nodes.filter(n=>String(n.cluster_id)===cl.value);
function setMsg(t){msg.textContent=t;}

async function load(){
  try{nodes=await gm('GET','/nodes');models=await gm('GET','/models');}
  catch(e){setMsg('error: '+e.message);return;}
  const cs=[...new Set(nodes.map(n=>n.cluster_id))];const cur=cl.value;
  cl.innerHTML=cs.map(c=>`<option value="${esc(c)}">cluster ${esc(c)}</option>`).join('');
  if(cs.map(String).includes(cur))cl.value=cur;
  render();poll();
}
function render(){
  const ns=vn();
  let h='<table class="msT"><thead><tr><th>model</th>'+ns.map(n=>{
    const st=n.state==='ready'?'#3fb083':(n.unreachable||n.state==='maintenance'?'#ff4d4f':'#fa8c16');
    return `<th><div>${esc(n.name)}</div><div style="font-size:10px;color:${st}">${esc(n.state||'')}</div>`+
      (n.gpu_name?`<div style="font-size:10px;color:#5a6270">${esc(n.gpu_name)}</div>`:'')+
      (n.vram_total?`<div style="font-size:10px;color:#999">VRAM ${gib(n.vram_used)}/${gib(n.vram_total)}</div>`:'')+'</th>';
  }).join('')+'</tr></thead><tbody>';
  if(!models.length)h+=`<tr><td colspan="${ns.length+1}" style="text-align:center;color:#999;padding:30px">no models</td></tr>`;
  h+=models.map(m=>{
    const cells=ns.map(n=>{
      const on=m.nodes.includes(n.id);
      return `<td><div class="cell" data-cell="${esc(m.path+'@'+n.id)}" data-have="${m.have.includes(n.id)}" data-srv="${(m.serving||[]).includes(n.id)}">`+
        `<input type="checkbox" data-m="${esc(m.path)}" data-n="${esc(n.id)}" ${on?'checked':''}><div class="u"></div></div></td>`;
    }).join('');
    return `<tr><td><button class="rs" data-path="${esc(m.path)}" title="recover stuck folder">↻</button>`+
      `<span class="mp">${esc(m.path)}</span> <span style="color:#999;font-size:11px">${gib(m.size)}</span></td>${cells}</tr>`;
  }).join('');
  body.innerHTML=h+'</tbody></table>';
  body.querySelectorAll('input[type=checkbox]').forEach(c=>c.onchange=()=>{dirty=true;apply.textContent='Apply *';});
  body.querySelectorAll('.rs').forEach(b=>b.onclick=()=>reset(b.dataset.path));
  paint();
}
function paint(){
  body.querySelectorAll('[data-cell]').forEach(el=>{
    const u=el.querySelector('.u'),s=stat[el.dataset.cell],have=el.dataset.have==='true',srv=el.dataset.srv==='true';
    if(srv){u.innerHTML='<span class="bdg k-ok">▶ serving</span>';return;}
    if(s){
      if(s.errors>0){u.innerHTML='<span class="bdg k-err">error</span>';return;}
      if(!s.complete){const left=s.need_bytes?' · '+gib(s.need_bytes)+' left':'';
        u.innerHTML=`<span class="bar"><i style="width:${Number(s.completion)||0}%"></i></span> <span style="font-size:10px;color:#999">${(Number(s.completion)||0).toFixed(0)}% ${esc(s.state)}${left}</span>`;return;}
      u.innerHTML=have?'<span class="bdg k-ok">ready</span>':'<span class="bdg k-pend">registering…</span>';return;
    }
    u.innerHTML=have?'<span class="bdg k-gh">present</span>':'';
  });
}
async function poll(){if(polling)return;polling=true;try{const st=await gm('GET','/status');stat={};st.forEach(s=>stat[s.path+'@'+s.worker_id]=s);paint();}catch(e){}finally{polling=false;}}
async function doApply(){
  const plan={};body.querySelectorAll('input:checked').forEach(c=>{(plan[c.dataset.m]=plan[c.dataset.m]||[]).push(+c.dataset.n);});
  setMsg('applying…');
  try{const r=await gm('POST','/plan',{plan});const p=[];
    if(r.added&&r.added.length)p.push('added '+r.added.length);
    if(r.removed&&r.removed.length)p.push('removed '+r.removed.length+' (unshared + deregistered)');
    if(r.warnings&&r.warnings.length)p.push('⚠ '+r.warnings.join(' · '));
    setMsg(p.join(' · ')||'no changes');}
  catch(e){setMsg('error: '+e.message);}
  dirty=false;apply.textContent='Apply';load();
}
async function reset(path){setMsg('resetting…');try{const r=await gm('POST','/reset',{path});
  setMsg(r.ok===false?('reset: '+(r.error||'failed')):('reset → '+((r.actions||[]).join(' · ')||'no targets')));}
  catch(e){setMsg('error: '+e.message);}poll();}

cl.onchange=render;apply.onclick=doApply;
close.onclick=()=>{panel.classList.remove('o');if(timer){clearInterval(timer);timer=null;}};
btn.onclick=()=>{panel.classList.add('o');load();if(!timer)timer=setInterval(()=>{poll();},3000);};
})();
"""
