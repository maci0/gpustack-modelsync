"""Single-page matrix UI served at / (HTML+CSS) with JS at /app.js.

Security: all GPUStack-derived strings (model paths, node/GPU names) are HTML-
escaped before insertion, no inline event handlers are used (event delegation +
addEventListener), and the page is served under a CSP that forbids inline/foreign
scripts — so an injected `<script>`/`onerror` in a model name cannot execute.

Look matches the GPUStack dashboard using the design tokens from the gpustack-ui
source (src/config/theme/light.ts + global.less): primary #007BFF, success
#54cc98, text #1F1F1F, border #d3d8de, layout bg #f4f5f6, radius 4/6, Helvetica
Neue, antd-style tags/table.
ponytail: two strings, no framework. A GPUStack frontend fork would replace it."""

PAGE = """<!doctype html>
<html><head><meta charset="utf-8"><title>GPUStack Model Sync</title>
<link rel="icon" href="data:,">
<style>
 /* Design tokens lifted from gpustack-ui (src/config/theme + global.less):
    primary #007BFF, success #54cc98, text #1F1F1F, border #d3d8de, layout bg
    #f4f5f6, radius 4/6, Helvetica Neue stack, table hover #f9f9f9. */
 :root{--bg:#f4f5f6;--card:#fff;--fg:#1F1F1F;--mut:rgba(0,0,0,.45);--line:#e8e8e8;
   --acc:#007BFF;--acc-d:#0069d9;--tint:#e6f4ff;--ok:#54cc98;--warn:#fa8c16;--bad:#ff4d4f}
 *{box-sizing:border-box}
 body{background:var(--bg);color:var(--fg);
   font:14px/1.55 "Helvetica Neue",-apple-system,BlinkMacSystemFont,Arial,"Noto Sans",sans-serif;margin:0;padding:24px;min-height:100vh}
 header{display:flex;align-items:center;gap:14px;margin-bottom:16px}
 .logo{width:26px;height:26px;border-radius:6px;background:linear-gradient(135deg,#3d9bff,#007BFF);display:inline-block}
 h1{font-size:17px;margin:0;font-weight:600;letter-spacing:-.2px}
 .spacer{flex:1}
 select,button{font:inherit;border-radius:6px;border:1px solid #d3d8de;outline:none}
 select{background:var(--card);color:var(--fg);padding:7px 11px}
 select:focus{border-color:var(--acc);box-shadow:0 0 0 2px rgba(0,123,255,.12)}
 button{background:var(--acc);color:#fff;border:0;padding:9px 18px;cursor:pointer;font-weight:600}
 button:hover{background:var(--acc-d)} button:disabled{opacity:.45;cursor:default}
 .msg{color:var(--mut);font-size:13px;min-height:18px;margin:0 0 12px}
 .stats{display:flex;gap:18px;font-size:12.5px;color:var(--mut)}
 .stats b{color:var(--fg);font-weight:600} .stats .err b{color:var(--bad)} .stats .syn b{color:var(--acc-d)}
 .wrap{background:var(--card);border:1px solid #eaebec;border-radius:6px;overflow:auto;max-height:calc(100vh - 150px);box-shadow:0 1px 2px rgba(0,0,0,.03)}
 table{border-collapse:collapse;width:100%}
 th,td{padding:8px 16px;border-bottom:1px solid var(--line);text-align:center;vertical-align:middle}
 tr:last-child td{border-bottom:0}
 tbody tr:hover{background:#f9f9f9}
 tbody tr:hover td:first-child{background:#f9f9f9}
 th:first-child,td:first-child{text-align:left;max-width:560px;position:sticky;left:0;background:var(--card);z-index:1}
 thead th{color:var(--fg);font-weight:600;font-size:12px;background:var(--card);position:sticky;top:0;z-index:2;border-bottom:1px solid #e8e8e8}
 thead th:first-child{z-index:3}
 tfoot td{position:sticky;bottom:0;background:#f7f8fa;font-size:11px;color:var(--mut);border-top:1px solid var(--line)}
 tfoot td:first-child{background:#f7f8fa}
 .cap b{color:var(--fg);font-weight:600} .cap.over b{color:var(--bad)}
 .hbar{height:4px;width:74px;background:#f1f2f3;border-radius:3px;overflow:hidden;margin:3px auto 0}
 .hbar>span{display:block;height:100%;background:var(--acc)}
 .hbar.hot>span{background:var(--warn)} .hbar.full>span{background:var(--bad)}
 .nname{font-weight:600;font-size:13px} .nstate{font-size:10px;text-transform:uppercase;letter-spacing:.5px;margin-top:1px}
 .s-ready{color:var(--ok)} .s-bad{color:var(--bad)} .s-warn{color:var(--warn)}
 .gpu{color:#5a6270;font-size:10px;font-weight:600;max-width:150px;margin:2px auto 0;line-height:1.25}
 .nfree{color:var(--mut);font-size:10px}
 .mpath{display:flex;align-items:center;gap:9px;min-width:0}
 .mtxt{min-width:0;flex:1}
 .mname{font-weight:600;font-size:13px;color:#1f2733;word-break:break-all}
 .morg{color:var(--mut);font-size:11px;font-family:ui-monospace,Menlo,monospace;word-break:break-all}
 .sz{color:var(--mut);font-size:11px;white-space:nowrap}
 .allb{display:inline-flex;align-items:center;gap:4px;color:var(--mut);font-size:10.5px;margin-left:auto;cursor:pointer;white-space:nowrap}
 .allb input{width:13px;height:13px;accent-color:var(--acc);cursor:pointer}
 .colall{width:13px;height:13px;accent-color:var(--acc);cursor:pointer;margin-top:5px}
 #q{font:inherit;background:var(--card);border:1px solid #d3d8de;border-radius:8px;padding:7px 11px;width:210px;outline:none}
 #q:focus{border-color:var(--acc);box-shadow:0 0 0 2px rgba(0,123,255,.12)}
 #age{font-size:11px;color:var(--mut)} #age.stale{color:var(--bad)}
 .reset,.cpy{background:#fff;border:1px solid #d3d8de;color:var(--mut);padding:2px 8px;font-size:12px;border-radius:6px;font-weight:500}
 .reset:hover{border-color:var(--warn);color:var(--warn)}
 .cpy:hover{border-color:var(--acc);color:var(--acc)}
 tr.dirtyrow td{background:#fffbe9}
 tr.dirtyrow td:first-child{background:#fffbe9}
 tr.dirtyrow:hover td,tr.dirtyrow:hover td:first-child{background:#fff7d6}
 .stats .pend b{color:#d48806}
 #mth{cursor:pointer;user-select:none}
 .sortmark{color:var(--acc);font-weight:700}
 .cell{display:inline-flex;flex-direction:column;align-items:center;gap:5px;min-width:78px}
 .cell input{width:17px;height:17px;accent-color:var(--acc);cursor:pointer}
 .bar{position:relative;height:6px;width:70px;background:#f1f2f3;border-radius:3px;overflow:hidden}
 .bar>span{position:absolute;inset:0 auto 0 0;background:var(--acc);transition:width .6s}
 .pct{font-size:10px;color:var(--mut)}
 /* antd-style tags; success = gpustack colorSuccess #54cc98 */
 .badge{font-size:11px;font-weight:500;padding:0 7px;border-radius:4px;border:1px solid transparent;line-height:1.7}
 .b-present{color:#2f9d70;background:#f0faf6;border-color:#b5e7d2}
 .b-serving{color:#0068d6;background:#e6f4ff;border-color:#91caff}
 .b-pending{color:#d48806;background:#fffbe6;border-color:#ffe58f}
 .b-ghost{color:rgba(0,0,0,.65);background:#fafafa;border-color:#d9d9d9}
 .b-err{color:#cf1322;background:#fff2f0;border-color:#ffa39e}
 .empty{color:var(--mut);padding:34px;text-align:center}
</style></head><body>
<header>
 <span class="logo"></span><h1>Model Sync</h1>
 <div class="stats" id="stats"></div>
 <span id="age"></span>
 <span class="spacer"></span>
 <input id="q" type="search" placeholder="filter models…">
 <select id="cluster"></select>
 <button id="apply">Apply</button>
</header>
<div class="msg" id="msg"></div>
<div class="wrap"><table><thead><tr id="head"></tr></thead><tbody id="body"></tbody><tfoot><tr id="foot"></tr></tfoot></table></div>
<script src="/app.js"></script>
</body></html>"""

SCRIPT = """
// edits: path -> array of node ids the user has ticked (overrides server plan).
// Apply builds the plan from models+edits, NOT from the visible DOM, so a
// filtered-out (hidden) model is never mistaken for "unticked everywhere".
let nodes=[],clusters=[],models=[],edits={},dirty=false,statusMap={},polling=false,lastPoll=0;
const $=id=>document.getElementById(id);
const esc=s=>String(s==null?'':s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const cluster=()=>$('cluster').value;
const vNodes=()=>nodes.filter(n=>String(n.cluster_id)===cluster());
const gib=b=>b?(b/1073741824).toFixed(b>1073741824?0:1)+'G':'0G';
// key-membership, NOT truthiness: an empty edit ([] = remove from ALL nodes) is
// a real selection and must not fall back to the server plan.
const selOf=m=>(m.path in edits)?edits[m.path]:m.nodes;
const sameSet=(a,b)=>{if(a.length!==b.length)return false;const x=[...a].sort(),y=[...b].sort();return x.every((v,i)=>v===y[i]);};
let sortBy='name';                          // 'name' | 'size' (click the model header)
function pruneEdits(){                      // drop no-op edits; recompute dirty
  models.forEach(m=>{ if(edits[m.path]&&sameSet(edits[m.path],m.nodes))delete edits[m.path]; });
  dirty=Object.keys(edits).length>0;
  $('apply').textContent=dirty?'Apply *':'Apply';
}

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
  try{ nodes=await jget('/nodes'); }
  catch(e){ return; }  // transient: keep last view
  try{ clusters=await jget('/clusters'); }catch(e){}  // cosmetic only: names fall back
  const cs=[...new Set(nodes.map(n=>n.cluster_id))];
  const cname=Object.fromEntries(clusters.map(c=>[String(c.id),c.name]));
  const sel=$('cluster'),cur=sel.value;
  sel.innerHTML=cs.map(c=>`<option value="${esc(c)}">${esc(cname[String(c)]||('cluster '+c))}</option>`).join('');
  if(cs.map(String).includes(cur))sel.value=cur;
}
async function loadModels(){
  if(dirty)return;                       // never clobber unsaved ticks
  try{ models=await jget('/models'); }catch(e){ return; }
  render();
}
function nodeHead(n,allOn){
  const cls=n.state==='ready'?'s-ready':(n.unreachable||n.state==='maintenance'?'s-bad':'s-warn');
  const pct=n.vram_total?Math.round(n.vram_used/n.vram_total*100):0;
  const hcls=pct>90?'hbar full':pct>70?'hbar hot':'hbar';
  const vram=n.vram_total?`<div class="nfree">VRAM ${gib(n.vram_used)}/${gib(n.vram_total)}${n.gpu_util!=null?' · '+esc(n.gpu_util)+'%':''}</div>`+
    `<div class="${hcls}"><span style="width:${pct}%"></span></div>`:'';
  const gpu=n.gpu_name?`<div class="gpu" title="${esc(n.gpu_name)}">${esc(n.gpu_name)}</div>`:'';
  return `<th><div class="nname">${esc(n.name)}</div><div class="nstate ${cls}">${esc(n.state||'')}</div>`+
    gpu+vram+`<div class="nfree">${n.free_bytes?gib(n.free_bytes)+' free':''}</div>`+
    `<input type="checkbox" class="colall" data-n="${esc(n.id)}" title="all models on ${esc(n.name)}" ${allOn?'checked':''}></th>`;
}
function updateFoot(){  // per-node capacity preview: new bytes the plan would pull
  const ns=vNodes();
  $('foot').innerHTML='<td>planned</td>'+ns.map(n=>{
    let add=0;
    models.forEach(m=>{ if(selOf(m).includes(n.id)&&!m.have.includes(n.id))add+=m.size; });
    if(!add)return '<td></td>';
    const over=n.free_bytes!=null&&add>n.free_bytes;
    return `<td><span class="cap${over?' over':''}" title="new data this node would pull">+<b>${gib(add)}</b>${n.free_bytes?' / '+gib(n.free_bytes)+' free':''}${over?' ⚠':''}</span></td>`;
  }).join('');
}
function splitPath(p){
  const seg=p.replace(/\\/+$/,'').split('/');
  return {name:seg[seg.length-1]||p, org:seg.slice(0,-1).join('/')};
}
function visModels(){
  const q=$('q').value.trim().toLowerCase();
  const vis=q? models.filter(m=>m.path.toLowerCase().includes(q)) : [...models];
  return sortBy==='size'? vis.sort((a,b)=>b.size-a.size||a.path.localeCompare(b.path))
                        : vis.sort((a,b)=>splitPath(a.path).name.localeCompare(splitPath(b.path).name));
}
function render(){
  const ns=vNodes(), vis=visModels();
  const allByNode=n=>models.length>0&&models.every(m=>selOf(m).includes(n.id));
  $('head').innerHTML=`<th id="mth" title="click to sort">model <span class="sortmark">${sortBy==='size'?'↓ size':'a–z'}</span></th>`+
    ns.map(n=>nodeHead(n,allByNode(n))).join('');
  if(!vis.length){
    $('body').innerHTML=`<tr><td colspan="${ns.length+1}" class="empty">${models.length?'no models match the filter':'no models in GPUStack yet'}</td></tr>`;
    updateStats();return;
  }
  $('body').innerHTML=vis.map(m=>{
    const sel=selOf(m);
    const cells=ns.map(n=>{
      const cb=`<input type="checkbox" data-m="${esc(m.path)}" data-n="${esc(n.id)}" ${sel.includes(n.id)?'checked':''}>`;
      return `<td><div class="cell" data-cell="${esc(m.path+'@'+n.id)}" data-have="${m.have.includes(n.id)}" data-serving="${(m.serving||[]).includes(n.id)}">${cb}<div class="under"></div></div></td>`;
    }).join('');
    const sp=splitPath(m.path);
    const rowAll=ns.length>0&&ns.every(n=>sel.includes(n.id));
    const changed=!!edits[m.path];
    return `<tr data-path="${esc(m.path)}"${changed?' class="dirtyrow"':''}><td><span class="mpath">`+
      `<button class="reset" data-path="${esc(m.path)}" title="recover a stuck/conflicted folder">⟳</button>`+
      `<button class="cpy" data-path="${esc(m.path)}" title="copy model path">⧉</button>`+
      `<span class="mtxt"><div class="mname" title="${esc(m.path)}">${esc(sp.name)}</div>`+
      `<div class="morg">${esc(sp.org)} · ${gib(m.size)} · on ${m.have.length} node${m.have.length===1?'':'s'}</div></span>`+
      `<label class="allb" title="all nodes for this model"><input type="checkbox" class="rowall" ${rowAll?'checked':''}>all</label>`+
      `</span></td>${cells}</tr>`;
  }).join('');
  syncBulk();paint();
}
function rowOf(el){return el.closest('tr');}
function recordRow(tr){  // read the row's boxes into edits; no-op edits self-clear
  const path=tr.dataset.path;
  edits[path]=[...tr.querySelectorAll('input[data-n]')].filter(c=>c.checked).map(c=>+c.dataset.n);
  pruneEdits();
}
function syncBulk(){  // row-all + col-all reflect current selection (indeterminate when mixed)
  document.querySelectorAll('#body tr[data-path]').forEach(tr=>{
    tr.classList.toggle('dirtyrow',!!edits[tr.dataset.path]);  // pending-change tint
    const boxes=[...tr.querySelectorAll('input[data-n]')], ra=tr.querySelector('.rowall');
    if(!ra||!boxes.length)return;
    const on=boxes.filter(b=>b.checked).length;
    ra.checked=on===boxes.length;ra.indeterminate=on>0&&on<boxes.length;
  });
  document.querySelectorAll('#head .colall').forEach(ca=>{
    const boxes=[...document.querySelectorAll(`#body input[data-n="${ca.dataset.n}"]`)];
    if(!boxes.length)return;
    const on=boxes.filter(b=>b.checked).length;
    ca.checked=on===boxes.length;ca.indeterminate=on>0&&on<boxes.length;
  });
  updateFoot();
}
function pendingRemovals(){  // copies the next apply would deregister
  const out=[];
  models.forEach(m=>{const sel=selOf(m);m.nodes.forEach(id=>{if(!sel.includes(id))out.push(splitPath(m.path).name+' ✗ node '+id);});});
  return out;
}
async function apply(){
  const plan={};
  models.forEach(m=>{const sel=selOf(m);if(sel.length)plan[m.path]=sel;});
  const rem=pendingRemovals();
  if(rem.length&&!confirm(`Remove ${rem.length} cop${rem.length===1?'y':'ies'}?\\n(unshares + deregisters from GPUStack; files stay on disk)\\n\\n`+rem.slice(0,12).join('\\n')+(rem.length>12?'\\n…':'')))return;
  $('msg').textContent='applying…';$('apply').disabled=true;
  try{
    const r=await jpost('/plan',{plan});
    const parts=[];
    if(r.added&&r.added.length)   parts.push(`added ${r.added.length} (${r.added.map(esc).join(', ')})`);
    if(r.removed&&r.removed.length)parts.push(`removed ${r.removed.length} — unshared + deregistered (${r.removed.map(esc).join(', ')})`);
    if(r.warnings&&r.warnings.length) parts.push('⚠ '+r.warnings.map(esc).join(' · '));
    $('msg').innerHTML = parts.length ? parts.join(' · ') : 'no changes';
    dirty=false;edits={};
  }catch(e){
    $('msg').textContent='apply failed: '+e.message+' — your ticks are kept, retry';
  }finally{
    $('apply').textContent=dirty?'Apply *':'Apply';$('apply').disabled=false;  // never leave it bricked
  }
  await loadModels(); poll();
}
async function reset(path){
  $('msg').textContent='resetting '+esc(path.split('/').pop())+'…';
  try{
    const r=await jpost('/reset',{path});
    $('msg').textContent = r.ok===false ? ('reset: '+esc(r.error||'failed')) : ('reset → '+((r.actions||[]).map(esc).join(' · ')||'no targets'));
  }catch(e){ $('msg').textContent='reset failed: '+e.message; }
  poll();
}
let rates={};  // cell -> {t,need,bps} : client-side transfer rate from poll deltas
async function poll(){
  if(polling)return;               // no pile-up if /status is slow
  polling=true;
  try{
    const st=await jget('/status'); statusMap={};
    const now=Date.now();
    st.forEach(s=>{
      const k=s.path+'@'+s.worker_id; statusMap[k]=s;
      const p=rates[k];
      if(p&&s.need_bytes<p.need&&now>p.t){
        const bps=(p.need-s.need_bytes)/((now-p.t)/1000);
        rates[k]={t:now,need:s.need_bytes,bps:p.bps?0.6*bps+0.4*p.bps:bps};  // EMA smooth
      }else rates[k]={t:now,need:s.need_bytes,bps:p&&s.need_bytes===p.need?p.bps:0};
    });
    lastPoll=now;
    paint();
  }catch(e){}finally{ polling=false; }
}
function rateTxt(k,need){
  const r=rates[k];
  if(!r||!r.bps||r.bps<1||!need)return '';
  const mbs=(r.bps/1048576).toFixed(r.bps>10485760?0:1), s=Math.round(need/r.bps);
  const eta=s>=3600?Math.round(s/3600)+'h':s>=60?Math.round(s/60)+'m':s+'s';
  return ` · ${mbs}MB/s · ${eta}`;
}
function tickAge(){
  const el=$('age'); if(!lastPoll){el.textContent='';return;}
  const s=Math.round((Date.now()-lastPoll)/1000);
  el.textContent=`updated ${s}s ago`;
  el.className=s>12?'stale':'';   // polling broken -> red
}
function updateStats(){
  const ns=vNodes(), ready=ns.filter(n=>n.state==='ready').length;
  const vals=Object.values(statusMap);
  const syncing=vals.filter(s=>!s.complete&&!s.errors&&s.state!=='unreachable').length;
  const errs=vals.filter(s=>s.errors>0||s.state==='unreachable').length;  // unreachable = problem, not progress
  const pend=Object.keys(edits).length;
  $('stats').innerHTML=`<span>models <b>${models.length}</b></span><span>nodes <b>${ready}/${ns.length}</b> ready</span>`+
    `<span class="syn">syncing <b>${syncing}</b></span><span class="err">errors <b>${errs}</b></span>`+
    (pend?`<span class="pend">pending <b>${pend}</b></span>`:'');
}
function paint(){
  updateStats();
  document.querySelectorAll('[data-cell]').forEach(el=>{
    const under=el.querySelector('.under'), s=statusMap[el.dataset.cell];
    const serving=el.dataset.serving==='true', have=el.dataset.have==='true';
    if(serving){under.innerHTML='<span class="badge b-serving" title="model instance running here">▶ serving</span>';return;}
    if(s){
      if(s.errors>0){under.innerHTML='<span class="badge b-err" title="Syncthing error — ⟳ reset">error</span>';return;}
      if(s.state==='unreachable'){under.innerHTML='<span class="badge b-err" title="Syncthing on this node not responding">unreachable</span>';return;}
      if(!s.complete){
        const left=s.need_bytes?` · ${gib(s.need_bytes)} left`:'';
        under.innerHTML=`<div class="bar"><span style="width:${Number(s.completion)||0}%"></span></div><span class="pct">${(Number(s.completion)||0).toFixed(0)}% ${esc(s.state)}${left}${esc(rateTxt(el.dataset.cell,s.need_bytes))}</span>`;
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
$('q').addEventListener('input',render);   // edits live in `edits`, so re-render is safe
$('body').addEventListener('change',e=>{
  if(e.target.matches('.rowall')){         // whole row on/off
    const tr=rowOf(e.target);
    tr.querySelectorAll('input[data-n]').forEach(c=>c.checked=e.target.checked);
    recordRow(tr);
  }else if(e.target.matches('input[data-n]')){
    recordRow(rowOf(e.target));
  }else return;
  syncBulk();
});
$('head').addEventListener('change',e=>{
  if(!e.target.matches('.colall'))return;  // whole column: this node for ALL models
  const id=+e.target.dataset.n, on=e.target.checked;
  models.forEach(m=>{
    const sel=new Set(selOf(m));
    on?sel.add(id):sel.delete(id);
    edits[m.path]=[...sel];
  });
  pruneEdits();
  render();
});
$('head').addEventListener('click',e=>{
  if(e.target.closest('#mth')){ sortBy=sortBy==='name'?'size':'name'; render(); }
});
$('body').addEventListener('click',e=>{
  const b=e.target.closest('.reset'); if(b){reset(b.dataset.path);return;}
  const c=e.target.closest('.cpy');
  if(c){ copyText(c.dataset.path); $('msg').textContent='path copied'; }
});
function copyText(t){  // clipboard API needs https; textarea fallback works on http LAN
  if(navigator.clipboard&&window.isSecureContext){navigator.clipboard.writeText(t);return;}
  const ta=document.createElement('textarea');ta.value=t;ta.style.position='fixed';ta.style.opacity='0';
  document.body.appendChild(ta);ta.select();document.execCommand('copy');ta.remove();
}
document.addEventListener('keydown',e=>{  // '/' focuses the filter (like GitHub)
  if(e.key==='/'&&!/INPUT|SELECT|TEXTAREA/.test(document.activeElement.tagName)){e.preventDefault();$('q').focus();}
});

(async()=>{await loadNodes();await loadModels();poll();})();
setInterval(poll,3000);
setInterval(loadModels,10000);
setInterval(loadNodes,10000);
setInterval(tickAge,1000);
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

const css=`#msb{position:fixed;right:22px;bottom:22px;z-index:99999;background:#007BFF;color:#fff;border:0;border-radius:24px;padding:11px 18px;font:600 13px system-ui;cursor:pointer;box-shadow:0 4px 14px rgba(0,0,0,.25)}
#msp{position:fixed;top:0;right:0;bottom:0;width:min(980px,96vw);z-index:99999;background:#fff;color:#141414;box-shadow:-4px 0 24px rgba(0,0,0,.25);display:none;flex-direction:column;font:14px system-ui}
#msp.o{display:flex}
.mshd{display:flex;align-items:center;gap:10px;padding:13px 18px;border-bottom:1px solid #ededed}
.mshd b{font-size:15px}.mssp{flex:1}
#msmsg{color:#8c8c8c;font-size:12px;min-height:16px;padding:6px 18px}
#msbody{overflow:auto;padding:0 18px 18px}
.msB{background:#007BFF;color:#fff;border:0;border-radius:7px;padding:8px 15px;font:600 13px system-ui;cursor:pointer}
.msX{background:#eee;color:#333;border:0;border-radius:7px;padding:7px 11px;cursor:pointer}
#mscl{padding:6px 9px;border-radius:6px;border:1px solid #ddd}
table.msT{border-collapse:collapse;width:100%}
.msT th,.msT td{border-bottom:1px solid #ededed;padding:8px 10px;text-align:center;font-size:12px;vertical-align:middle}
.msT th:first-child,.msT td:first-child{text-align:left;max-width:430px;word-break:break-all}
.msT .mp{font-family:ui-monospace,monospace}
.msT .rs{border:1px solid #ddd;background:#fff;border-radius:6px;cursor:pointer;color:#888;padding:1px 6px;margin-right:7px}
.msT .bar{height:6px;width:64px;background:#eef0f1;border-radius:4px;overflow:hidden;display:inline-block;vertical-align:middle}
.msT .bar>i{display:block;height:100%;background:#007BFF}
.bdg{font-size:11px;font-weight:600;padding:1px 7px;border-radius:20px}
.k-ok{color:#0069d9;background:#e6f4ff}.k-pend{color:#fa8c16;background:#fff7e6}.k-err{color:#ff4d4f;background:#fff2f0}.k-gh{color:#999;background:#f5f5f5}`;
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
  if(dirty){poll();return;}          // never clobber unsaved ticks (e.g. panel reopen)
  let clusters=[];
  try{nodes=await gm('GET','/nodes');models=await gm('GET','/models');clusters=await gm('GET','/clusters');}
  catch(e){setMsg('error: '+e.message);return;}
  const cname=Object.fromEntries(clusters.map(c=>[String(c.id),c.name]));
  const cs=[...new Set(nodes.map(n=>n.cluster_id))];const cur=cl.value;
  cl.innerHTML=cs.map(c=>`<option value="${esc(c)}">${esc(cname[String(c)]||('cluster '+c))}</option>`).join('');
  if(cs.map(String).includes(cur))cl.value=cur;
  render();poll();
}
function render(){
  const ns=vn();
  let h='<table class="msT"><thead><tr><th>model</th>'+ns.map(n=>{
    const st=n.state==='ready'?'#0069d9':(n.unreachable||n.state==='maintenance'?'#ff4d4f':'#fa8c16');
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
      if(s.state==='unreachable'){u.innerHTML='<span class="bdg k-err">unreachable</span>';return;}
      if(!s.complete){const left=s.need_bytes?' · '+gib(s.need_bytes)+' left':'';
        u.innerHTML=`<span class="bar"><i style="width:${Number(s.completion)||0}%"></i></span> <span style="font-size:10px;color:#999">${(Number(s.completion)||0).toFixed(0)}% ${esc(s.state)}${left}</span>`;return;}
      u.innerHTML=have?'<span class="bdg k-ok">ready</span>':'<span class="bdg k-pend">registering…</span>';return;
    }
    u.innerHTML=have?'<span class="bdg k-gh">present</span>':'';
  });
}
async function poll(){if(polling)return;polling=true;try{const st=await gm('GET','/status');stat={};st.forEach(s=>stat[s.path+'@'+s.worker_id]=s);paint();}catch(e){}finally{polling=false;}}
async function doApply(){
  const plan={};body.querySelectorAll('input[data-n]:checked').forEach(c=>{(plan[c.dataset.m]=plan[c.dataset.m]||[]).push(+c.dataset.n);});
  let rem=0;models.forEach(m=>m.nodes.forEach(id=>{if(!(plan[m.path]||[]).includes(id))rem++;}));
  if(rem&&!confirm('Remove '+rem+' cop'+(rem===1?'y':'ies')+'? (unshares + deregisters; files stay on disk)'))return;
  setMsg('applying…');
  try{const r=await gm('POST','/plan',{plan});const p=[];
    if(r.added&&r.added.length)p.push('added '+r.added.length);
    if(r.removed&&r.removed.length)p.push('removed '+r.removed.length+' (unshared + deregistered)');
    if(r.warnings&&r.warnings.length)p.push('⚠ '+r.warnings.join(' · '));
    setMsg(p.join(' · ')||'no changes');}
  catch(e){setMsg('error: '+e.message);}
  dirty=false;apply.textContent='Apply';await load();
}
async function reset(path){setMsg('resetting…');try{const r=await gm('POST','/reset',{path});
  setMsg(r.ok===false?('reset: '+(r.error||'failed')):('reset → '+((r.actions||[]).join(' · ')||'no targets')));}
  catch(e){setMsg('error: '+e.message);}poll();}

cl.onchange=render;apply.onclick=doApply;
close.onclick=()=>{panel.classList.remove('o');if(timer){clearInterval(timer);timer=null;}};
btn.onclick=()=>{panel.classList.add('o');load();if(!timer)timer=setInterval(()=>{poll();},3000);};
})();
"""
