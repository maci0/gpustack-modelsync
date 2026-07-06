"""Behavioral tests of the SHIPPED in-page JS (modelsync.web.SCRIPT), run under
node. The JS was previously only syntax-checked; its load-bearing pure helpers
(sameSet -> dirty detection, splitPath -> sort/display/purge name, selOf's
empty-edit semantics, gib) had no behavioral coverage. Skips if node is absent."""

import shutil
import subprocess

import pytest

from modelsync.web import SCRIPT, USERSCRIPT

node = shutil.which("node")

# Minimal DOM/env stubs so SCRIPT loads without a browser. fetch REJECTS, so the
# bootstrap IIFE (loadNodes/loadModels/poll) no-ops instead of touching the DOM;
# the pure helpers under test are then in file scope, callable directly.
HARNESS = r"""
const _el=()=>({value:'',innerHTML:'',className:'',textContent:'',checked:false,
  indeterminate:false,addEventListener(){},querySelectorAll(){return[]},
  querySelector(){return null},closest(){return null},dataset:{},
  classList:{toggle(){},add(){},remove(){}},focus(){},blur(){},appendChild(){},remove(){},select(){}});
const _els={};  // persistent by id so tests can set e.g. the cluster selector value
globalThis.document={getElementById:id=>_els[id]||(_els[id]=_el()),
  querySelectorAll:()=>[],addEventListener(){},
  createElement:_el,body:_el(),head:_el(),activeElement:{tagName:'BODY'}};
globalThis.localStorage={getItem:()=>'',setItem(){},removeItem(){}};
globalThis.fetch=()=>Promise.reject(new Error('no net in test'));
globalThis.setInterval=()=>0;
globalThis.EventSource=function(){return{};};
globalThis.navigator={};globalThis.window={};
"""

ASSERTS = r"""
function eq(a,b,msg){const x=JSON.stringify(a),y=JSON.stringify(b);
  if(x!==y){console.error('FAIL '+msg+': '+x+' !== '+y);process.exit(1);}}

// splitPath: last segment is the name, the rest is the org; trailing slash trimmed
eq(splitPath('/a/b/Qwen2.5-7B'),{name:'Qwen2.5-7B',org:'/a/b'},'splitPath basic');
eq(splitPath('/a/b/'),{name:'b',org:'/a'},'splitPath trailing slash');
eq(splitPath('model'),{name:'model',org:''},'splitPath bare');

// sameSet: order-insensitive set equality (drives no-op edit pruning)
eq(sameSet([1,2],[2,1]),true,'sameSet reorder');
eq(sameSet([1],[1,2]),false,'sameSet len differ');
eq(sameSet([],[]),true,'sameSet empty');
eq(sameSet([3,3],[3]),false,'sameSet dup vs single');

// selOf: an explicit empty edit ([]) means "remove from ALL", NOT fall back to plan
edits={};
eq(selOf({path:'/x',nodes:[1,2]}),[1,2],'selOf no edit -> plan');
edits={'/x':[]};
eq(selOf({path:'/x',nodes:[1,2]}),[],'selOf empty edit respected');
edits={'/x':[9]};
eq(selOf({path:'/x',nodes:[1,2]}),[9],'selOf edit overrides plan');

// gib: 0 renders '0G'; sub-GiB keeps a decimal; >=GiB rounds to integer.
// The >= boundary matters: exactly 1 GiB must read '1G', not '1.0G'.
eq(gib(0),'0G','gib zero');
eq(gib(1073741824),'1G','gib exact-GiB boundary');
eq(gib(1073741823),'1.0G','gib just under GiB keeps decimal');
eq(gib(2147483648),'2G','gib multi-GiB');
eq(gib(536870912),'0.5G','gib half');

// recordRow must PRESERVE planned nodes on other clusters (columns not shown):
// unticking node 1 while viewing cluster 1 must leave node 2 (cluster 2) planned.
nodes=[{id:1,cluster_id:1,name:'a'},{id:2,cluster_id:2,name:'b'}];
models=[{path:'/m',nodes:[1,2],have:[1,2],size:1}];
edits={};
document.getElementById('cluster').value='1';        // viewing cluster 1; node 2 hidden
recordRow({dataset:{path:'/m'},
  querySelectorAll:()=>[{checked:false,dataset:{n:'1'}}]});  // untick the in-view node 1
eq((edits['/m']||[]).slice().sort(),[2],'recordRow keeps other-cluster node 2');

// updateStats must count only the cluster IN VIEW: status rows span all clusters,
// so an error on a hidden cluster must not inflate the visible error count.
models=[{path:'/m'}];edits={};unre={};
statusMap={'/m@1':{worker_id:1,path:'/m',complete:false,errors:2,state:'idle'},
           '/m@2':{worker_id:2,path:'/m',complete:false,errors:9,state:'idle'}};
document.getElementById('cluster').value='1';    // cluster 1 in view; node 2 hidden
updateStats();
const _sh=document.getElementById('stats').innerHTML;
if(!/errors <b>1<\/b>/.test(_sh)){console.error('FAIL updateStats leaks other-cluster errors: '+_sh);process.exit(1);}

console.log('ALL-OK');
"""


@pytest.mark.skipif(node is None, reason="node not installed")
def test_shipped_js_pure_helpers(tmp_path):
    f = tmp_path / "harness.js"
    f.write_text(HARNESS + "\n" + SCRIPT + "\n" + ASSERTS)
    r = subprocess.run([node, str(f)], capture_output=True, text=True, timeout=30)
    assert r.returncode == 0, f"node failed:\n{r.stdout}\n{r.stderr}"
    assert "ALL-OK" in r.stdout


# Hand-rolled minimal DOM (no jsdom -> no host pollution). Every element is made
# via document.createElement, which we intercept to collect them, so we can find
# the real shipped button/select/body and drive doApply end-to-end. body.innerHTML
# is parsed just enough to expose <input data-n ...:checked> to querySelectorAll.
US_HARNESS = r"""
const created=[];
let lastPlan=null;
const routes={
  '/nodes':[{id:1,cluster_id:1,name:'a',state:'ready',have:[],gpu_name:null,vram_total:0},
            {id:2,cluster_id:2,name:'b',state:'ready',have:[],gpu_name:null,vram_total:0}],
  '/models':[{path:'/m',nodes:[1,2],have:[1,2],pending:[],serving:[],size:1073741824}],
  '/clusters':[{id:1,name:'c1'},{id:2,name:'c2'}],
  '/status':[],
};
globalThis.GM_getValue=(k,d)=>k==='token'?'tok':d;
globalThis.GM_setValue=()=>{};
globalThis.GM_xmlhttpRequest=({method,url,data,onload})=>{
  const p=url.slice('http://x'.length);
  if(method==='POST'&&p==='/plan'){lastPlan=JSON.parse(data).plan;onload({status:200,responseText:'{"ok":true}'});return;}
  onload({status:200,responseText:JSON.stringify(routes[p]||[])});
};
globalThis.confirm=()=>true;globalThis.prompt=()=>'tok';
globalThis.setInterval=()=>0;globalThis.clearInterval=()=>{};
function mkEl(tag){
  const el={tagName:(tag||'div').toUpperCase(),className:'',id:'',style:{},dataset:{},
    textContent:'',value:'',checked:false,type:'',children:[],_inputs:[],
    onclick:null,onchange:null,onmessage:null,
    appendChild(c){el.children.push(c);return c;},
    addEventListener(){},remove(){},focus(){},blur(){},select(){},
    classList:{add(){},remove(){},toggle(){},contains(){return false;}},
    querySelector(){return null;},
    get innerHTML(){return el._h||'';},
    set innerHTML(v){el._h=v;el._inputs=[];const re=/<input\b([^>]*)>/g;let m;
      while((m=re.exec(v))){const a=m[1];
        const dm=/data-m="([^"]*)"/.exec(a),dn=/data-n="([^"]*)"/.exec(a);
        el._inputs.push({type:/checkbox/.test(a)?'checkbox':'',checked:/\bchecked\b/.test(a),
          dataset:{m:dm&&dm[1],n:dn&&dn[1]},onchange:null});}},
    querySelectorAll(sel){
      if(/data-n/.test(sel)&&/:checked/.test(sel))return el._inputs.filter(i=>i.checked);
      if(/type=checkbox/.test(sel))return el._inputs;
      return [];}};
  return el;
}
globalThis.document={createElement:t=>{const e=mkEl(t);created.push(e);return e;},
  head:mkEl('head'),body:mkEl('body'),addEventListener(){},getElementById:()=>mkEl()};
const flush=()=>new Promise(r=>setTimeout(r,0));
"""

US_DRIVE = r"""
(async()=>{
  try{
    const btn=created.find(e=>e.id==='msb');
    const cl=created.find(e=>e.id==='mscl');
    const apply=created.find(e=>e.className==='msB');
    if(!btn||!cl||!apply){console.error('FAIL: shipped elements not found');process.exit(1);}
    btn.onclick(); await flush(); await flush();   // open -> load() -> render()
    cl.value='1'; cl.onchange(); await flush();     // view cluster 1 ONLY (node 2 hidden)
    await apply.onclick(); await flush();           // real doApply
    if(!lastPlan){console.error('FAIL: no /plan POSTed');process.exit(1);}
    const got=(lastPlan['/m']||[]).slice().sort((a,b)=>a-b);
    if(JSON.stringify(got)!=='[1,2]'){
      console.error('FAIL: applying on cluster 1 dropped cluster-2 node from plan: '+JSON.stringify(got));
      process.exit(1);}
    console.log('ALL-OK');
  }catch(e){console.error('FAIL exception: '+(e&&e.stack||e));process.exit(1);}
})();
"""


@pytest.mark.skipif(node is None, reason="node not installed")
def test_userscript_apply_preserves_other_cluster_plan(tmp_path):
    us = (USERSCRIPT.replace("__API__", "http://x")
          .replace("__GP_ORIGIN__", "http://y").replace("__ORCH_HOST__", "y"))
    f = tmp_path / "us_harness.js"
    f.write_text(US_HARNESS + "\n" + us + "\n" + US_DRIVE)
    r = subprocess.run([node, str(f)], capture_output=True, text=True, timeout=30)
    assert r.returncode == 0, f"node failed:\n{r.stdout}\n{r.stderr}"
    assert "ALL-OK" in r.stdout
