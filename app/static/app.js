const $=id=>document.getElementById(id);let state={};
async function api(path,opts={}){const r=await fetch(path,{headers:{'Content-Type':'application/json'},...opts});const j=await r.json();if(!r.ok)throw new Error(j.error||'Request failed');return j}
function esc(v){return String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]))}
function opts(pools,selected){return pools.map(p=>`<option ${p===selected?'selected':''}>${esc(p)}</option>`).join('')}
function render(){
 const pools=Object.keys(state.config?.pools||{}),groups=state.config?.groups||{};
 $('group-pool').innerHTML='<option value="">Select pool…</option>'+opts(pools,'');
 $('groups').innerHTML=Object.entries(groups).map(([name,g])=>`<article class="group"><div><b>${esc(name)}</b><small>Worker prefix: <code>${esc(g.match)}</code></small></div><select data-group="${esc(name)}">${opts(pools,g.pool)}</select><button data-group-switch="${esc(name)}">Switch all</button><button class="danger" data-group-delete="${esc(name)}">Delete</button></article>`).join('')||'<p>No rental groups yet.</p>';
 $('miners').innerHTML=(state.sessions||[]).map(s=>`<article><div><b>${esc(s.worker||s.ip)}</b><small>${esc(s.ip)} · ${esc(s.status)}${s.group?' · '+esc(s.group):''}</small></div><span class="route">${esc(s.pool||'Unassigned')}</span></article>`).join('')||'<p>No miners connected yet.</p>';
 $('pools').innerHTML=pools.map(p=>{const x=state.config.pools[p];return `<div class="pool"><b>${esc(p)}</b><code>${esc(x.host)}:${x.port}</code></div>`}).join('')||'<p>No pools configured.</p>';$('status').textContent='Online'
}
async function refresh(){try{state=await api('/api/state');render()}catch(e){$('status').textContent='Offline'}}
document.addEventListener('click',async e=>{
 if(e.target.dataset.groupSwitch){const name=e.target.dataset.groupSwitch,sel=document.querySelector(`select[data-group="${CSS.escape(name)}"]`);await api('/api/group-pool',{method:'POST',body:JSON.stringify({name,pool:sel.value})});setTimeout(refresh,500)}
 if(e.target.dataset.groupDelete){await api('/api/group-delete',{method:'POST',body:JSON.stringify({name:e.target.dataset.groupDelete})});refresh()}
});
$('group-form').addEventListener('submit',async e=>{e.preventDefault();await api('/api/groups',{method:'POST',body:JSON.stringify({name:$('group-name').value.trim(),match:$('group-match').value.trim(),pool:$('group-pool').value})});e.target.reset();refresh()});
$('pool-form').addEventListener('submit',async e=>{e.preventDefault();await api('/api/pools',{method:'POST',body:JSON.stringify({name:$('name').value.trim(),host:$('host').value.trim(),port:Number($('port').value)})});e.target.reset();refresh()});
refresh();setInterval(refresh,2000);
