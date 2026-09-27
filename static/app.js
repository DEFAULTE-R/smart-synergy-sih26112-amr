
async function edgeStart(){
  const r=await fetch('/api/edge/start'); const x=await r.json(); renderEdge(x);
}
async function edgeRun(){
  document.getElementById('edgeStatus').textContent='RUNNING...';
  const r=await fetch('/api/edge/run?seconds=12'); const x=await r.json(); renderEdge(x);
  document.getElementById('edgeStatus').textContent=x.all_tasks_completed?'COMPLETE':'RUN COMPLETE';
}
async function edgeStop(){
  const r=await fetch('/api/edge/stop'); const x=await r.json(); renderEdge(x.state);
  document.getElementById('edgeStatus').textContent='STOPPED';
}
async function renderEdge(x){
  document.getElementById('edgeProcs').textContent=x.processes??'—';
  document.getElementById('edgeAlive').textContent=x.alive_processes??'—';
  document.getElementById('edgeSent').textContent=x.total_packets_sent??x.robots?.reduce((n,r)=>n+(r.packets_sent||0),0)??'—';
  document.getElementById('edgeRecv').textContent=x.total_packets_received??x.robots?.reduce((n,r)=>n+(r.packets_received||0),0)??'—';
  document.getElementById('edgeDone').textContent=x.all_tasks_completed===undefined?'—':(x.all_tasks_completed?'YES':'NO');
  document.getElementById('edgeTable').innerHTML=(x.robots||[]).map(r=>`<div class="benchrow"><b>${r.id}</b><span>(${r.x}, ${r.y})</span><span>${r.stage||'—'}</span><span>${r.completed||0} tasks</span><span>${r.packets_sent||0}↑ / ${r.packets_received||0}↓</span></div>`).join('');
}

async function runValidation(){
  if(valBusy)return;
  valBusy=true;
  document.getElementById('validationStatus').textContent='RUNNING 10 TRIALS...';
  try{
    const r=await fetch('/api/validation?trials=10');
    const v=await r.json();
    const o=v.overall||{};
    document.getElementById('valMean').textContent=o.mean_improvement_pct===null?'N/A':o.mean_improvement_pct+'%';
    document.getElementById('valMedian').textContent=o.median_improvement_pct===null?'N/A':o.median_improvement_pct+'%';
    const ci=o.ci95_improvement||{};
    document.getElementById('valCI').textContent=(ci.ci95_low===null?'N/A':`${ci.ci95_low}%–${ci.ci95_high}%`);
    document.getElementById('valSuccess').textContent=`${o.successful_pairs||0}`;
    document.getElementById('validationTable').innerHTML=(v.scenarios||[]).map(x=>{
      const d=x.distributed,b=x.stopwait,imp=x.improvement_pct||{};
      return `<div class="benchrow">
        <b>${x.scenario.replace('_',' ').toUpperCase()}</b>
        <span>${d.sim_time.mean??'N/A'}s</span>
        <span>${b.sim_time.mean??'N/A'}s</span>
        <span class="imp">${imp.mean??'N/A'}%</span>
        <span>${d.completion_rate_pct}% / ${b.completion_rate_pct}%</span>
      </div>`;
    }).join('');
    document.getElementById('validationStatus').textContent='COMPLETE';
    document.getElementById('validationNote').textContent=
      `Paired validation: ${v.trials_per_scenario} trials/scenario, identical seeds per mode (${v.seed_range[0]}–${v.seed_range[1]}). Mean improvement is descriptive and should be independently repeated before being presented as final evidence.`;
  }catch(e){
    document.getElementById('validationStatus').textContent='ERROR';
  }finally{valBusy=false}
}
async function runFairness(){
  if(fairBusy)return;
  fairBusy=true;
  document.getElementById('fairnessStatus').textContent='RUNNING 10 × 3000 TICKS...';
  try{
    const r=await fetch('/api/fairness?trials=10&ticks=3000');
    const f=await r.json();
    document.getElementById('fairMean').textContent=f.mean_fairness_index;
    document.getElementById('fairMin').textContent=f.min_fairness_index;
    document.getElementById('fairWait').textContent=f.max_wait_ticks;
    document.getElementById('fairColl').textContent=`${f.collision_free_trials}/${f.trials}`;
    document.getElementById('fairThroughput').textContent=f.mean_completed;
    document.getElementById('fairnessTable').innerHTML=(f.rows||[]).map(x=>{
      const vals=Object.entries(x.completed_by_robot).map(([k,v])=>`${k}: ${v}`).join(' · ');
      return `<div class="benchrow"><b>SEED ${x.seed}</b><span>${x.completed_total} tasks</span><span>${vals}</span><span>FI ${x.fairness_index}</span><span>${x.collisions} coll · ${x.deadlocks} dead</span></div>`;
    }).join('');
    document.getElementById('fairnessStatus').textContent='COMPLETE';
    document.getElementById('fairnessNote').textContent=`Continuous-load validation: ${f.trials} trials × ${f.ticks} ticks. Fairness Index is Jain's index over completed tasks per AMR; higher means more even service. This is a diagnostic, not a SIH performance claim.`;
  }catch(e){document.getElementById('fairnessStatus').textContent='ERROR'}finally{fairBusy=false}
}
function downloadValidation(){window.location='/api/validation_export?trials=10'}
function downloadFairness(){window.location='/api/fairness_export?trials=10&ticks=3000'}
const canvas=document.getElementById('map'),ctx=canvas.getContext('2d');
const W=24,H=14; let state=null,benchBusy=false,valBusy=false,fairBusy=false,currentScenario='normal';
async function getState(){const r=await fetch('/api/state');state=await r.json();currentScenario=state.scenario;render()}
async function resetSim(mode='distributed',scenario=currentScenario){await fetch('/api/reset?mode='+mode+'&scenario='+encodeURIComponent(scenario));await getState()}
async function setScenario(s){currentScenario=s;await resetSim('distributed',s)}
async function failRobot(id){await fetch('/api/fail?robot='+encodeURIComponent(id));await getState()}
async function injectNetworkLoss(){await fetch('/api/network?ticks=20&drop=1');await getState()}
function downloadRun(){window.location='/api/export'}
async function runBenchmark(){if(benchBusy)return;benchBusy=true;document.getElementById('benchStatus').textContent='RUNNING 5 SCENARIOS...';try{const r=await fetch('/api/benchmark');const b=await r.json();document.getElementById('benchTable').innerHTML=b.scenarios.map(x=>{const d=x.distributed,base=x.baseline;return `<div class="benchrow"><b>${x.scenario.replace('_',' ').toUpperCase()}</b><span>${d.sim_time}s · ${d.completed}/${d.tasks_total}</span><span>${base.sim_time}s · ${base.completed}/${base.tasks_total}</span><span class="imp">${x.improvement_pct===null?'N/A':x.improvement_pct+'%'}</span><span>${d.collisions} / ${base.collisions}</span></div>`}).join('');document.getElementById('benchStatus').textContent='COMPLETE'}finally{benchBusy=false}}
function render(){document.getElementById('clock').textContent=state.time+'s';document.getElementById('done').textContent=state.completed+'/'+state.tasks_total;document.getElementById('collisions').textContent=state.collisions;document.getElementById('msgs').textContent=state.robots.reduce((a,r)=>a+r.messages,0);document.getElementById('replans').textContent=state.robots.reduce((a,r)=>a+r.replans,0);document.getElementById('deadlocks').textContent=state.deadlocks;document.getElementById('reassign').textContent=state.reassignments;document.getElementById('dropped').textContent=state.robots.reduce((a,r)=>a+r.messages_dropped,0);document.getElementById('stale').textContent=state.stale_peer_holds;document.getElementById('blockReassign').textContent=state.blockage_reassignments;document.getElementById('systemMode').textContent=`${state.mode.toUpperCase()} · ${state.scenario.replace('_',' ').toUpperCase()}`;const tp=state.transport||{};document.getElementById('tpSent').textContent=tp.packets_sent??0;document.getElementById('tpRecv').textContent=tp.packets_received??0;document.getElementById('tpDrop').textContent=tp.packets_dropped??0;document.getElementById('transportHealth').textContent=tp.enabled?'UDP ACTIVE':'UDP OFF';document.getElementById('network').textContent=state.network_healthy?'P2P HEALTHY':`P2P DEGRADED · ${state.network_loss_ticks} TICKS`;document.getElementById('block').textContent=state.dynamic_block?`DYNAMIC BLOCKAGE @ ${state.dynamic_block.join(',')}`:'NO ACTIVE BLOCKAGE';document.getElementById('robots').innerHTML=state.robots.map(r=>`<div class="robot"><div class="rline"><span class="id">${r.id}</span><span class="status">${r.status}</span></div><div class="meta">Task ${r.task||'—'} · Target ${r.target.join(',')} · Priority ${r.priority}</div><div class="bar"><i style="width:${r.battery}%"></i></div><div class="meta">Battery ${r.battery}% · Conflicts ${r.conflicts} · Replans ${r.replans} · Peer msgs ${r.messages} · Drops ${r.messages_dropped}</div></div>`).join('');document.getElementById('tasks').innerHTML=state.tasks.map(t=>`<div class="task"><b>${t.id}</b> <span class="pill">${t.stage}</span><div class="meta">${t.pickup.join(',')} → ${t.drop.join(',')} · ${t.assigned||'unassigned'}</div></div>`).join('');document.getElementById('log').innerHTML=state.log.map(e=>`<div class="event"><time>${e.t}s</time><b>${e.src}</b>${e.msg}</div>`).join('');draw()}
function draw(){const dpr=devicePixelRatio||1,w=canvas.clientWidth,h=canvas.clientHeight;canvas.width=w*dpr;canvas.height=h*dpr;ctx.setTransform(dpr,0,0,dpr,0,0);ctx.clearRect(0,0,w,h);const cw=w/W,ch=h/H;ctx.strokeStyle='#1c1f24';ctx.lineWidth=1;for(let x=0;x<=W;x++){ctx.beginPath();ctx.moveTo(x*cw,0);ctx.lineTo(x*cw,h);ctx.stroke()}for(let y=0;y<=H;y++){ctx.beginPath();ctx.moveTo(0,y*ch);ctx.lineTo(w,y*ch);ctx.stroke()}state.blocks.forEach(([x,y])=>{ctx.fillStyle='#272a2f';ctx.fillRect(x*cw,y*ch,cw,ch)});state.tasks.forEach(t=>{let[x,y]=t.pickup;ctx.fillStyle=t.stage==='DONE'?'#151719':'#333';ctx.fillRect(x*cw+cw*.3,y*ch+ch*.3,cw*.4,ch*.4);[x,y]=t.drop;ctx.strokeStyle='#555';ctx.strokeRect(x*cw+cw*.3,y*ch+ch*.3,cw*.4,ch*.4)});if(state.dynamic_block){const[x,y]=state.dynamic_block;ctx.fillStyle='#b32626';ctx.fillRect(x*cw,y*ch,cw,ch)}state.robots.forEach(r=>{if(r.route.length>1){ctx.strokeStyle='#777';ctx.setLineDash([4,4]);ctx.beginPath();ctx.moveTo(r.x*cw+cw/2,r.y*ch+ch/2);r.route.slice(1).forEach(([x,y])=>ctx.lineTo(x*cw+cw/2,y*ch+ch/2));ctx.stroke();ctx.setLineDash([])}});state.robots.forEach(r=>{let x=r.x*cw+cw/2,y=r.y*ch+ch/2;ctx.beginPath();ctx.arc(x,y,Math.min(cw,ch)*.35,0,Math.PI*2);ctx.fillStyle=r.status==='FAILED'?'#777':'#e33';ctx.fill();ctx.fillStyle='#fff';ctx.font='bold 10px Arial';ctx.textAlign='center';ctx.textBaseline='middle';ctx.fillText(r.id.split('-')[1],x,y)})}
getState();setInterval(getState,250);window.addEventListener('resize',()=>state&&draw());
