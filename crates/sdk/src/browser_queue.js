// Browser I/O for the canonical SDK's already-redacted event/OTLP batches.
// One origin: <=8 MiB, 4096 records, 256 batches, 16 authenticated scopes.
// Expiry comes from the SDK's original batch lifetime. No credential lives here.
const DATABASE='nds-telemetry-v1', MAX_COUNTER=Number.MAX_SAFE_INTEGER;
const scopes=/^[a-f0-9]{64}$/, ids=/^[a-f0-9-]{36}$/;
const senders=new Map();
let operations=0;
const failure=()=>new Error('telemetry_store_unavailable');
const request=operation=>new Promise((resolve,reject)=>{
  operation.onsuccess=()=>resolve(operation.result);
  operation.onerror=()=>reject(failure());
});
function open() {
  return new Promise((resolve,reject)=>{
    const operation=indexedDB.open(DATABASE,1);
    let abandoned=false;
    const timer=setTimeout(()=>{abandoned=true;reject(failure());},5000);
    operation.onupgradeneeded=()=>{
      const db=operation.result;
      const records=db.createObjectStore('records',{keyPath:'id'});
      records.createIndex('scope','scope');
      records.createIndex('expiry','expires_at_ms');
      records.createIndex('scope_expiry',['scope','expires_at_ms']);
      db.createObjectStore('health',{keyPath:'key'});
    };
    operation.onblocked=operation.onerror=()=>{clearTimeout(timer);abandoned=true;reject(failure());};
    operation.onsuccess=()=>{
      clearTimeout(timer);
      const db=operation.result;db.onversionchange=()=>db.close();
      if(abandoned)db.close();else resolve(db);
    };
  });
}
const empty=()=>({pending:0,dropped:0,delivered:0,failures:0});
const increment=(value,amount)=>Math.min(MAX_COUNTER,value+amount);
const integer=value=>Number.isSafeInteger(value)&&value>=0;
function validHealth(value) {
  return value && ['pending','dropped','delivered','failures'].every(key=>integer(value[key]));
}
function output(value,revision) {
  return JSON.stringify({events:value.events,traces:value.traces,enabled:value.enabled,last_success_at:value.last_success_at,revision});
}
export function releaseSender(id) {
  const lease=senders.get(id);
  if(lease){senders.delete(id);clearTimeout(lease.timer);lease.release();}
}
export function acquireSender(scope,id) {
  if(!scopes.test(scope)||!ids.test(id)||senders.has(id)||senders.size>=16||!navigator.locks)return Promise.reject(failure());
  return new Promise((resolve,reject)=>{
    let release;
    const held=new Promise(done=>{release=done;});
    const lease={scope,release};senders.set(id,lease);
    // The accounts request deadline is 15s. The final bound also covers a
    // cancelled caller which never gets to poll its Rust lock guard again.
    lease.timer=setTimeout(()=>{releaseSender(id);reject(failure());},35000);
    navigator.locks.request('nds-telemetry-send:'+scope,{ifAvailable:true},async lock=>{
      if(!lock){releaseSender(id);resolve(false);return;}
      if(senders.get(id)!==lease)throw failure();
      resolve(true);await held;
    }).catch(()=>{releaseSender(id);reject(failure());});
  });
}
export async function queueOperation(scope,command,encoded,input) {
  if(!scopes.test(scope)||encoded.length>1024||input.length>65536||operations>=4)throw failure();
  // Copy before the first await: the WASM memory buffer may grow meanwhile.
  const bytes=new Uint8Array(input);
  const allowed=new Set(['append','health','next','enable','disable','acknowledge','failed','discard','losses']);
  if(!allowed.has(command))throw failure();
  let data;
  if(command==='append') {
    data=JSON.parse(encoded);
    if(!ids.test(data?.id)||!['events','traces'].includes(data.signal)||!integer(data.count)||data.count<1||data.count>32||!integer(data.expires_at_ms)||data.attempts!==0||bytes.length===0)throw failure();
  } else {
    if(bytes.length!==0)throw failure();
    if(command==='losses') {
      data=JSON.parse(encoded);
      if(!['events','traces'].every(signal=>data[signal]&&integer(data[signal].dropped)&&integer(data[signal].failures)))throw failure();
    } else if(['acknowledge','failed','discard'].includes(command)) {
      data=JSON.parse(encoded);
      if(!ids.test(data?.batch)||senders.get(data?.sender)?.scope!==scope)throw failure();
    } else if(command==='next' && senders.get(encoded)?.scope!==scope)throw failure();
    else if(command!=='next' && encoded!=='')throw failure();
  }
  operations++;
  let db,tx,timer,done;
  try {
    db=await open();
    tx=db.transaction(['records','health'],'readwrite',{durability:'strict'});
    done=new Promise((resolve,reject)=>{
      tx.oncomplete=()=>resolve();
      tx.onabort=tx.onerror=()=>reject(failure());
    });
    // Attach immediately, so an abort while another IDB request is awaited
    // never becomes an unhandled rejection containing browser driver details.
    done.catch(()=>{});
    timer=setTimeout(()=>{try{tx.abort();}catch{}},5000);
    const records=tx.objectStore('records'),health=tx.objectStore('health');
    const values=await request(health.getAll());
    if(values.length>17)throw failure();
    const state=new Map(values.map(value=>[value.key,value]));
    let total=state.get('global');
    if(!total) {
      if(values.length!==0||await request(records.count())!==0)throw failure();
      total={key:'global',bytes:0,records:0,batches:0,revision:0};
    }
    if(!['bytes','records','batches','revision'].every(key=>integer(total[key])))throw failure();
    for(const [key,value] of state) {
      if(key!=='global' && (!scopes.test(key)||!validHealth(value.events)||!validHealth(value.traces)||typeof value.enabled!=='boolean'||!integer(value.last_success_at)))throw failure();
    }
    let current=state.get(scope),changed=new Set(),globalChanged=!state.has('global');
    if(!current) {
      if([...state.keys()].filter(key=>key!=='global').length>=16)throw failure();
      current={key:scope,events:empty(),traces:empty(),enabled:true,last_success_at:0};
      state.set(scope,current);changed.add(scope);
    }
    function remove(record,disposition) {
      const owner=state.get(record.scope);
      if(!owner||!['events','traces'].includes(record.signal)||!integer(record.count)||record.count<1||record.count>32||!integer(record.size)||record.size<1||record.size>65536)throw failure();
      const signal=owner[record.signal];
      if(signal.pending<record.count||total.records<record.count||total.batches<1||total.bytes<record.size)throw failure();
      records.delete(record.id);
      signal.pending-=record.count;signal[disposition]=increment(signal[disposition],record.count);
      total.records-=record.count;total.batches--;total.bytes-=record.size;
      changed.add(record.scope);globalChanged=true;
    }
    const now=Date.now();
    for(const record of await request(records.index('expiry').getAll(IDBKeyRange.upperBound(now)))) {
      if(!record.inflight_until || record.inflight_until<=now)remove(record,'dropped');
    }
    let result;
    if(command==='append') {
      const previous=await request(records.get(data.id));
      if(previous)throw failure();
      if(!current.enabled||data.expires_at_ms<=now||total.batches>=256||total.records+data.count>4096||total.bytes+bytes.length>8*1024*1024) {
        current[data.signal].dropped=increment(current[data.signal].dropped,data.count);
      } else {
        records.add({...data,scope,size:bytes.length,bytes});
        current[data.signal].pending+=data.count;
        total.batches++;total.records+=data.count;total.bytes+=bytes.length;
        globalChanged=true;
      }
      changed.add(scope);
    } else if(command==='losses') {
      for(const signal of ['events','traces']) {
        current[signal].dropped=increment(current[signal].dropped,data[signal].dropped);
        current[signal].failures=increment(current[signal].failures,data[signal].failures);
      }
      changed.add(scope);
    } else if(command==='enable'||command==='disable') {
      const enabled=command==='enable';
      if(current.enabled!==enabled){current.enabled=enabled;changed.add(scope);}
      if(!enabled)for(const record of await request(records.index('scope').getAll(scope)))remove(record,'dropped');
    } else if(command==='next') {
      const cursor=current.enabled?await request(records.index('scope_expiry').openCursor(IDBKeyRange.bound([scope,0],[scope,MAX_COUNTER]))):null;
      if(cursor) {
        const record=cursor.value;
        if(!(record.bytes instanceof Uint8Array)||record.bytes.length!==record.size)throw failure();
        // Claim and expiry observation share this transaction. A short grace
        // keeps expiry sweeps from discarding an in-flight receipt. The Web
        // Lock still owns exclusion; a replacement sender can claim at once.
        record.inflight_until=now+35000;records.put(record);
        result=[JSON.stringify({id:record.id,signal:record.signal,count:record.count,expires_at_ms:record.expires_at_ms,attempts:record.attempts}),record.bytes];
      } else result=null;
    } else if(['acknowledge','failed','discard'].includes(command)) {
      const record=await request(records.get(data.batch));
      // A concurrent opt-out/expiry may already have discarded this exact ID.
      if(record) {
        if(record.scope!==scope)throw failure();
        if(command==='failed') {
          current[record.signal].failures=increment(current[record.signal].failures,1);
          record.attempts=Math.min(8,record.attempts+1);record.inflight_until=0;records.put(record);changed.add(scope);
        } else {
          remove(record,command==='acknowledge'?'delivered':'dropped');
          if(command==='acknowledge')current.last_success_at=Math.floor(now/1000);
        }
      }
    }
    if(globalChanged||changed.size) {
      if(total.revision===MAX_COUNTER)throw failure();
      total.revision++;globalChanged=true;
    }
    for(const key of changed)health.put(state.get(key));
    if(globalChanged)health.put(total);
    await done;
    return command==='next'?result:output(current,total.revision);
  } catch {
    if(tx)try{tx.abort();}catch{}
    if(done)await done.catch(()=>{});
    throw failure();
  } finally {clearTimeout(timer);db?.close();operations--;}
}
