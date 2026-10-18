import asyncio
import json
import os
from contextlib import asynccontextmanager
import httpx
from fastapi import FastAPI, HTTPException, Header, Request
from fastapi.responses import JSONResponse, Response
from .algorithms import Lamport

ID = os.getenv('NODE_ID', 'A')
PRIORITY = int(os.getenv('NODE_PRIORITY', {'A':1,'B':2,'C':3}[ID]))
PRIORITIES = json.loads(os.getenv('NODE_PRIORITIES','{"A":1,"B":2,"C":3}'))
PRIORITIES[ID] = PRIORITY
PEERS = json.loads(os.getenv('NODE_URLS','{"A":"http://127.0.0.1:8101","B":"http://127.0.0.1:8102","C":"http://127.0.0.1:8103"}'))
BACKEND = os.getenv('BACKEND_URL','http://127.0.0.1:8000')
KEY = os.getenv('DEMO_ADMIN_KEY','local-demo-admin')
clock = Lamport()
enabled = True
coordinator = None
history = []
requesting = None
held = False
deferred = {}
operation_lock = asyncio.Lock()
election_lock = asyncio.Lock()

def record(kind, **values):
    entry = {'kind':kind,'clock':clock.tick(), **values}
    history.append(entry)
    del history[:-200]
    print(json.dumps({'node':ID, **entry}), flush=True)

async def post(node, path, data):
    async with httpx.AsyncClient(timeout=2) as client:
        r = await client.post(PEERS[node]+path,json={**data,'clock':clock.tick()})
        r.raise_for_status()
        result = r.json()
        clock.tick(result.get('clock',0))
        return result

async def elect():
    global coordinator
    if election_lock.locked() or not enabled:
        return
    async with election_lock:
        record('election-started')
        higher = []
        for node in PEERS:
            if (PRIORITIES[node],node) > (PRIORITY,ID):
                try:
                    await post(node,'/election',{'candidate':ID})
                    higher.append(node)
                except httpx.HTTPError:
                    pass
        if higher:
            # Higher peers run their own election. A bounded wait allows a missing announcement retry.
            coordinator = None
            return
        coordinator = ID
        record('coordinator-announced',coordinator=ID)
        for node in PEERS:
            if node != ID:
                try:
                    await post(node,'/coordinator',{'node':ID})
                except httpx.HTTPError:
                    pass

async def heartbeat():
    global coordinator
    while True:
        if enabled:
            if coordinator is None:
                await elect()
            elif coordinator != ID:
                try:
                    async with httpx.AsyncClient(timeout=1) as client:
                        r = await client.get(PEERS[coordinator]+'/health')
                        r.raise_for_status()
                except httpx.HTTPError:
                    record('coordinator-suspected',coordinator=coordinator)
                    coordinator = None
                    await elect()
        await asyncio.sleep(float(os.getenv('HEARTBEAT_INTERVAL','2')))

@asynccontextmanager
async def lifespan(app):
    task = asyncio.create_task(heartbeat())
    yield
    task.cancel()

app = FastAPI(title=f'Relief node {ID}',lifespan=lifespan)

@app.get('/health')
def health():
    if not enabled:
        raise HTTPException(503,'Simulated node failure')
    return {'id':ID,'status':'ok'}

@app.get('/status')
def status():
    return {'id':ID,'priority':PRIORITY,'enabled':enabled,'coordinator':coordinator,'clock':clock.value,'requesting':requesting,'held':held,'deferred':list(deferred),'history':history}

def receive(data):
    if not enabled:
        raise HTTPException(503,'Simulated failure')
    clock.tick(data.get('clock',0))

@app.post('/election')
async def election(data: dict):
    receive(data)
    record('election-received',candidate=data['candidate'])
    asyncio.create_task(elect())
    return {'answer':'OK','clock':clock.tick()}

@app.post('/coordinator')
async def announce(data: dict):
    global coordinator
    receive(data)
    coordinator = data['node']
    record('coordinator-received',coordinator=coordinator)
    if (PRIORITIES[coordinator],coordinator) < (PRIORITY,ID):
        asyncio.create_task(elect())
    return {'clock':clock.tick()}

@app.post('/mutex/request')
async def mutex_request(data: dict):
    receive(data)
    incoming = (int(data['timestamp']),data['node'])
    # Ricart–Agrawala: grant only when released or the incoming request precedes ours.
    if held or (requesting and tuple(requesting) < incoming):
        future = asyncio.get_running_loop().create_future()
        deferred[data['token']] = future
        record('mutex-deferred',peer=data['node'])
        try:
            await asyncio.wait_for(future,6)
        finally:
            deferred.pop(data['token'],None)
    record('mutex-reply',peer=data['node'])
    return {'reply':True,'clock':clock.tick()}

async def release():
    global requesting, held
    requesting, held = None, False
    for future in list(deferred.values()):
        if not future.done():
            future.set_result(True)
    record('mutex-released')

@app.post('/control/{action}')
async def control(action: str,x_demo_key: str = Header(default='')):
    global enabled, coordinator
    if x_demo_key != KEY:
        raise HTTPException(403,'Demo key required')
    if action not in ['stop','start','elect']:
        raise HTTPException(404,'Unknown action')
    enabled = action != 'stop'
    if enabled:
        coordinator = None
        asyncio.create_task(elect())
    record('simulated-'+action)
    return status()

@app.post('/rpc')
async def rpc(request: Request):
    global requesting, held
    try:
        body = await request.json()
    except Exception:
        return {'jsonrpc':'2.0','id':None,'error':{'code':-32700,'message':'Parse error'}}
    if not isinstance(body,dict) or body.get('jsonrpc') != '2.0' or not isinstance(body.get('method'),str) or not isinstance(body.get('params',{}),dict):
        return {'jsonrpc':'2.0','id':body.get('id') if isinstance(body,dict) else None,'error':{'code':-32600,'message':'Invalid request'}}
    reply = {'jsonrpc':'2.0','id':body.get('id')}
    try:
        if not enabled:
            raise ValueError('Node is simulated offline')
        method, params = body['method'],body.get('params',{})
        if method == 'node.status':
            result = status()
        elif method in ['inventory.query','incident.status']:
            async with httpx.AsyncClient(timeout=3) as client:
                r = await client.get(BACKEND+('/api/state' if method == 'inventory.query' else '/api/incidents/'+params['id']))
                r.raise_for_status()
                result = [i for i in r.json()['inventory'] if i['center_id']==ID] if method == 'inventory.query' else r.json()
        elif method == 'reservation.request':
            if params.get('center_id') != ID:
                raise ValueError('Reservation belongs to a different node')
            async with operation_lock:
                requesting = (clock.tick(),ID)
                record('mutex-requested',timestamp=requesting[0])
                try:
                    # Fixed membership: all peers must reply. Never enter after a timeout.
                    async def permission(node):
                        async with httpx.AsyncClient(timeout=7) as client:
                            r = await client.post(PEERS[node]+'/mutex/request',json={'node':ID,'timestamp':requesting[0],'clock':clock.tick(),'token':params['key']+ID})
                            r.raise_for_status()
                            clock.tick(r.json()['clock'])
                    await asyncio.gather(*(permission(n) for n in PEERS if n != ID))
                    if not enabled:
                        raise ValueError('Node failed during permission collection')
                    held = True
                    record('mutex-entered')
                    async with httpx.AsyncClient(timeout=3) as client:
                        r = await client.post(BACKEND+'/api/internal/reserve',json=params,headers={'X-Demo-Key':KEY})
                        if r.status_code >= 400:
                            raise ValueError(r.json().get('detail','Reservation failed'))
                        result = r.json()
                finally:
                    await release()
        else:
            reply['error'] = {'code':-32601,'message':'Method not found'}
            return reply
        reply['result'] = result
    except (KeyError,ValueError,httpx.HTTPError,asyncio.TimeoutError) as exc:
        reply['error'] = {'code':-32000,'message':str(exc) or 'Coordination timeout: all peer replies required'}
    return JSONResponse(reply) if 'id' in body else Response(status_code=204)
