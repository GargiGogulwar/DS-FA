import asyncio
import json
import logging
import math
import os
import time
from contextlib import asynccontextmanager
from typing import Literal
import httpx
from fastapi import FastAPI, Depends, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy import select, update, func, text
from .db import *
from .algorithms import Lamport, Snapshot, cycle

logging.basicConfig(level=logging.INFO, format='%(message)s')
clock = Lamport()
RESOURCES = ['water', 'food_kits', 'medical_kits', 'ambulances', 'rescue_boats', 'rescue_teams', 'shelter_supplies']
NODE_URLS = json.loads(os.getenv('NODE_URLS', '{"A":"http://127.0.0.1:8101","B":"http://127.0.0.1:8102","C":"http://127.0.0.1:8103"}'))

def emit(db, kind, aggregate_id='', payload=None):
    # The database serializes logical timestamp allocation across backend processes.
    if db.bind.dialect.name == 'postgresql':
        db.execute(text('SELECT pg_advisory_xact_lock(729145)'))
    latest = db.scalar(select(func.max(Event.clock))) or 0
    event = Event(kind=kind, aggregate_id=aggregate_id, payload=payload or {}, clock=clock.tick(latest))
    db.add(event)
    db.flush()
    return event

def database():
    with Session() as db:
        yield db

def admin(x_demo_key: str = Header(default='')):
    if x_demo_key != os.getenv('DEMO_ADMIN_KEY', 'local-demo-admin'):
        raise HTTPException(403, 'Administrative demo key required')

async def monitor():
    async with httpx.AsyncClient(timeout=1) as client:
        while True:
            for node, url in NODE_URLS.items():
                with Session() as db:
                    previous = db.get(NodeHealth, node)
                    try:
                        r = await client.get(url + '/status')
                        r.raise_for_status()
                        state = r.json()
                        if not state['enabled']:
                            state['health'] = 'simulated-offline'
                        else:
                            state['health'] = 'online'
                        last = now()
                    except (httpx.HTTPError, ValueError):
                        last = previous.last_seen if previous else now()
                        elapsed = (now().replace(tzinfo=None) - last.replace(tzinfo=None)).total_seconds()
                        state = {**(previous.state if previous else {}), 'id': node, 'health': 'suspected' if elapsed < 6 else 'unreachable', 'missed_heartbeats': int(elapsed / 2)}
                    if previous is None or previous.state.get('health') != state['health']:
                        emit(db, 'node-health-changed', node, state)
                    if previous and previous.state.get('coordinator') != state.get('coordinator'):
                        emit(db, 'coordinator-changed', node, state)
                    if previous:
                        previous.state, previous.last_seen = state, last
                    else:
                        db.add(NodeHealth(id=node, state=state, last_seen=last))
                    db.commit()
            await asyncio.sleep(2)

@asynccontextmanager
async def lifespan(app):
    task = asyncio.create_task(monitor())
    yield
    task.cancel()

app = FastAPI(title='ReliefNet simulated disaster coordination', lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=[os.getenv('FRONTEND_ORIGIN', 'http://localhost:5173')], allow_methods=['*'], allow_headers=['*'])

@app.middleware('http')
async def correlation(request: Request, call_next):
    correlation_id = request.headers.get('X-Correlation-ID', uid())[:100]
    try:
        response = await call_next(request)
    except Exception:
        logging.exception(json.dumps({'correlation_id': correlation_id, 'path': request.url.path}))
        response = JSONResponse(status_code=500, content={'detail': 'Internal operation failed', 'correlation_id': correlation_id})
    response.headers['X-Correlation-ID'] = correlation_id
    logging.info(json.dumps({'correlation_id':correlation_id,'method':request.method,'path':request.url.path,'status':response.status_code}))
    return response

class IncidentInput(BaseModel):
    title: str = Field(min_length=3, max_length=150)
    description: str = Field(default='', max_length=5000)
    disaster_type: Literal['Flood', 'Earthquake', 'Cyclone', 'Fire', 'Landslide']
    severity: Literal['Critical', 'High', 'Medium', 'Low']
    location: str = Field(min_length=2, max_length=200)
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)
    people_affected: int = Field(ge=0)
    requirements: dict[str, int] = Field(default_factory=dict)

def quantities(values):
    if not values or any(k not in RESOURCES or isinstance(v, bool) or v <= 0 for k, v in values.items()):
        raise HTTPException(422, 'Select known resources with positive integer quantities')

def required(db, model, key):
    value = db.get(model, key)
    if value is None:
        raise HTTPException(404, 'Record not found')
    return value

@app.get('/health')
def health(db=Depends(database)):
    db.execute(select(1))
    return {'status': 'ok', 'simulated': True}

@app.get('/api/state')
def state(db=Depends(database)):
    return {key: [serialize(row) for row in db.scalars(select(model))] for key, model in [('incidents', Incident), ('centers', Center), ('inventory', Inventory), ('reservations', Reservation), ('events', Event), ('nodes', NodeHealth)]}

@app.post('/api/incidents', status_code=201)
def create_incident(body: IncidentInput, db=Depends(database)):
    quantities(body.requirements)
    row = Incident(**body.model_dump())
    db.add(row)
    db.flush()
    emit(db, 'incident-created', row.id, body.model_dump())
    db.commit()
    return serialize(row)

@app.get('/api/incidents/{key}')
def get_incident(key: str, db=Depends(database)):
    return serialize(required(db, Incident, key))

@app.put('/api/incidents/{key}')
def edit_incident(key: str, body: IncidentInput, db=Depends(database)):
    quantities(body.requirements)
    row = db.scalar(select(Incident).where(Incident.id == key).with_for_update())
    if not row:
        raise HTTPException(404, 'Incident not found')
    if row.status != 'Reported':
        raise HTTPException(409, 'Only reported incidents can be edited')
    for k, v in body.model_dump().items():
        setattr(row, k, v)
    row.version += 1
    emit(db, 'incident-updated', key, body.model_dump())
    db.commit()
    return serialize(row)

@app.delete('/api/incidents/{key}', dependencies=[Depends(admin)])
def delete_incident(key: str, db=Depends(database)):
    row = required(db, Incident, key)
    if db.scalar(select(Reservation).where(Reservation.incident_id == key)):
        raise HTTPException(409, 'Incident with reservation history cannot be deleted')
    db.delete(row)
    emit(db, 'incident-deleted', key)
    db.commit()
    return {'deleted': key}

class Transition(BaseModel):
    status: str
    notes: str = Field(default='', max_length=5000)

@app.post('/api/incidents/{key}/transition')
def transition(key: str, body: Transition, db=Depends(database)):
    order = ['Reported', 'Verified', 'Assigned', 'In Progress', 'Resolved']
    row = required(db, Incident, key)
    if row.status == 'Resolved' or body.status != order[order.index(row.status)+1]:
        raise HTTPException(409, 'Invalid lifecycle transition')
    if body.status == 'Assigned':
        raise HTTPException(409, 'Confirm a reservation to assign a center')
    if body.status == 'Resolved' and not body.notes.strip():
        raise HTTPException(422, 'Resolution notes required')
    result = db.execute(update(Incident).where(Incident.id == key, Incident.version == row.version).values(status=body.status, resolution_notes=body.notes, version=row.version+1))
    if result.rowcount != 1:
        raise HTTPException(409, 'Incident changed; refresh and retry')
    emit(db, 'incident-updated', key, body.model_dump())
    db.commit()
    return {'status': body.status}

@app.get('/api/incidents/{key}/recommendations')
def recommendations(key: str, db=Depends(database)):
    incident = required(db, Incident, key)
    ranked = []
    for center in db.scalars(select(Center)):
        health_row = db.get(NodeHealth, center.id)
        online = bool(health_row and health_row.state.get('health') == 'online')
        stock = {i.resource: i.stock-i.reserved for i in db.scalars(select(Inventory).where(Inventory.center_id == center.id))}
        coverage = sum(min(stock.get(k, 0)/v, 1) for k,v in incident.requirements.items())/max(len(incident.requirements), 1)
        a,b = math.radians(incident.latitude),math.radians(center.latitude)
        dlat,dlon = b-a,math.radians(center.longitude-incident.longitude)
        distance = 6371*2*math.asin(min(1,math.sqrt(math.sin(dlat/2)**2+math.cos(a)*math.cos(b)*math.sin(dlon/2)**2)))
        urgency = {'Critical': 4, 'High': 3, 'Medium': 2, 'Low': 1}[incident.severity]
        score = round(coverage*60 + min(center.capacity/max(incident.people_affected,1),1)*15 + 25/(1+distance/(10*urgency)),2) if online else 0
        ranked.append({'center_id': center.id, 'name': center.name, 'score': score, 'distance_km': round(distance,1), 'coverage': coverage, 'online': online, 'capacity': center.capacity, 'reason': f'{coverage:.0%} stock coverage; {distance:.1f} km; urgency {urgency}; '+('node online' if online else 'node unavailable')})
    return sorted(ranked, key=lambda x: -x['score'])

class Reserve(BaseModel):
    incident_id: str
    center_id: Literal['A','B','C']
    quantities: dict[str, int]
    key: str = Field(min_length=8, max_length=100)

def reserve_transaction(body: Reserve):
    quantities(body.quantities)
    with Session() as db:
        existing = db.scalar(select(Reservation).where(Reservation.key == body.key))
        if existing:
            if existing.incident_id != body.incident_id or existing.center_id != body.center_id or existing.quantities != body.quantities:
                raise HTTPException(409, 'Idempotency key belongs to a different request')
            return serialize(existing)
        incident = required(db, Incident, body.incident_id)
        if incident.status != 'Verified':
            raise HTTPException(409, 'Incident must be verified')
        for resource, quantity in sorted(body.quantities.items()):
            result = db.execute(update(Inventory).where(Inventory.center_id == body.center_id, Inventory.resource == resource, Inventory.stock-Inventory.reserved >= quantity).values(reserved=Inventory.reserved+quantity))
            if result.rowcount != 1:
                db.rollback()
                raise HTTPException(409, 'Insufficient available stock')
        row = Reservation(**body.model_dump())
        db.add(row)
        db.flush()
        emit(db, 'allocation-requested', row.id, body.model_dump())
        db.commit()
        return serialize(row)

@app.post('/api/reservations', status_code=201)
async def reserve(body: Reserve):
    async with httpx.AsyncClient(timeout=8) as client:
        try:
            response = await client.post(NODE_URLS[body.center_id] + '/rpc', json={'jsonrpc':'2.0','id':body.key,'method':'reservation.request','params':body.model_dump()})
            response.raise_for_status()
            result = response.json()
        except httpx.HTTPError:
            raise HTTPException(503, 'Relief node unavailable; retry with the same key')
    if 'error' in result:
        raise HTTPException(409, result['error']['message'])
    return result['result']

@app.post('/api/internal/reserve', dependencies=[Depends(admin)])
def internal_reserve(body: Reserve):
    return reserve_transaction(body)

@app.post('/api/reservations/{key}/{action}')
def reservation_action(key: str, action: Literal['confirm','cancel'], db=Depends(database)):
    row = db.scalar(select(Reservation).where(Reservation.id == key).with_for_update())
    if not row:
        raise HTTPException(404, 'Reservation not found')
    target = 'confirmed' if action == 'confirm' else 'cancelled'
    if row.status == target:
        return serialize(row)
    result = db.execute(update(Reservation).where(Reservation.id == key, Reservation.status == 'pending').values(status=target))
    if result.rowcount != 1:
        raise HTTPException(409, 'Reservation already completed')
    incident = required(db, Incident, row.incident_id)
    if action == 'confirm':
        result = db.execute(update(Incident).where(Incident.id == incident.id, Incident.status == 'Verified').values(status='Assigned', center_id=row.center_id, version=Incident.version+1))
        if result.rowcount != 1:
            raise HTTPException(409, 'Incident already assigned')
    for resource, quantity in row.quantities.items():
        values = {'reserved': Inventory.reserved-quantity}
        if action == 'confirm':
            values['stock'] = Inventory.stock-quantity
        db.execute(update(Inventory).where(Inventory.center_id == row.center_id, Inventory.resource == resource).values(**values))
    emit(db, 'allocation-confirmed' if action == 'confirm' else 'allocation-cancelled', key, {'incident_id':row.incident_id})
    emit(db, 'inventory-updated', row.center_id)
    db.commit()
    return serialize(row)

class StockChange(BaseModel):
    delta: int = Field(ge=-100000, le=100000)

@app.post('/api/inventory/{key}', dependencies=[Depends(admin)])
def stock_change(key: str, body: StockChange, db=Depends(database)):
    result = db.execute(update(Inventory).where(Inventory.id == key, Inventory.stock+body.delta >= Inventory.reserved).values(stock=Inventory.stock+body.delta))
    if result.rowcount != 1:
        raise HTTPException(409, 'Stock would be below reservations or item missing')
    emit(db, 'inventory-updated', key, body.model_dump())
    db.commit()
    return serialize(required(db, Inventory, key))

@app.post('/api/nodes/{node}/{action}', dependencies=[Depends(admin)])
async def control(node: Literal['A','B','C'], action: Literal['stop','start','elect']):
    async with httpx.AsyncClient(timeout=8) as client:
        try:
            r = await client.post(NODE_URLS[node]+'/control/'+action, headers={'X-Demo-Key':os.getenv('DEMO_ADMIN_KEY','local-demo-admin')})
            r.raise_for_status()
            return r.json()
        except httpx.HTTPError:
            raise HTTPException(503, 'Node process unreachable; restart its process/container')

@app.get('/api/stream')
async def stream(request: Request, after: int = 0):
    try:
        cursor = max(after, int(request.headers.get('last-event-id', '0')))
    except ValueError:
        raise HTTPException(422, 'Invalid stream cursor')
    async def events():
        nonlocal cursor
        while not await request.is_disconnected():
            with Session() as db:
                # Demo reset starts a new event history; reconnecting clients must replay it.
                latest = db.scalar(select(func.max(Event.clock))) or 0
                if cursor > latest:
                    cursor = 0
                rows = list(db.scalars(select(Event).where(Event.clock > cursor).order_by(Event.clock, Event.id)))
                for row in rows:
                    yield f'id: {row.clock}\nevent: update\ndata: {json.dumps(serialize(row))}\n\n'
                    cursor = row.clock
            yield ': heartbeat\n\n'
            await asyncio.sleep(1)
    return StreamingResponse(events(), media_type='text/event-stream', headers={'Cache-Control':'no-cache','X-Accel-Buffering':'no'})

class Graph(BaseModel):
    graph: dict[str,list[str]]

@app.post('/api/demo/deadlock')
def deadlock(body: Graph):
    return {'cycle': cycle(body.graph), 'graph': body.graph}

@app.post('/api/demo/snapshot')
async def snapshot_demo():
    # Explicit FIFO simulator; sampled HTTP node states are inputs, not an atomic live snapshot.
    async with httpx.AsyncClient(timeout=2) as client:
        states = {}
        for node, url in NODE_URLS.items():
            try:
                r = await client.get(url+'/status')
                r.raise_for_status()
                states[node] = r.json()
            except httpx.HTTPError:
                raise HTTPException(503, 'All three nodes must be reachable for this demonstration')
    snap = Snapshot(list(states))
    trace = []
    snap.states['A'] = states['A']
    snap.marker('A','B',states['B']); trace.append('A marker → B: record B')
    snap.message('C','B',{'type':'inventory-update','quantity':10}); trace.append('C application message → B: record in transit')
    for source in states:
        for target in states:
            if source != target and (source,target) not in snap.closed:
                snap.marker(source,target,states[target])
                trace.append(f'{source} marker → {target}: close channel')
    return {'mode':'Educational reliable FIFO channel simulator with live node state inputs', 'states':snap.states,'channels':snap.channels,'complete':snap.complete,'trace':trace}

@app.post('/api/demo/duplicate', dependencies=[Depends(admin)])
def duplicate(db=Depends(database)):
    event = emit(db, 'inventory-updated', 'duplicate-demo', {'demo':True})
    db.commit()
    if os.getenv('RABBITMQ_URL'):
        import pika
        try:
            connection = pika.BlockingConnection(pika.URLParameters(os.environ['RABBITMQ_URL']))
            channel = connection.channel()
            channel.confirm_delivery()
            for _ in range(2):
                channel.basic_publish('relief.events','',json.dumps(serialize(event)),properties=pika.BasicProperties(delivery_mode=2,message_id=event.id))
            connection.close()
            deadline = time.monotonic()+4
            while time.monotonic()<deadline:
                with Session() as check:
                    if check.get(Receipt,event.id):
                        return {'event_id':event.id,'first_processed':True,'duplicate_processed':False,'transport':'RabbitMQ','published':2}
                time.sleep(.1)
            raise HTTPException(503,'Messages published; worker receipt not yet available')
        except pika.exceptions.AMQPError:
            raise HTTPException(503,'Broker unavailable; the durable outbox will retry this event')
    from .worker import consume
    first, second = consume(serialize(event)), consume(serialize(event))
    return {'event_id':event.id,'first_processed':first,'duplicate_processed':second,'transport':'local consumer only'}

class CenterInput(BaseModel):
    name: str = Field(min_length=3,max_length=150)
    capacity: int = Field(ge=1,le=100000)

@app.put('/api/centers/{key}',dependencies=[Depends(admin)])
def update_center(key: str,body: CenterInput,db=Depends(database)):
    row = required(db,Center,key)
    row.name,row.capacity = body.name,body.capacity
    emit(db,'center-updated',key,body.model_dump())
    db.commit()
    return serialize(row)
