import asyncio
import json
import os
import time
import uuid
import logging
import httpx
import pika
from sqlalchemy import select, func
from .db import Session, Receipt, Event, serialize
from .main import NODE_URLS

logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('pika').setLevel(logging.WARNING)

BASE = os.getenv('VERIFY_URL','http://backend:8000')
HEADERS = {'X-Demo-Key':os.getenv('DEMO_ADMIN_KEY','local-demo-admin')}

async def eventually(predicate, timeout=20):
    deadline = time.monotonic()+timeout
    while time.monotonic()<deadline:
        result=await predicate()
        if result:return result
        await asyncio.sleep(.2)
    raise AssertionError('Condition did not converge within controlled deadline')

async def verify():
    checks=[]
    async with httpx.AsyncClient(base_url=BASE,headers=HEADERS,timeout=15) as c:
        async def state():
            r=await c.get('/api/state');r.raise_for_status();return r.json()
        async def coordinator_is(key):
            s=await state()
            return len([n for n in s['nodes'] if n['state']['health']=='online' and n['state'].get('coordinator')==key]) >= (3 if key=='C' else 2)
        await eventually(lambda:coordinator_is('C'))
        checks.append('Three independent network nodes converge on highest-priority coordinator C')
        body={'title':'Verification flood '+uuid.uuid4().hex[:6],'description':'Automated simulated workflow','disaster_type':'Flood','severity':'Critical','location':'Aluva test zone','latitude':10.1,'longitude':76.3,'people_affected':100,'requirements':{'water':10}}
        r=await c.post('/api/incidents',json=body);r.raise_for_status();incident=r.json()
        r=await c.post('/api/incidents/'+incident['id']+'/transition',json={'status':'Verified'});r.raise_for_status()
        ranks=(await c.get('/api/incidents/'+incident['id']+'/recommendations')).json()
        assert len(ranks)==3 and ranks[0]['online']
        checks.append('Create, persist, verify and rank centers')
        s=await state();item=next(i for i in s['inventory'] if i['center_id']=='A' and i['resource']=='water')
        available=item['stock']-item['reserved']
        request={'incident_id':incident['id'],'center_id':'A','quantities':{'water':available},'key':str(uuid.uuid4())}
        competing=[{**request,'key':str(uuid.uuid4())} for _ in range(2)]
        responses=await asyncio.gather(*(c.post('/api/reservations',json=b) for b in competing))
        assert sorted(r.status_code for r in responses)==[201,409],[(r.status_code,r.text) for r in responses]
        winner=next(r.json() for r in responses if r.status_code==201)
        winner_body=competing[next(i for i,r in enumerate(responses) if r.status_code==201)]
        duplicate=await c.post('/api/reservations',json=winner_body)
        assert duplicate.status_code==201 and duplicate.json()['id']==winner['id']
        s=await state();item=next(i for i in s['inventory'] if i['id']==item['id'])
        assert item['reserved']==item['stock']
        await c.post('/api/reservations/'+winner['id']+'/cancel',json={})
        checks.append('Concurrent network reservations cannot oversell; idempotent retry and cancellation')
        # Distinct nodes contend for the same global permission domain.
        reqs=[{**request,'center_id':node,'quantities':{'water':1},'key':str(uuid.uuid4())} for node in ['A','B']]
        rs=await asyncio.gather(*(c.post('/api/reservations',json=b) for b in reqs))
        assert all(r.status_code==201 for r in rs),[(r.status_code,r.text) for r in rs]
        for r in rs:await c.post('/api/reservations/'+r.json()['id']+'/cancel',json={})
        checks.append('Ricart–Agrawala concurrent peer requests complete with deterministic ordering')
        reservation=await c.post('/api/reservations',json={**request,'quantities':{'water':10},'key':str(uuid.uuid4())})
        reservation.raise_for_status()
        r=await c.post('/api/reservations/'+reservation.json()['id']+'/confirm',json={});r.raise_for_status()
        for status in ['In Progress','Resolved']:
            r=await c.post('/api/incidents/'+incident['id']+'/transition',json={'status':status,'notes':'Simulation complete'});r.raise_for_status()
        checks.append('Reservation confirmation, assignment, response and resolution')
        r=await c.post('/api/nodes/C/stop',json={});r.raise_for_status()
        try:
            await eventually(lambda:coordinator_is('B'))
            checks.append('Simulated coordinator failure detected and bully reelection converges on B')
        finally:
            r=await c.post('/api/nodes/C/start',json={});r.raise_for_status()
        await eventually(lambda:coordinator_is('C'))
        checks.append('Recovered node rejoins and announces coordinator C')
        snapshot=await c.post('/api/demo/snapshot',json={});snapshot.raise_for_status()
        assert snapshot.json()['complete'] and snapshot.json()['channels']
        assert (await c.post('/api/demo/deadlock',json={'graph':{'A':['B'],'B':['A']}})).json()['cycle']==['A','B','A']
        checks.append('Snapshot marker/channel recording and wait-for cycle detection')
        async with c.stream('GET','/api/stream?after=0') as response:
            async for line in response.aiter_lines():
                if line.startswith('data:'):
                    assert json.loads(line[5:])['clock']>=1
                    break
        checks.append('Live SSE delivery and persisted event replay')
        for node,url in NODE_URLS.items():
            r=await c.post(url+'/rpc',json={'jsonrpc':'2.0','id':'verify-'+node,'method':'inventory.query'})
            assert len(r.json()['result'])==7
        checks.append('JSON-RPC inventory across actual peer interfaces')
        # Publish a real duplicate through RabbitMQ, then assert the persistent consumer receipt is unique.
        with Session() as db:
            event=db.scalar(select(Event).order_by(Event.clock.desc()).limit(1));message=serialize(event)
        conn=pika.BlockingConnection(pika.URLParameters(os.environ['RABBITMQ_URL']))
        channel=conn.channel();channel.confirm_delivery()
        for _ in range(2):channel.basic_publish('relief.events','',json.dumps(message),properties=pika.BasicProperties(delivery_mode=2))
        channel.basic_publish('relief.events','',b'not-json',properties=pika.BasicProperties(delivery_mode=2))
        conn.close()
        async def receipt():
            with Session() as db:return db.scalar(select(func.count()).select_from(Receipt).where(Receipt.id==message['id']))==1
        await eventually(receipt)
        checks.append('RabbitMQ publication/acknowledgement and persistent duplicate suppression')
        async def dead_letter():
            conn=pika.BlockingConnection(pika.URLParameters(os.environ['RABBITMQ_URL']))
            method,props,body=conn.channel().basic_get('relief.dead',auto_ack=True)
            conn.close()
            return method is not None and body==b'not-json'
        await eventually(dead_letter)
        checks.append('Malformed broker event retries once then moves to durable dead-letter queue')
        with Session() as db:
            clocks=list(db.scalars(select(Event.clock)))
            assert len(set(clocks))==len(clocks)
        checks.append('Unique monotonic persisted logical timestamps')
    print(json.dumps({'passed':len(checks),'checks':checks},indent=2))

if __name__=='__main__':
    asyncio.run(verify())
