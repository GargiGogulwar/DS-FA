from concurrent.futures import ThreadPoolExecutor
import asyncio
import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from sqlalchemy import select
from app.main import app, Reserve, reserve_transaction, recommendations, stream
from app.db import Session, Incident, Inventory, NodeHealth, Event, serialize
from app.seed import seed
from app.algorithms import Lamport, Vector, Snapshot, cycle
from app.worker import consume

@pytest.fixture
def client():
    seed()
    return TestClient(app)

def test_lamport():
    c = Lamport()
    assert c.tick() == 1
    assert c.tick(10) == 11
    assert c.tick(2) == 12

def test_vectors():
    a,b = Vector(),Vector()
    a.tick('A'); b.tick('B')
    assert a.relation(b.values) == 'concurrent'
    b.tick('B',a.values)
    assert a.relation(b.values) == 'before'
    assert b.relation(a.values) == 'after'
    assert a.relation(a.values) == 'equal'

@pytest.mark.parametrize('graph,expected',[({'A':['B'],'B':['A']},['A','B','A']),({'A':['B'],'B':[]},[]),({'A':['A']},['A','A']),({},[])])
def test_deadlock(graph,expected):
    assert cycle(graph) == expected

def test_snapshot():
    s = Snapshot(['A','B','C'])
    assert s.marker('A','B',{'stock':10})
    s.message('C','B',{'quantity':4})
    s.marker('C','B',{'stock':20})
    s.message('C','B',{'quantity':99})
    assert s.channels['C->B'] == [{'quantity':4}]
    assert s.states['B'] == {'stock':10}
    assert not s.complete
    for a in s.nodes:
        for b in s.nodes:
            if a!=b:s.marker(a,b,{})
    assert s.complete

def incident_body():
    return {'title':'Critical flood test','description':'Simulated','disaster_type':'Flood','severity':'Critical','location':'Aluva','latitude':10.1,'longitude':76.3,'people_affected':100,'requirements':{'water':20}}

@pytest.mark.parametrize('field,value',[('latitude',91),('longitude',181),('people_affected',-1),('requirements',{'water':-1}),('requirements',{'unknown':10}),('requirements',{})])
def test_validation(client,field,value):
    body=incident_body();body[field]=value
    assert client.post('/api/incidents',json=body).status_code == 422

def test_lifecycle_persistence(client):
    response=client.post('/api/incidents',json=incident_body())
    assert response.status_code==201
    key=response.json()['id']
    assert client.get('/api/incidents/'+key).json()['title']=='Critical flood test'
    assert client.post('/api/incidents/'+key+'/transition',json={'status':'Resolved'}).status_code==409
    assert client.post('/api/incidents/'+key+'/transition',json={'status':'Verified'}).status_code==200
    r=reserve_transaction(Reserve(incident_id=key,center_id='A',quantities={'water':20},key='lifecycle-test'))
    assert client.post('/api/reservations/'+r['id']+'/confirm',json={}).status_code==200
    assert client.post('/api/reservations/'+r['id']+'/confirm',json={}).status_code==200
    assert client.post('/api/incidents/'+key+'/transition',json={'status':'In Progress'}).status_code==200
    assert client.post('/api/incidents/'+key+'/transition',json={'status':'Resolved'}).status_code==422
    assert client.post('/api/incidents/'+key+'/transition',json={'status':'Resolved','notes':'Training completed'}).status_code==200
    assert client.get('/api/incidents/'+key).json()['status']=='Resolved'
    assert client.get('/health').headers.get('x-correlation-id')

def test_edit_delete_permissions(client):
    r=client.post('/api/incidents',json=incident_body()).json();key=r['id']
    body=incident_body();body['title']='Edited flood'
    assert client.put('/api/incidents/'+key,json=body).status_code==200
    assert client.delete('/api/incidents/'+key).status_code==403
    assert client.delete('/api/incidents/'+key,headers={'X-Demo-Key':'local-demo-admin'}).status_code==200
    assert client.get('/api/incidents/'+key).status_code==404

def test_concurrent_reservations(client):
    with Session() as db:
        row=db.scalar(select(Inventory).where(Inventory.center_id=='A',Inventory.resource=='water'));row.stock=100;db.commit()
    def attempt(n):
        try:return reserve_transaction(Reserve(incident_id='demo-1',center_id='A',quantities={'water':80},key=f'concurrent-{n}'))
        except HTTPException as exc:return exc.status_code
    with ThreadPoolExecutor(max_workers=4) as pool:results=list(pool.map(attempt,range(4)))
    assert sum(isinstance(r,dict) for r in results)==1
    assert results.count(409)==3
    with Session() as db:
        row=db.scalar(select(Inventory).where(Inventory.center_id=='A',Inventory.resource=='water'))
        assert row.reserved==80 and row.stock==100

def test_idempotency_and_cancel(client):
    body=Reserve(incident_id='demo-1',center_id='A',quantities={'water':25},key='unique-idempotency')
    first=reserve_transaction(body);second=reserve_transaction(body)
    assert first['id']==second['id']
    with pytest.raises(HTTPException):reserve_transaction(body.model_copy(update={'quantities':{'water':26}}))
    assert client.post('/api/reservations/'+first['id']+'/cancel',json={}).status_code==200
    assert client.post('/api/reservations/'+first['id']+'/confirm',json={}).status_code==409
    with Session() as db:assert db.scalar(select(Inventory).where(Inventory.center_id=='A',Inventory.resource=='water')).reserved==0

def test_ranking_health_and_coverage(client):
    with Session() as db:
        db.add(NodeHealth(id='A',state={'health':'online'}));db.commit()
        ranked=recommendations('demo-1',db)
        assert ranked[0]['center_id']=='A' and ranked[0]['coverage']==1
        assert ranked[-1]['score']==0

def test_inventory_constraints(client):
    state=client.get('/api/state').json();item=state['inventory'][0]
    assert client.post('/api/inventory/'+item['id'],json={'delta':10}).status_code==403
    assert client.post('/api/inventory/'+item['id'],json={'delta':-100000},headers={'X-Demo-Key':'local-demo-admin'}).status_code==409

def test_consumer_duplicate(client):
    with Session() as db:e=db.scalar(select(Event));message=serialize(e)
    assert consume(message)
    assert not consume(message)

@pytest.mark.asyncio
@pytest.mark.parametrize('last_id',['0','999999'])
async def test_sse_replay(client,last_id):
    class Request:
        headers={'last-event-id':last_id}
        async def is_disconnected(self):return False
    response=await stream(Request(),0)
    generator=response.body_iterator
    first=await anext(generator)
    assert 'event: update' in first and 'demo-seeded' in first and 'id: 1' in first
    await generator.aclose()

def test_rpc_validation():
    from app.node import app as node_app
    c=TestClient(node_app)
    assert c.post('/rpc',content='bad').json()['error']['code']==-32700
    assert c.post('/rpc',json={}).json()['error']['code']==-32600
    r=c.post('/rpc',json={'jsonrpc':'2.0','id':7,'method':'node.status'}).json()
    assert r['id']==7 and r['result']['id']=='A'
    assert c.post('/rpc',json={'jsonrpc':'2.0','id':8,'method':'unknown'}).json()['error']['code']==-32601
