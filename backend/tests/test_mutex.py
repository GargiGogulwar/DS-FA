import asyncio
import pytest
from app import node

@pytest.mark.asyncio
async def test_bully_highest_announces(monkeypatch):
    sent=[]
    async def post(peer,path,data):
        sent.append((peer,path,data));return {'clock':10}
    monkeypatch.setattr(node,'ID','C')
    monkeypatch.setattr(node,'PRIORITY',3)
    monkeypatch.setattr(node,'enabled',True)
    monkeypatch.setattr(node,'coordinator',None)
    monkeypatch.setattr(node,'post',post)
    await node.elect()
    assert node.coordinator=='C'
    assert {p for p,path,data in sent if path=='/coordinator'}=={'A','B'}

@pytest.mark.asyncio
async def test_bully_lower_waits_for_higher(monkeypatch):
    sent=[]
    async def post(peer,path,data):
        sent.append(peer);return {'answer':'OK','clock':12}
    monkeypatch.setattr(node,'enabled',True)
    monkeypatch.setattr(node,'coordinator',None)
    monkeypatch.setattr(node,'post',post)
    await node.elect()
    assert node.coordinator is None
    assert set(sent)=={'B','C'}

@pytest.mark.asyncio
async def test_mutex_deferred_reply_and_release():
    node.enabled=True;node.held=True;node.requesting=(1,'A')
    pending=asyncio.create_task(node.mutex_request({'node':'B','timestamp':2,'clock':2,'token':'test'}))
    await asyncio.sleep(0)
    assert 'test' in node.deferred and not pending.done()
    await node.release()
    assert (await pending)['reply']
    assert not node.deferred

@pytest.mark.asyncio
async def test_mutex_tie_breaking():
    node.enabled=True;node.held=False;node.requesting=(5,'B')
    result=await node.mutex_request({'node':'A','timestamp':5,'clock':5,'token':'tie'})
    assert result['reply']
    await node.release()
