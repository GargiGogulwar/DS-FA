import asyncio
import sys
from sqlalchemy import select
from .verify import BASE, HEADERS, eventually
from .db import Session, Event, Receipt
import httpx

async def run(mode):
    async with httpx.AsyncClient(base_url=BASE,headers=HEADERS,timeout=5) as client:
        if mode in ['node-down','node-up']:
            async def matches():
                state=(await client.get('/api/state')).json()
                nodes={n['id']:n['state'] for n in state['nodes']}
                if mode=='node-down':
                    return nodes.get('C',{}).get('health')=='unreachable' and all(nodes.get(n,{}).get('coordinator')=='B' for n in ['A','B'])
                return len(nodes)==3 and all(n.get('health')=='online' and n.get('coordinator')=='C' for n in nodes.values())
            await eventually(matches,30)
        elif mode=='outage-write':
            r=await client.post('/api/incidents',json={'title':'Broker outage recovery probe','description':'Automated simulation','disaster_type':'Flood','severity':'Low','location':'Test region','latitude':10,'longitude':76,'people_affected':1,'requirements':{'water':1}})
            r.raise_for_status()
            with Session() as db:
                event=db.scalar(select(Event).where(Event.aggregate_id==r.json()['id']))
                assert event and event.published==0
        elif mode=='reconcile':
            async def drained():
                with Session() as db:
                    pending=db.scalar(select(Event).where(Event.published==0))
                    last=db.scalar(select(Event).order_by(Event.clock.desc()).limit(1))
                    return not pending and last and db.get(Receipt,last.id) is not None
            await eventually(drained,40)
        else:
            raise ValueError('Unknown fault probe')
    print(mode+': PASS')

if __name__=='__main__':
    asyncio.run(run(sys.argv[1]))
