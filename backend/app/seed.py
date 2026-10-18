from sqlalchemy import select
from .db import *

def seed():
    with Session() as db:
        if db.scalar(select(Center)):
            return
        for key,name,lat,lon,capacity in [('A','Kochi Coastal Response',9.97,76.28,1200),('B','Thrissur Relief Hub',10.52,76.21,900),('C','Alappuzha Rescue Center',9.49,76.34,1100)]:
            db.add(Center(id=key,name=name,latitude=lat,longitude=lon,capacity=capacity))
            db.flush()
            for resource,stock in [('water',2500),('food_kits',1600),('medical_kits',450),('ambulances',12),('rescue_boats',18),('rescue_teams',24),('shelter_supplies',800)]:
                db.add(Inventory(center_id=key,resource=resource,stock=stock))
        for n,(title,severity,kind,lat,lon,people) in enumerate([('Periyar river overflow','Critical','Flood',10.11,76.35,850),('Coastal evacuation','High','Cyclone',9.59,76.33,430),('School shelter support','Medium','Flood',10.45,76.24,180),('Hillside road obstruction','Low','Landslide',10.2,76.5,40)]):
            db.add(Incident(id=f'demo-{n+1}',title=title,description='Simulated training incident. No real emergency dispatch.',severity=severity,disaster_type=kind,location=['Aluva','Alappuzha coast','Thrissur','Eastern hills'][n],latitude=lat,longitude=lon,people_affected=people,requirements={'water':200,'food_kits':100,'medical_kits':20},status='Verified' if n < 3 else 'Reported'))
        db.flush()
        db.add(Reservation(incident_id='demo-2',center_id='C',quantities={'water':100},key='seed-pending-allocation'))
        item = db.scalar(select(Inventory).where(Inventory.center_id=='C',Inventory.resource=='water'))
        item.reserved = 100
        db.add(Event(kind='demo-seeded',aggregate_id='system',payload={'simulated':True},clock=1))
        db.commit()

if __name__ == '__main__':
    seed()
