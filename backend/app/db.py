import os
import uuid
from datetime import datetime, timezone
from sqlalchemy import create_engine, String, Integer, Float, JSON, DateTime, CheckConstraint, UniqueConstraint, ForeignKey
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

def now():
    return datetime.now(timezone.utc)

def uid():
    return str(uuid.uuid4())

engine = create_engine(os.getenv('DATABASE_URL', 'sqlite:///./reliefnet.db'), connect_args={'check_same_thread': False, 'timeout': 30} if os.getenv('DATABASE_URL', 'sqlite').startswith('sqlite') else {}, pool_pre_ping=True)
Session = sessionmaker(engine, expire_on_commit=False)

class Base(DeclarativeBase):
    pass

class Incident(Base):
    __tablename__ = 'incidents'
    __table_args__ = (CheckConstraint('people_affected >= 0 AND latitude >= -90 AND latitude <= 90 AND longitude >= -180 AND longitude <= 180'),)
    id: Mapped[str] = mapped_column(String, primary_key=True, default=uid)
    title: Mapped[str]
    description: Mapped[str]
    disaster_type: Mapped[str]
    severity: Mapped[str]
    location: Mapped[str]
    latitude: Mapped[float]
    longitude: Mapped[float]
    people_affected: Mapped[int]
    requirements: Mapped[dict] = mapped_column(JSON)
    status: Mapped[str] = mapped_column(default='Reported')
    center_id: Mapped[str | None] = mapped_column(ForeignKey('centers.id'))
    resolution_notes: Mapped[str] = mapped_column(default='')
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)
    version: Mapped[int] = mapped_column(default=1)

class Center(Base):
    __tablename__ = 'centers'
    id: Mapped[str] = mapped_column(primary_key=True)
    name: Mapped[str]
    latitude: Mapped[float]
    longitude: Mapped[float]
    capacity: Mapped[int]

class Inventory(Base):
    __tablename__ = 'inventory'
    __table_args__ = (UniqueConstraint('center_id', 'resource'), CheckConstraint('stock >= 0 AND reserved >= 0 AND reserved <= stock'))
    id: Mapped[str] = mapped_column(primary_key=True, default=uid)
    center_id: Mapped[str] = mapped_column(ForeignKey('centers.id'))
    resource: Mapped[str]
    stock: Mapped[int]
    reserved: Mapped[int] = mapped_column(default=0)

class Reservation(Base):
    __tablename__ = 'reservations'
    id: Mapped[str] = mapped_column(primary_key=True, default=uid)
    incident_id: Mapped[str] = mapped_column(ForeignKey('incidents.id'))
    center_id: Mapped[str] = mapped_column(ForeignKey('centers.id'))
    quantities: Mapped[dict] = mapped_column(JSON)
    status: Mapped[str] = mapped_column(default='pending')
    key: Mapped[str] = mapped_column(unique=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)

class Event(Base):
    __tablename__ = 'events'
    id: Mapped[str] = mapped_column(primary_key=True, default=uid)
    kind: Mapped[str]
    aggregate_id: Mapped[str]
    payload: Mapped[dict] = mapped_column(JSON)
    clock: Mapped[int] = mapped_column(unique=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)
    published: Mapped[int] = mapped_column(default=0)

class Receipt(Base):
    __tablename__ = 'receipts'
    id: Mapped[str] = mapped_column(primary_key=True)
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)

class NodeHealth(Base):
    __tablename__ = 'node_health'
    id: Mapped[str] = mapped_column(primary_key=True)
    state: Mapped[dict] = mapped_column(JSON)
    last_seen: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)

def serialize(row):
    return {c.name: getattr(row, c.name).isoformat() if isinstance(getattr(row, c.name), datetime) else getattr(row, c.name) for c in row.__table__.columns}
