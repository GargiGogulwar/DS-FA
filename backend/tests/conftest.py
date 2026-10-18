import pytest
from sqlalchemy import create_engine
from app.db import Base, Session

@pytest.fixture(autouse=True)
def isolated_database(tmp_path):
    engine = create_engine('sqlite:///'+str(tmp_path/'test.db'),connect_args={'check_same_thread':False,'timeout':30})
    Base.metadata.create_all(engine)
    original = Session.kw['bind']
    Session.configure(bind=engine)
    yield
    Session.configure(bind=original)
    engine.dispose()
