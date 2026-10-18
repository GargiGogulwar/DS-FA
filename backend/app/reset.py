from .db import Base, engine
from .seed import seed

if __name__ == '__main__':
    # Only the explicit demo reset command drops application tables.
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    seed()
