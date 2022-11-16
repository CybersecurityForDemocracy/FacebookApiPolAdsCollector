from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# use it with Direct SQL Query
def get_crowdtangle_db_conn():
    # Creating sql engine instance
    engine = create_engine("postgresql://me@localhost/mydb", pool_size=20, max_overflow=0,pool_recycle=3600)
    connection = engine.connect()
    return connection

# Use this with SQL ORM
def get_crowdtangle_db_session():
    # Creating sql engine instance
    engine = create_engine("postgresql://me@localhost/mydb", pool_size=20, max_overflow=0,pool_recycle=3600)
    return sessionmaker(bind = engine)
