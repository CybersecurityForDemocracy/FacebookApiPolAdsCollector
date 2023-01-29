from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# use it with Direct SQL Query
def get_crowdtangle_db_conn(config):
    # Creating sql engine instance
    uri = "postgresql://{}:{}@{}/{}".format(config['USER'],config['PASSWORD'],config['HOST'],config['DBNAME'])
    engine = create_engine(uri, pool_size=int(config['POOLSIZE']), max_overflow=0,pool_recycle=int(config['POOL_RECYCLE']))
    connection = engine.connect()
    return connection

# Use this with SQL ORM
def get_crowdtangle_db_session(config):
    # Creating sql engine instance
    uri = "postgresql://{}:{}@{}/{}".format(config['USER'],str(config['PASSWORD']),config['HOST'],config['DBNAME'])
    engine = create_engine(uri, pool_size=int(config['POOLSIZE']), max_overflow=0,pool_recycle=int(config['POOL_RECYCLE']))
    return sessionmaker(bind = engine)


    