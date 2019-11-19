from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
import enum

Base = declarative_base()

class DataObjectInfo(Base):
    # TODO: add index constraints, namespace + path should be unique
    __tablename__ = 'data_object_info'

    id                  = Column(Integer, primary_key=True, autoincrement=True)
    namespace_name      = Column(String)
    path                = Column(String)
    format              = Column(String)
    timestamp           = Column(DateTime)

    def __repr__(self):
        return "DataObject(id={}, namespace={}, path={}, timestamp={})".format(self.id, self.namespace, self.path, self.timestamp)

class DataRegistry(object):
    def __init__(self, db_path):
        self.engine = create_engine('sqlite:///{}'.format(db_path))
        self.Session = sessionmaker(bind=self.engine)
    
    @contextmanager
    def session_scope(self):
        session = self.Session()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    def create_db(self):
        Base.metadata.create_all(self.engine)

    def create_data_object_info(self, namespace_name, path, format):
        with self.session_scope() as session: 
            do = DataObject(namespace_name=namespace_name, path=path, format=format)
            session.add(do)

    def get_data_object_info(self, namespace_name, path):
        with self.session_scope() as session: 
            r = session.query(DataObjectInfo).filter(DataObjectInfo.namespace_name==namespace_name).filter(DataObjectInfo.path==path)
            asse
            if len(r) == 0:
                return None
            assert(len(r) == 1)
            return r[0]
