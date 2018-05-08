from sqlalchemy import create_engine, exists
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, TEXT, DateTime, Float
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class Error(Base):
    __tablename__ = 'errors'

    id          = Column(Integer, primary_key=True)
    description = Column(String(255))
    error       = Column((String(4)))
    timeout     = Column(String(4))
    type        = Column(String(255))
    url         = Column(String(255))


class Complain(Base):
    __tablename__ = 'complaint'

    id              = Column(Integer, primary_key=True)
    business        = Column(String(255))
    location        = Column(String(255))
    date            = Column(String(255))
    title           = Column(String(800))
    complaint_body  = Column(TEXT)
    final_answer    = Column(TEXT)
    solved          = Column(String(20))
    again           = Column(String(5))
    rate            = Column(String(3))
    url             = Column(String(255))
    created_at      = Column(DateTime)


class Dataset(object):
    def __init__(self, **kwargs):
        self.conection_string = 'mysql://root:root@localhost/reclameaqui'
        self.eng      = self._create_eng()
        self.session = None

        Base.metadata.bind = self.eng
        Base.metadata.create_all()

    def open_session(self):
        self.session = self._create_session()

    def close_session(self):
        if self.session:
            self.session.close()

    def commit(self):
        self.session.commit()

    def insert(self, obj):
        self.session.add(obj)

    def _create_eng(self):
        return create_engine(self.conection_string)

    def _create_session(self):
        return sessionmaker(bind=self.eng)()


class DataController(object):
    def __init__(self):
        self.ds = Dataset()

    def __enter__(self):
        self.ds.open_session()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.ds.close_session()

    def insert_error(self, error):
        self.ds.insert(Error(
            description=error.get('description'),
            error=error.get('error'),
            timeout=error.get('timeout'),
            type=error.get('type'),
            url=error.get('url'),
        ))
        self.ds.commit()

    def insert(self, complaint):
        self.ds.insert(Complain(
            business        =complaint.get('business'),
            location        =complaint.get('location'),
            date            =complaint.get('date'),
            title           =complaint.get('title'),
            complaint_body  =complaint.get('complaint_body'),
            final_answer    =complaint.get('final_answer'),
            solved          =complaint.get('solved'),
            again           =complaint.get('again'),
            rate            =complaint.get('rate'),
            url             =complaint.get('url'),
            created_at      =complaint.get('created_at'),
        ))
        self.ds.commit()