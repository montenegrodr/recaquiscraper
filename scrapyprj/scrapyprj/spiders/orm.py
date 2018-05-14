from sqlalchemy import create_engine, exists
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, TEXT, DateTime, Float, Boolean
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func


Base = declarative_base()


class Business(Base):
    __tablename__ = 'business'

    id          = Column(Integer, primary_key=True)
    name        = Column(String(255))
    store_id    = Column(Integer)
    nb_pages    = Column(Integer)
    created_at  = Column(DateTime)
    processed   = Column(Boolean)
    error       = Column(String(255))


class Page(Base):
    __tablename__ = 'business_pages'

    id          = Column(Integer, primary_key=True)
    page        = Column(Integer)
    processed   = Column(Boolean)
    id_business = Column(Integer)

class Error(Base):
    __tablename__ = 'errors'

    id          = Column(Integer, primary_key=True)
    description = Column(String(255))
    error       = Column((String(4)))
    timeout     = Column(String(4))
    type        = Column(String(255))
    url         = Column(String(255))
    created_at  = Column(DateTime)


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
    store_id        = Column(Integer)


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
            self.eng.dispose()

    def commit(self):
        self.session.commit()

    def insert(self, obj):
        self.session.add(obj)
        return obj

    def _create_eng(self):
        return create_engine(self.conection_string)

    def _create_session(self):
        return sessionmaker(bind=self.eng)()

    def business_max_id(self):
        return self.session.query(func.max(Business.store_id)).scalar()


class DataController(object):
    def __init__(self):
        self.ds = Dataset()

    def __enter__(self):
        self.ds.open_session()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.ds.commit()
        self.ds.close_session()

    def insert(self, obj, commit=True):
        if 'nb_pages' in obj:
            pojo_obj = Business(
                name=obj.get('name'),
                store_id=obj.get('store_id'),
                nb_pages=obj.get('nb_pages'),
                created_at=obj.get('created_at'),
                error=obj.get('error')
            )
        elif 'id_business' in obj:
            pojo_obj = Page(
                page=obj.get('page'),
                processed=obj.get('processed'),
                id_business=obj.get('id_business')
            )
        self.ds.insert(pojo_obj)
        if commit:
            self.ds.commit()
        return pojo_obj

    def insert_error(self, error):
        self.ds.insert(Error(
            description=error.get('description'),
            error=error.get('error'),
            timeout=error.get('timeout'),
            type=error.get('type'),
            url=error.get('url'),
            created_at=error.get('created_at')
        ))
        self.ds.commit()

    def business_max_id(self):
        return self.ds.business_max_id()

    # def insert(self, complaint):
    #     self.ds.insert(Complain(
    #         business        =complaint.get('business'),
    #         location        =complaint.get('location'),
    #         date            =complaint.get('date'),
    #         title           =complaint.get('title'),
    #         complaint_body  =complaint.get('complaint_body'),
    #         final_answer    =complaint.get('final_answer'),
    #         solved          =complaint.get('solved'),
    #         again           =complaint.get('again'),
    #         rate            =complaint.get('rate'),
    #         url             =complaint.get('url'),
    #         created_at      =complaint.get('created_at'),
    #         store_id        =complaint.get('id')
    #     ))
    #     self.ds.commit()