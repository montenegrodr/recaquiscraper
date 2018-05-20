from sqlalchemy import create_engine, exists
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, TEXT, DateTime, Float, Boolean
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func


Base = declarative_base()


class NoPageToProcessException(Exception):
    pass


class Business(Base):
    __tablename__ = 'business'

    id          = Column(Integer, primary_key=True)
    name        = Column(String(255))
    store_id    = Column(Integer)
    nb_pages    = Column(Integer)
    created_at  = Column(DateTime)
    processed   = Column(Boolean)
    error       = Column(String(1025))


class Page(Base):
    __tablename__ = 'business_page'

    id          = Column(Integer, primary_key=True)
    page        = Column(Integer)
    processed   = Column(Boolean)
    id_business = Column(Integer)
    locked      = Column(Boolean)
    error       = Column(String(1025))


class ComplaintPage(Base):
    __tablename__ = 'complaint_page'

    id          = Column(Integer, primary_key=True)
    id_business = Column(Integer)
    id_page     = Column(Integer)
    url         = Column(String(255))
    processed   = Column(Boolean)
    locked      = Column(Boolean)
    error       = Column(String(1025))


class Error(Base):
    __tablename__ = 'errors'

    id          = Column(Integer, primary_key=True)
    description = Column(String(255))
    error       = Column((String(4)))
    timeout     = Column(String(4))
    type        = Column(String(255))
    url         = Column(String(255))
    created_at  = Column(DateTime)


class Complaint(Base):
    __tablename__ = 'complaint'

    id                = Column(Integer, primary_key=True)
    business          = Column(String(255))
    location          = Column(String(255))
    date              = Column(String(255))
    title             = Column(String(800))
    complaint_body    = Column(TEXT)
    final_answer      = Column(TEXT)
    solved            = Column(String(20))
    again             = Column(String(5))
    rate              = Column(String(3))
    url               = Column(String(255))
    created_at        = Column(DateTime)
    store_id          = Column(Integer)
    complaint_page_id = Column(Integer)


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

    def next_page(self):
        return self.session.query(Page).filter(Page.processed == False, Page.locked == False).first()

    def next_complaint(self):
        return self.session.query(ComplaintPage).filter(ComplaintPage.processed == False, ComplaintPage.locked == False).first()

    def get_page(self, id):
        return self.session.query(Page).filter(Page.id == id).one()

    def get_complaint_page(self, id):
        return self.session.query(ComplaintPage).filter(ComplaintPage.id == id).one()


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
        elif 'complaint_body' in obj:
            pojo_obj = Complaint(
                business          =obj.get('business'),
                location          =obj.get('location'),
                date              =obj.get('date'),
                title             =obj.get('title'),
                complaint_body    =obj.get('complaint_body'),
                final_answer      =obj.get('final_answer'),
                solved            =obj.get('solved'),
                again             =obj.get('again'),
                rate              =obj.get('rate'),
                url               =obj.get('url'),
                created_at        =obj.get('created_at'),
                store_id          =obj.get('id'),
                complaint_page_id =obj.get('complaint_page_id')
                )
        elif 'url' in obj:
            pojo_obj = ComplaintPage(
                url=obj.get('url'),
                id_business=obj.get('id_business'),
                id_page=obj.get('id_page'),
                processed=False,
                locked=False
            )
        elif 'id_business' in obj:
            pojo_obj = Page(
                page=obj.get('page'),
                processed=obj.get('processed'),
                id_business=obj.get('id_business'),
                locked=obj.get('locked')
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

    def next_page(self):
        page = self.ds.next_page()
        if not page:
            raise NoPageToProcessException()
        page.locked = True
        self.ds.commit()
        return page

    def unlock(self, page, err):
        page.locked = False
        page.error = err

    def get_page(self, id):
        return self.ds.get_page(id)

    def next_complaint(self):
        complaint = self.ds.next_complaint()
        if not complaint:
            raise NoPageToProcessException()
        complaint.locked = True
        self.ds.commit()
        return complaint

    def get_complaint_page(self, id):
        return self.ds.get_complaint_page(id)

    def commit(self):
        self.ds.commit()