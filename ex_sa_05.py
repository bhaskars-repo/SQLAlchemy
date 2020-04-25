from datetime import datetime
from sqlalchemy import Column, DateTime, ForeignKey
from sqlalchemy import Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class Customer(Base):
    __tablename__ = "customer"

    id = Column(Integer, autoincrement=True, primary_key=True)
    first_name = Column(String(25), nullable=False)
    last_name = Column(String(25), nullable=False, index=True)
    email = Column(String(50))
    mobile = Column(String(10))

    def __repr__(self):
        return "[Customer: id=%d, first_name=%s, last_name=%s, email=%s]" % \
               (self.id, self.first_name, self.last_name, self.email)


class Account(Base):
    __tablename__ = "account"

    acct_no = Column(Integer, primary_key=True)
    acct_name = Column(String(50), nullable=False)
    acct_open_dt = Column(DateTime(), default=datetime.now)
    acct_update_dt = Column(DateTime(), default=datetime.now, onupdate=datetime.now)
    cust_id = Column(Integer, ForeignKey('customer.id'))

    customer = relationship("Customer", backref='accounts')

    def __repr__(self):
        return "[Account: acct_no=%d, acct_name=%s, acct_open_dt=%s, acct_update_dt=%s, customer=%s]" % \
               (self.acct_no, self.acct_name, self.acct_open_dt, self.acct_update_dt, self.customer.last_name)
