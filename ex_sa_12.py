from datetime import datetime
from sqlalchemy import Column, CheckConstraint, DateTime, ForeignKey, Numeric
from sqlalchemy import Integer, String
from sqlalchemy.orm import relationship

import SQLAlchemy
from SQLAlchemy.ex_sa_05 import Base


class Account(SQLAlchemy.ex_sa_05.Account):
    security = relationship('Securities', secondary='trades')


class Securities(Base):
    __tablename__ = "securities"

    id = Column(Integer, autoincrement=True, primary_key=True)
    symbol = Column(String(10), nullable=False, unique=True)
    price = Column(Numeric(5, 2), default=0.0)

    account = relationship(Account, secondary='trades')

    def __repr__(self):
        return "[Securities: id=%d, symbol=%s, price=%5.2f]" % (self.id, self.symbol, self.price)


class Trades(Base):
    __tablename__ = "trades"
    __table_args__ = (CheckConstraint("trade_type IN ('B', 'S')"),)

    trade_id = Column(Integer, autoincrement=True, primary_key=True)
    trade_dt = Column(DateTime(), default=datetime.now)
    trade_type = Column(String(1), nullable=False)
    quantity = Column(Integer, nullable=False, default=0)
    sid = Column(Integer, ForeignKey('securities.id'))
    aid = Column(Integer, ForeignKey('account.acct_no'))

    account = relationship(Account, backref='trades')
    security = relationship(Securities, backref='trades')

    def __repr__(self):
        return "[Trades: customer=%s %s, trade_dt=%s, trade_type=%s, quantity=%d, security=%s]" % \
               (self.account.customer.first_name, self.account.customer.last_name, self.trade_dt, self.trade_type, \
                self.quantity, self.security.symbol)
