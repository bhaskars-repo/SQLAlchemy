from datetime import datetime
from sqlalchemy import Column, ForeignKey, DateTime
from sqlalchemy import Integer, Boolean
from sqlalchemy.orm import relationship
from SQLAlchemy.ex_sa_05 import Base


class Kyc(Base):
    __tablename__ = "kyc"

    kyc_id = Column(Integer, autoincrement=True, primary_key=True)
    kyc_flag = Column(Boolean, default=False)
    kyc_update_dt = Column(DateTime(), default=datetime.now, onupdate=datetime.now)
    cid = Column(Integer, ForeignKey('customer.id'))
    ano = Column(Integer, ForeignKey('account.acct_no'))

    customer = relationship("Customer", uselist=False)

    account = relationship("Account", uselist=False)

    def __repr__(self):
        return "[Kyc: kyc_flag=%s, kyc_update_dt=%s, customer=%s, account=%s]" % \
               (self.kyc_flag, self.kyc_update_dt, self.customer.last_name, self.account.acct_name)
