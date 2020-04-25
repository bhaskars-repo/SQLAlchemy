import logging
import time

from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker
from SQLAlchemy.ex_sa_00 import create_db_engine
from SQLAlchemy.ex_sa_09 import Kyc

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


def query_kyc(engine: Engine):
    if engine.dialect.has_table(engine, 'customer') and engine.dialect.has_table(engine, 'account') and \
            engine.dialect.has_table(engine, 'kyc'):
        Session = sessionmaker(bind=engine)

        session = Session()

        recs = session.query(Kyc).all()
        if len(recs) > 0:
            for r in recs:
                logging.info(r)
        else:
            logging.info("Record(s) for kyc *DOES NOT* exist !!!")

        session.close()
    else:
        logging.info("The customer/account/kyc table(s) *DOES NOT* exist !!!")


def query_kyc_order(engine: Engine):
    if engine.dialect.has_table(engine, 'customer') and engine.dialect.has_table(engine, 'account') and \
            engine.dialect.has_table(engine, 'kyc'):
        Session = sessionmaker(bind=engine)

        session = Session()

        recs = session.query(Kyc).order_by(Kyc.kyc_update_dt.desc())
        if recs.count() > 0:
            for r in recs:
                logging.info(r)
        else:
            logging.info("Record(s) for kyc *DOES NOT* exist !!!")

        session.close()
    else:
        logging.info("The customer/account/kyc table(s) *DOES NOT* exist !!!")


def update_kyc(engine: Engine, name: str, flag: bool):
    if engine.dialect.has_table(engine, 'kyc'):
        Session = sessionmaker(bind=engine)

        session = Session()

        rec = session.query(Kyc).filter(Kyc.customer.has(last_name=name)).first()
        if rec:
            rec.kyc_flag = flag
        else:
            logging.info("Record for Customer '%s' *DOES NOT* exist !!!" % name)

        session.commit()

        logging.info("Updated record for Customer '%s'" % name)

        session.close()
    else:
        logging.info("The kyc table *DOES NOT* exist !!!")


if __name__ == "__main__":
    db_engine = create_db_engine()
    query_kyc(db_engine)
    update_kyc(db_engine, 'Driver', True)
    query_kyc(db_engine)
    update_kyc(db_engine, 'Driver', False)
    update_kyc(db_engine, 'Builder', True)
    time.sleep(1)
    update_kyc(db_engine, 'Builder', False)
    query_kyc_order(db_engine)
