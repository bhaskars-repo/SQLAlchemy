import logging

from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from SQLAlchemy.ex_sa_00 import create_db_engine
from SQLAlchemy.ex_sa_05 import Base, Account

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


def create_account_table(engine: Engine) -> bool:
    status = False

    if not engine.dialect.has_table(engine, 'account'):
        Base.metadata.create_all(db_engine)

        logging.info("Created the account table !!!")

        status = True
    else:
        logging.info("The account table already exists !!!")

    return status


def insert_account_recs(engine: Engine):
    if engine.dialect.has_table(engine, 'account'):
        Session = sessionmaker(bind=engine)

        session = Session()

        try:
            ad_acct = Account(acct_no=1001, acct_name='Alice Trade Account', cust_id=1)
            session.add(ad_acct)
            session.commit()

            logging.info("Inserted account for Alice")
        except SQLAlchemyError as e:
            logging.error(e)

        try:
            bb_acct = Account(acct_no=1002, acct_name='Bob Credit Account', cust_id=2)
            session.add(bb_acct)
            session.commit()

            logging.info("Inserted account for Bob")
        except SQLAlchemyError as e:
            logging.error(e)

        try:
            cd_acct = Account(acct_no=1003, acct_name='Charlie Trade Account', cust_id=3)
            session.add(cd_acct)
            session.commit()

            logging.info("Inserted account for Charlie")
        except SQLAlchemyError as e:
            logging.error(e)

        session.close()
    else:
        logging.info("The account table *DOES NOT* exists !!!")


if __name__ == "__main__":
    db_engine = create_db_engine()
    if create_account_table(db_engine):
        insert_account_recs(db_engine)
