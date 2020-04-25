import logging

from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from SQLAlchemy.ex_sa_00 import create_db_engine
from SQLAlchemy.ex_sa_05 import Customer, Account

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


def create_dummy_account(engine: Engine):
    if engine.dialect.has_table(engine, 'customer') and engine.dialect.has_table(engine, 'account'):
        Session = sessionmaker(bind=engine)

        session = Session()

        dummy_cust = Customer(first_name='Dummy', last_name='Joker', email='djoker@losers.io')

        try:
            session.add(dummy_cust)
            session.commit()

            logging.info("Inserted record for Dummy customer: %s" % dummy_cust)
        except SQLAlchemyError as e:
            logging.error(e)

        try:
            dummy_acct = Account(acct_no=9999, acct_name='Dummy Coin Account', cust_id=dummy_cust.id)
            session.add(dummy_acct)

            session.commit()

            logging.info("Inserted record for Dummy account: %s" % dummy_acct)
        except SQLAlchemyError as e:
            logging.error(e)

        session.close()
    else:
        logging.info("The customer and/or account table(s) *DOES NOT* exist !!!")


def query_dummy_account(engine: Engine):
    if engine.dialect.has_table(engine, 'account'):
        Session = sessionmaker(bind=engine)

        session = Session()

        recs = session.query(Account).filter(Account.acct_no == 9999)
        if recs.count() == 1:
            for r in recs:
                logging.info(r)
        else:
            logging.info("Record for Dummy account *DOES NOT* exist !!!")

        session.close()
    else:
        logging.info("The account table *DOES NOT* exist !!!")


def update_dummy_account(engine: Engine):
    if engine.dialect.has_table(engine, 'account'):
        Session = sessionmaker(bind=engine)

        session = Session()

        rec = session.query(Account).filter(Account.acct_no == 9999).first()
        if rec:
            rec.acct_name = 'Dummy Crypto Account'
        else:
            logging.info("Record for Dummy account *DOES NOT* exist !!!")

        session.commit()

        logging.info("Updated record for Dummy account")

        session.close()
    else:
        logging.info("The account table *DOES NOT* exist !!!")


def delete_dummy_account(engine: Engine):
    if engine.dialect.has_table(engine, 'account'):
        Session = sessionmaker(bind=engine)

        session = Session()

        session.query(Account).filter(Account.acct_no == 9999).delete()

        session.commit()

        logging.info("Deleted record for Dummy account")

        session.query(Customer).filter(Customer.last_name == 'Joker').delete()

        session.commit()

        logging.info("Deleted record for Dummy customer")

        session.close()
    else:
        logging.info("The account table *DOES NOT* exist !!!")


if __name__ == "__main__":
    db_engine = create_db_engine()
    create_dummy_account(db_engine)
    query_dummy_account(db_engine)
    update_dummy_account(db_engine)
    query_dummy_account(db_engine)
    delete_dummy_account(db_engine)
    query_dummy_account(db_engine)
