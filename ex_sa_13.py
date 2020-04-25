import logging

from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from SQLAlchemy.ex_sa_00 import create_db_engine
from SQLAlchemy.ex_sa_05 import Customer
from SQLAlchemy.ex_sa_12 import Base, Account, Securities, Trades

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


def create_trades_table(engine: Engine) -> bool:
    status = False

    if not engine.dialect.has_table(engine, 'trades'):
        Base.metadata.create_all(db_engine)

        logging.info("Created the trades table !!!")

        status = True
    else:
        logging.info("The trades table already exists !!!")

    return status


def insert_trades(engine: Engine):
    if engine.dialect.has_table(engine, 'trades'):
        Session = sessionmaker(bind=engine)

        session = Session()

        try:
            t1 = Trades(trade_type='B', quantity=100, aid=1001, sid=1)
            t2 = Trades(trade_type='B', quantity=300, aid=1001, sid=3)
            t3 = Trades(trade_type='B', quantity=50, aid=1003, sid=1)
            t4 = Trades(trade_type='B', quantity=150, aid=1003, sid=2)
            t5 = Trades(trade_type='S', quantity=100, aid=1001, sid=3)
            t6 = Trades(trade_type='S', quantity=50, aid=1003, sid=2)

            tlst = [t1, t2, t3, t4, t5, t6]

            session.add_all(tlst)
            session.commit()

            logging.info("Inserted record(s) for 6 trades:")

            for tr in tlst:
                logging.info("\t==> %s" % tr)
        except SQLAlchemyError as e:
            logging.error(e)

        session.close()
    else:
        logging.info("The trades table *DOES NOT* exist !!!")


def query_trades(engine: Engine):
    if engine.dialect.has_table(engine, 'account') and engine.dialect.has_table(engine, 'customer') and \
            engine.dialect.has_table(engine, 'securities') and engine.dialect.has_table(engine, 'trades'):
        Session = sessionmaker(bind=engine)

        session = Session()

        logging.info("SQL => %s" % session.query(Customer.first_name, Customer.last_name, Account.acct_name,
                                                 Trades.trade_dt, Trades.trade_type, Trades.quantity,
                                                 Securities.symbol, Securities.price) \
                     .select_from(Trades).join(Account).join(Securities).join(Customer))
        recs = session.query(Customer.first_name, Customer.last_name, Account.acct_name,
                             Trades.trade_dt, Trades.trade_type, Trades.quantity,
                             Securities.symbol, Securities.price) \
            .select_from(Trades).join(Account).join(Securities).join(Customer)
        if recs.count() > 0:
            logging.info("< -------------------------")
            for r in recs:
                logging.info(r)
            logging.info("------------------------- >")
        else:
            logging.info("Record(s) for trades by customers *DOES NOT* exist !!!")

        session.close()
    else:
        logging.info("The account/customer/securities/trades table(s) *DOES NOT* exist !!!")


if __name__ == "__main__":
    db_engine = create_db_engine()
    if create_trades_table(db_engine):
        insert_trades(db_engine)
    query_trades(db_engine)
