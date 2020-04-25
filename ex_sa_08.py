import logging

from sqlalchemy import and_, func
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from SQLAlchemy.ex_sa_00 import create_db_engine
from SQLAlchemy.ex_sa_05 import Customer, Account

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


def create_cust_accts(engine: Engine):
    if engine.dialect.has_table(engine, 'customer') and engine.dialect.has_table(engine, 'account'):
        Session = sessionmaker(bind=engine)

        session = Session()

        c1 = Customer(first_name='David', last_name='Plumber', email='dplum@home.co', mobile='4445556666')
        c2 = Customer(first_name='Emily', last_name='Player', email='emilyp@cool.net')
        c3 = Customer(first_name='Frank', last_name='Doctor', email='fdoctor@pain.cc', mobile='5556667777')

        try:
            session.add_all([c1, c2, c3])
            session.commit()

            logging.info("Inserted record(s) for 3 customers: %s, %s, %s" % \
                         (c1.first_name, c2.first_name, c3.first_name))
        except SQLAlchemyError as e:
            logging.error(e)

        try:
            d1 = Account(acct_no=2001, acct_name='David Trade Account', cust_id=c1.id)
            d2 = Account(acct_no=2002, acct_name='David Cash Account', cust_id=c1.id)
            d3 = Account(acct_no=2003, acct_name='Emily Crypto Account', cust_id=c2.id)
            d4 = Account(acct_no=2004, acct_name='Frank Cash Account', cust_id=c3.id)
            d5 = Account(acct_no=2005, acct_name='Frank Credit Account', cust_id=c3.id)
            session.add_all([d1, d2, d3, d4, d5])

            session.commit()

            logging.info("Inserted record(s) for 5 accounts for: %s, %s, %s" % \
                         (c1.first_name, c2.first_name, c3.first_name))
        except SQLAlchemyError as e:
            logging.error(e)

        session.close()
    else:
        logging.info("The customer and/or account table(s) *DOES NOT* exist !!!")


def query_cust_accts(engine: Engine):
    if engine.dialect.has_table(engine, 'customer') and engine.dialect.has_table(engine, 'account'):
        Session = sessionmaker(bind=engine)

        session = Session()

        logging.info("SQL => %s" % session.query(Customer.last_name, Customer.email))
        recs = session.query(Customer.last_name, Customer.email).all()
        if len(recs) > 0:
            for r in recs:
                logging.info(r)
        else:
            logging.info("Record(s) in customer *DO NOT* exist !!!")
        logging.info("-------------------------")

        logging.info("SQL => %s" % session.query(Customer.last_name, Customer.email).\
                     filter(Customer.last_name.like('pl%')))
        recs = session.query(Customer.last_name, Customer.email).filter(Customer.last_name.like('pl%'))
        if recs.count() > 0:
            for r in recs:
                logging.info(r)
        else:
            logging.info("Record(s) in customer for last_name like 'pl%' *DO NOT* exist !!!")
        logging.info("-------------------------")

        recs = session.query(Customer.last_name, Customer.email).filter(Customer.last_name.ilike('pl%'))
        if recs.count() > 0:
            for r in recs:
                logging.info(r)
        else:
            logging.info("Record(s) in customer for last_name like (insensitive) 'pl%' *DO NOT* exist !!!")
        logging.info("-------------------------")

        logging.info("SQL => %s" % session.query(Customer.first_name, Customer.last_name, Customer.email).\
                     filter(Customer.last_name.in_(['Driver', 'Plumber'])))
        recs = session.query(Customer.first_name, Customer.last_name, Customer.email). \
            filter(Customer.last_name.in_(['Driver', 'Plumber']))
        if recs.count() > 0:
            for r in recs:
                logging.info(r)
        else:
            logging.info("Record(s) in customer for last_name in ['Driver', 'Plumber'] *DO NOT* exist !!!")
        logging.info("-------------------------")

        logging.info("SQL => %s" % session.query(Customer).order_by(Customer.last_name))
        recs = session.query(Customer).order_by(Customer.last_name)
        if recs.count() > 0:
            for r in recs:
                logging.info(r)
        else:
            logging.info("Record(s) in customer *DO NOT* exist !!!")
        logging.info("-------------------------")

        recs = session.query(Customer.first_name, Customer.last_name, Customer.mobile).filter(Customer.mobile != None)
        if recs.count() > 0:
            for r in recs:
                logging.info(r)
        else:
            logging.info("Record(s) in customer with mobile *DO NOT* exist !!!")
        logging.info("-------------------------")

        logging.info("SQL => %s" % session.query(Account).limit(2))
        recs = session.query(Account).limit(2)
        if recs.count() > 0:
            for r in recs:
                logging.info(r)
        else:
            logging.info("Record(s) in account *DO NOT* exist !!!")
        logging.info("-------------------------")

        logging.info("SQL => %s" % session.query(Customer.last_name, Account.acct_name).join(Account))
        recs = session.query(Customer.last_name, Account.acct_name).join(Account)
        if recs.count() > 0:
            for r in recs:
                logging.info(r)
        else:
            logging.info("Record(s) in account.join(customer) *DO NOT* exist !!!")
        logging.info("-------------------------")

        logging.info("SQL => %s" % session.query(Customer.last_name, func.count(Account.cust_id).label('count')).\
                     join(Account).group_by(Customer.id).order_by('count'))
        recs = session.query(Customer.last_name, func.count(Account.cust_id).label('count')).\
                     join(Account).group_by(Customer.id).order_by('count')
        if recs.count() > 0:
            for r in recs:
                logging.info(r)
        else:
            logging.info("Record(s) in account.join(customer) group_by *DO NOT* exist !!!")
        logging.info("-------------------------")

        session.close()
    else:
        logging.info("The account/customer table(s) *DOES NOT* exist !!!")


def delete_cust_accts(engine: Engine):
    if engine.dialect.has_table(engine, 'customer') and engine.dialect.has_table(engine, 'account'):
        Session = sessionmaker(bind=engine)

        session = Session()

        session.query(Account).filter(and_(Account.acct_no >= 2001, Account.acct_no <= 2005)).delete()

        session.commit()

        logging.info("Deleted record(s) for account numbers: [2001 thru 2005]")

        session.query(Customer).filter(Customer.first_name == 'Frank').delete()
        session.query(Customer).filter(Customer.last_name.like('%Pl%')).delete(synchronize_session=False)

        session.commit()

        logging.info("Deleted record(s) for customers: [David, Emily, Frank]")

        session.close()
    else:
        logging.info("The account table *DOES NOT* exist !!!")


if __name__ == "__main__":
    db_engine = create_db_engine()
    create_cust_accts(db_engine)
    query_cust_accts(db_engine)
    delete_cust_accts(db_engine)
