from sqlalchemy.engine import Engine
from sqlalchemy import MetaData
from SQLAlchemy.ex_sa_00 import create_db_engine

import logging

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


def create_dummy_customer(engine: Engine):
    if engine.dialect.has_table(engine, 'customer'):
        metadata = MetaData(bind=engine, reflect=True)
        customer_table = metadata.tables['customer']
        with engine.connect() as db_conn:
            dummy = customer_table.insert().values(
                first_name='Dummy',
                last_name='Joker',
                email='dj@nowhere.cc'
            )
            resp = db_conn.execute(dummy)
            if resp.rowcount == 1:
                logging.info("Inserted record for Dummy")
            else:
                logging.info("Failed to insert record for Dummy")
    else:
        logging.info("The customer table *DOES NOT* exists !!!")


def query_dummy_customer(engine: Engine):
    if engine.dialect.has_table(engine, 'customer'):
        metadata = MetaData(bind=engine, reflect=True)
        customer_table = metadata.tables['customer']
        with engine.connect() as db_conn:
            # Select all columns
            dummy = customer_table.select() \
                .where(customer_table.columns.last_name == 'Joker')
            resp = db_conn.execute(dummy)
            if resp.rowcount == 1:
                logging.info("Selected record for Dummy")

                row = resp.fetchone()

                logging.info('First name: %s, Last name: %s, Email: %s' % (row['first_name'], row[2], row['email']))
            else:
                logging.info("Record for Dummy *DOES NOT* exists !!!")
    else:
        logging.info("The customer table *DOES NOT* exists !!!")


def update_dummy_customer(engine: Engine):
    if engine.dialect.has_table(engine, 'customer'):
        metadata = MetaData(bind=engine, reflect=True)
        customer_table = metadata.tables['customer']
        with engine.connect() as db_conn:
            dummy = customer_table.update() \
                .where(customer_table.c.last_name == 'Joker') \
                .values(email='djoker@dummy.io')
            resp = db_conn.execute(dummy)
            if resp.rowcount == 1:
                logging.info("Updated record for Dummy")
            else:
                logging.info("Record for Dummy *DOES NOT* exist !!!")
    else:
        logging.info("The customer table *DOES NOT* exist !!!")


def delete_dummy_customer(engine: Engine):
    if engine.dialect.has_table(engine, 'customer'):
        metadata = MetaData(bind=engine, reflect=True)
        customer_table = metadata.tables['customer']
        with engine.connect() as db_conn:
            dummy = customer_table.delete().where(customer_table.c.last_name == 'Joker')
            resp = db_conn.execute(dummy)
            if resp.rowcount == 1:
                logging.info("Deleted record for Dummy")
            else:
                logging.info("Record for Dummy *DOES NOT* exist !!!")
    else:
        logging.info("The customer table *DOES NOT* exist !!!")


def query_customer(engine: Engine):
    if engine.dialect.has_table(engine, 'customer'):
        metadata = MetaData(bind=engine, reflect=True)
        customer_table = metadata.tables['customer']
        with engine.connect() as db_conn:
            # Select all records and all columns
            query = customer_table.select()
            resp = db_conn.execute(query)
            if resp.rowcount > 0:
                for row in resp.fetchall():
                    logging.info('First name: %s, Last name: %s, Email: %s' % (row['first_name'], row[2], row['email']))
                logging.info("-------------------------")
            else:
                logging.info("No record(s) exist !!!")

            # Select all records and only columns last_name and email
            query = customer_table.select().with_only_columns([customer_table.c.last_name, customer_table.c.email])
            resp = db_conn.execute(query)
            if resp.rowcount > 0:
                for row in resp.fetchall():
                    logging.info('Last name: %s, Email: %s' % (row[0], row['email']))
                logging.info("-------------------------")
            else:
                logging.info("No record(s) exist !!!")

            # Select all records and only columns last_name and email order by last_name
            query = customer_table.select().with_only_columns([customer_table.c.last_name, customer_table.c.email]) \
                                  .order_by('last_name')
            resp = db_conn.execute(query)
            if resp.rowcount > 0:
                for row in resp.fetchall():
                    logging.info('Last name: %s, Email: %s' % (row[0], row['email']))
                logging.info("-------------------------")
            else:
                logging.info("No record(s) exist !!!")
    else:
        logging.info("The customer table *DOES NOT* exist !!!")


if __name__ == "__main__":
    db_engine = create_db_engine()
    create_dummy_customer(db_engine)
    query_dummy_customer(db_engine)
    update_dummy_customer(db_engine)
    query_dummy_customer(db_engine)
    delete_dummy_customer(db_engine)
    query_dummy_customer(db_engine)
    query_customer(db_engine)
