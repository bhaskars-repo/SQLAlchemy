from sqlalchemy.engine import Engine
from sqlalchemy import MetaData, Table, Column
from sqlalchemy import Integer, String
from SQLAlchemy.ex_sa_00 import create_db_engine

import logging

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


def create_customer_table(engine: Engine) -> bool:
    status = False

    if not engine.dialect.has_table(engine, 'customer'):
        metadata = MetaData()
        customer_table = Table(
            'customer',
            metadata,
            Column('id', Integer, autoincrement=True, primary_key=True),
            Column('first_name', String(25), nullable=False),
            Column('last_name', String(25), nullable=False, index=True),
            Column('email', String(50)),
            Column('mobile', String(10))
        )
        customer_table.create(engine)

        logging.info("Created the customer table !!!")

        status = True
    else:
        logging.info("The customer table already exists !!!")

    return status


def insert_customer_recs(engine: Engine):
    if engine.dialect.has_table(engine, 'customer'):
        metadata = MetaData(bind=engine, reflect=True)
        customer_table = metadata.tables['customer']

        with engine.connect() as db_conn:
            # Record - 1
            rec_1 = customer_table.insert().values(
                first_name='Alice',
                last_name='Doctor',
                email='alice.d@timbuk2.do',
                mobile='1112223333'
            )
            resp = db_conn.execute(rec_1)
            if resp.rowcount == 1:
                logging.info("Inserted record for Alice")
            else:
                logging.info("Failed to insert record for Alice")

            # Record - 2
            rec_2 = customer_table.insert().values(
                first_name='Bob',
                last_name='Builder',
                email='bbuilder@nomansland.bu'
            )
            resp = db_conn.execute(rec_2)
            if resp.rowcount == 1:
                logging.info("Inserted record for Bob")
            else:
                logging.info("Failed to insert record for Bob")

            # Record - 3
            rec_3 = customer_table.insert().values(
                first_name='Charlie',
                last_name='Driver',
                email='charlie.driver@vehicles.ve',
                mobile='2223334444'
            )
            resp = db_conn.execute(rec_3)
            if resp.rowcount == 1:
                logging.info("Inserted record for Charlie")
            else:
                logging.info("Failed to insert record for Charlie")
    else:
        logging.info("The customer table *DOES NOT* exist !!!")


if __name__ == "__main__":
    db_engine = create_db_engine()
    if create_customer_table(db_engine):
        insert_customer_recs(db_engine)
