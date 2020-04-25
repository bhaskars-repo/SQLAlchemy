from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import URL
from sqlalchemy.engine import create_engine

import logging

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


def create_db_engine() -> Engine:
    postgres_db = {'drivername': 'postgres',
                   'username': 'bswamina',
                   'password': 'bswamina$123',
                   'host': 'localhost',
                   'port': 5432,
                   'database': 'my_test_db'}
    db_url = URL(**postgres_db)

    logging.info("Postgres database url: %s" % db_url)

    db_engine = create_engine(db_url)

    with db_engine.connect() as db_conn:
        logging.info("Connected to the Postgres database !!!")

    return db_engine
