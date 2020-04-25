from sqlalchemy.engine import Engine
from SQLAlchemy.ex_sa_00 import create_db_engine

import logging

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


def create_securities_table(engine: Engine) -> bool:
    status = False

    if not engine.dialect.has_table(engine, 'securities'):
        engine.execute('CREATE TABLE securities ('
                       'id serial PRIMARY KEY,'
                       'symbol varchar(10) UNIQUE NOT NULL,'
                       'price NUMERIC(5, 2))')

        logging.info("Created the securities table !!!")

        engine.execute('CREATE INDEX idx_securities_symbol '
                       'ON securities(symbol)')

        logging.info("Created the idx_securities_symbol index !!!")

        status = True
    else:
        logging.info("The securities table already exists !!!")

    return status


def insert_securities_recs(engine: Engine):
    if engine.dialect.has_table(engine, 'securities'):
        with engine.connect() as db_conn:
            # Record - 1
            resp = db_conn.execute('INSERT INTO securities '
                                   '(symbol, price) '
                                   'VALUES (\'BULL.ST\', 25.75)')
            if resp.rowcount == 1:
                logging.info("Inserted record for BULL.ST")
            else:
                logging.info("Failed to insert record for BULL.ST")

            # Record - 2
            resp = db_conn.execute('INSERT INTO securities '
                                   '(symbol, price) '
                                   'VALUES (\'DOG.ST\', 54.15)')
            if resp.rowcount == 1:
                logging.info("Inserted record for DOG.ST")
            else:
                logging.info("Failed to insert record for DOG.ST")

            # Record - 3
            resp = db_conn.execute('INSERT INTO securities '
                                   '(symbol, price) '
                                   'VALUES (\'BARK.ST\', 144.90)')
            if resp.rowcount == 1:
                logging.info("Inserted record for BARK.ST")
            else:
                logging.info("Failed to insert record for BARK.ST")
    else:
        logging.info("The securities table *DOES NOT* exist !!!")


if __name__ == "__main__":
    db_engine = create_db_engine()
    if create_securities_table(db_engine):
        insert_securities_recs(db_engine)
