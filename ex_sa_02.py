from sqlalchemy.engine import Engine
from SQLAlchemy.ex_sa_00 import create_db_engine

import logging

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


def create_dummy_security(engine: Engine):
    if engine.dialect.has_table(engine, 'securities'):
        with engine.connect() as db_conn:
            resp = db_conn.execute('INSERT INTO securities '
                                   '(symbol, price) '
                                   'VALUES (\'DUMMY\', 1.00)')
            if resp.rowcount == 1:
                logging.info("Inserted record for DUMMY")
            else:
                logging.info("Failed to insert record for DUMMY")
    else:
        logging.info("The securities table *DOES NOT* exist !!!")


def query_dummy_security(engine: Engine):
    if engine.dialect.has_table(engine, 'securities'):
        with engine.connect() as db_conn:
            resp = db_conn.execute('SELECT symbol, price '
                                   'FROM securities '
                                   'WHERE symbol = \'DUMMY\'')
            if resp.rowcount == 1:
                logging.info("Selected record for DUMMY")

                row = resp.fetchone()

                logging.info('Symbol: %s, Price: %d' % (row['symbol'], row[1]))
            else:
                logging.info("Record for DUMMY *DOES NOT* exist !!!")
    else:
        logging.info("The securities table *DOES NOT* exist !!!")


def update_dummy_security(engine: Engine):
    if engine.dialect.has_table(engine, 'securities'):
        with engine.connect() as db_conn:
            resp = db_conn.execute('UPDATE securities '
                                   'SET price = 2.00 '
                                   'WHERE symbol = \'DUMMY\'')
            if resp.rowcount == 1:
                logging.info("Updated record for DUMMY")
            else:
                logging.info("Record for DUMMY *DOES NOT* exist !!!")
    else:
        logging.info("The securities table *DOES NOT* exist !!!")


def delete_dummy_security(engine: Engine):
    if engine.dialect.has_table(engine, 'securities'):
        with engine.connect() as db_conn:
            resp = db_conn.execute('DELETE FROM securities '
                                   'WHERE symbol = \'DUMMY\'')
            if resp.rowcount == 1:
                logging.info("Deleted record for DUMMY")
            else:
                logging.info("Record for DUMMY *DOES NOT* exist !!!")
    else:
        logging.info("The securities table *DOES NOT* exist !!!")


if __name__ == "__main__":
    db_engine = create_db_engine()
    create_dummy_security(db_engine)
    query_dummy_security(db_engine)
    update_dummy_security(db_engine)
    query_dummy_security(db_engine)
    delete_dummy_security(db_engine)
    query_dummy_security(db_engine)
