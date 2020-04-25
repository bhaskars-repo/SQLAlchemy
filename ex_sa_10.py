import logging

from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from SQLAlchemy.ex_sa_00 import create_db_engine
from SQLAlchemy.ex_sa_09 import Base, Kyc

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


def create_kyc_table(engine: Engine) -> bool:
    status = False

    if not engine.dialect.has_table(engine, 'kyc'):
        Base.metadata.create_all(db_engine)

        logging.info("Created the kyc table !!!")

        status = True
    else:
        logging.info("The kyc table already exists !!!")

    return status


def insert_kyc_recs(engine: Engine):
    if engine.dialect.has_table(engine, 'kyc'):
        Session = sessionmaker(bind=engine)

        session = Session()

        try:
            ad_kyc = Kyc(kyc_flag=True, cid=1, ano=1001)
            session.add(ad_kyc)
            session.commit()

            logging.info("Inserted kyc for Alice")
        except SQLAlchemyError as e:
            logging.error(e)

        try:
            bb_kyc = Kyc(cid=2, ano=1002)
            session.add(bb_kyc)
            session.commit()

            logging.info("Inserted kyc for Bob")
        except SQLAlchemyError as e:
            logging.error(e)

        try:
            cd_kyc = Kyc(cid=3, ano=1003)
            session.add(cd_kyc)
            session.commit()

            logging.info("Inserted kyc for Charlie")
        except SQLAlchemyError as e:
            logging.error(e)

        session.close()
    else:
        logging.info("The kyc table *DOES NOT* exist !!!")


if __name__ == "__main__":
    db_engine = create_db_engine()
    if create_kyc_table(db_engine):
        insert_kyc_recs(db_engine)
