"""
Entrypoint for the ETL process running.
"""
import sys
import warnings
from datetime import datetime
from time import sleep

from models import stage_models as models
from common.logger import logging
from utils.db_migration import start_up as start_db
from utils.download_raw_data import start_up as download_files
from utils.launcher import start_up as launch
from utils.upload_raw_data_to_stage import start_up as upload_raw_data


logger = logging.getLogger(__name__)


def check_data():
    if not models.Metadata.table_exists():
        start_db()


def delay_db():
    i = 1
    run_flag = 0

    while i <= 10 and run_flag != 1:
        try:
            models.db_handle.connect()
            run_flag = 1
            break
        except Exception as e:
            sleep(i)
            i += 1

    if run_flag == 1:
        logger.info(f"""Database started""")
        print(f"""Database started""")
    else:
        logger.error(f"""Database didn't start""")
        print(f"""Database didn't start""")
        sys.exit(1)


if __name__ == '__main__':
    warnings.simplefilter(action='ignore')

    logger.info(f"""Start process""")
    print('Start in', datetime.now())

    delay_db()
    check_data()
    download_files()
    upload_raw_data()
    launch()

    logger.info(f"""Finish process""")
    print('Finish in', datetime.now())
