"""
Launcher for the ETL process running.
"""
from etl_core.etl_script.query_launcher import start_up as launcher
from common.logger import logging


logger = logging.getLogger(__name__)


def start_up():
    logger.info(f"""Start ETL process""")
    print(f"""Start ETL process""")

    launcher()

    logger.info(f"""Finish ETL process""")
    print(f"""Finish ETL process""")
