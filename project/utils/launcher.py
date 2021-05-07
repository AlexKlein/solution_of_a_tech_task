"""
Launcher for the ETL process running.
"""
from etl_core.etl_script.query_launcher import start_up as launcher_core
from etl_datamart.etl_script.query_launcher import start_up as launcher_datamart
from common.logger import logging


logger = logging.getLogger(__name__)


def start_up():
    logger.info(f"""Start ETL process""")
    print(f"""Start ETL process""")

    launcher_core()
    launcher_datamart()

    logger.info(f"""Finish ETL process""")
    print(f"""Finish ETL process""")
