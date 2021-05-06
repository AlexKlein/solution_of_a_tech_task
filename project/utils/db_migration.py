"""
Util for running SQL statements for the first step of working with the database.
"""
import os

from common import postgres_wrapper
from common.logger import logging


logger = logging.getLogger(__name__)


def commands_launcher(sql_file, conn):
    sql_commands = sql_file.split(';')

    for command in sql_commands:

        if len(command.strip()) > 0:
            try:
                conn.execute(raw_sql=command)
            except Exception as e:
                e = str(e).replace('\n', '')
                logger.error(f"""Crashed when running the command: {e}""")
                print(f"""Crashed when running the command: {e}""")

    conn.execute(raw_sql='commit')


def start_up():
    connection = postgres_wrapper.PostgresWrapper()

    for path, dirs, files in os.walk(os.path.dirname(os.getcwd())):

        if path.find('migrations') > 0:

            for f in files:
                full_path = os.path.join(path, f)

                with open(full_path, 'r', encoding='UTF-8') as file:
                    try:
                        commands_launcher(file.read(), connection)
                    except:
                        logger.error(f"""Troubles with {full_path}""")

    connection.close()
