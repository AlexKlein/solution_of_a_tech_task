"""
An ETL process for uploading CSV and JSON files to a PostgreSQL DB.
"""
import csv
import datetime
from os.path import join

import settings as config
from common.logger import logging
from models import stage_models as models


logger = logging.getLogger(__name__)


FILES_DIR_PATH = config.FILES_DIR_PATH


def insert_set(data_source, model_name):
    try:
        model_name.create(**data_source)
    except Exception as e:
        logger.error(f"""Model: {model_name.__name__} Data: {data_source}""")
        print(f"""Model: {model_name.__name__} Data: {data_source}""")


def upload_files(model_name, headers, file_name):
    rows_counter = 0
    try:
        with open(join(FILES_DIR_PATH, file_name), newline='') as f:
            logger.info(f"""Inserting data to {model_name.__name__}""")
            print(f"""Inserting data to {model_name.__name__}""")

            model_name.truncate_table()
            reader = csv.reader(f)

            for row in reader:
                insert_row = {headers[i]: row[i] for i in range(len(headers))}
                insert_row['created_at'] = datetime.date.today()
                insert_set(insert_row, model_name)
                rows_counter += 1

        return rows_counter

    except OSError as e:
        logger.error(f"""{e}""")
        print(f"""{e}""")


def upload_jsons(model_name, headers, file_name):
    rows_counter = 0
    try:
        with open(join(FILES_DIR_PATH, file_name), newline='') as f:
            logger.info(f"""Inserting data to {model_name.__name__}""")
            print(f"""Inserting data to {model_name.__name__}""")

            model_name.truncate_table()
            reader = csv.reader(f)

            for row in reader:
                insert_row = {headers[0]: row, 'created_at': datetime.date.today()}
                insert_set(insert_row, model_name)
                rows_counter += 1

        return rows_counter

    except OSError as e:
        logger.error(f"""{e}""")
        print(f"""{e}""")


def start_up():
    model_name = models.Metadata
    file_name = 'metadata.json'
    headers = ['json_text']
    logger.info(f"""Reading {file_name} in memory""")
    print(f"""Reading {file_name} in memory""")
    result = upload_jsons(model_name, headers, file_name)
    logger.info(f"""{result} were processed""")
    print(f"""{result} were processed""")

    model_name = models.Rating
    file_name = 'ratings.csv'
    headers = ['user_id', 'item', 'rating', 'event_timestamp']
    logger.info(f"""Reading {file_name} in memory""")
    print(f"""Reading {file_name} in memory""")
    result = upload_files(model_name, headers, file_name)
    logger.info(f"""{result} were processed""")
    print(f"""{result} were processed""")
