"""
Download and unzip files from external sources as raw data.
"""
import gzip
import shutil
from os import getcwd
from os.path import join

import requests

import settings as config
from common.logger import logging


logger = logging.getLogger(__name__)


FILES_DIR_PATH = join(join(getcwd(), 'storage'), 'data')
METADATA_LINK = config.LINKS['METADATA']
RATINGS_LINK = config.LINKS['RATINGS']


def download_file(url, file_name):
    logger.info(f"""Downloading file from {url}""")
    print(f"""Downloading file from {url}""")
    try:
        r = requests.get(url, allow_redirects=True)
        open(file_name, 'wb').write(r.content)
    except Exception as e:
        logger.error(f"""{e}""")
        print(f"""{e}""")


def unzip(file_name):
    logger.info(f"""Unzipping file {file_name}""")
    print(f"""Unzipping file {file_name}""")

    try:
        with gzip.open(file_name, 'rb') as f_in:
            with open(str(file_name).replace('.gz', ''), 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
    except Exception as e:
        logger.error(f"""{e}""")
        print(f"""{e}""")


def start_up():
    download_file(METADATA_LINK, join(FILES_DIR_PATH, 'metadata.json.gz'))
    download_file(RATINGS_LINK, join(FILES_DIR_PATH, 'ratings.csv'))
    unzip(join(FILES_DIR_PATH, 'metadata.json.gz'))
