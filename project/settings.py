"""
Settings for the app.
"""
import os


VERSION = '0.1'
DATABASE = {
    'ENGINE': 'psycopg2',
    'HOST': os.getenv('POSTGRES_HOST'),
    'PORT': os.getenv('POSTGRES_PORT'),
    'DATABASE': os.getenv('POSTGRES_DB'),
    'USER': os.getenv('POSTGRES_USER'),
    'PASSWORD': os.getenv('POSTGRES_PASSWORD')
}
LINKS = {
    'METADATA': 'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/meta_Movies_and_TV.json.gz',
    'RATINGS': 'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/ratings_Movies_and_TV.csv'
}
