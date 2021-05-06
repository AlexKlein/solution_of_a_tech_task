"""
ORM models for stage area of the PostgreSQL DB.
"""
from peewee import (
    CharField,
    DateField,
    FloatField,
    IntegerField,
    Model,
    PostgresqlDatabase,
    TextField,
    CompositeKey
)

import settings as config


POSTGRES_HOST = config.DATABASE['HOST']
POSTGRES_PORT = int(config.DATABASE['PORT'])
POSTGRES_DATABASE = config.DATABASE['DATABASE']
POSTGRES_USER = config.DATABASE['USER']
POSTGRES_PASSWORD = config.DATABASE['PASSWORD']

db_handle = PostgresqlDatabase(database=POSTGRES_DATABASE,
                               host=POSTGRES_HOST,
                               port=POSTGRES_PORT,
                               user=POSTGRES_USER,
                               password=POSTGRES_PASSWORD)


class BaseModel(Model):
    class Meta:
        database = db_handle


class Metadata(BaseModel):
    json_text = TextField()
    created_at = DateField(primary_key=True)

    class Meta:
        db_table = 'metadata'
        schema = 'stage'


class Rating(BaseModel):
    user_id = CharField(max_length=14)
    item = CharField(max_length=10)
    rating = FloatField()
    event_timestamp = IntegerField()
    created_at = DateField()

    class Meta:
        db_table = 'rating'
        schema = 'stage'
        primary_key = CompositeKey('user_id', 'item', 'event_timestamp')
