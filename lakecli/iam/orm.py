import os
from peewee import *

database_proxy = DatabaseProxy()


class BaseModel(Model):
    class Meta:
        database = database_proxy


class DatabasePrivilege(BaseModel):
    class Meta:
        table_name = 'database_privileges'

    id = AutoField()
    schema_name = TextField()
    principal = TextField()
    permission = TextField()
    grant = BooleanField()


class TablePrivilege(BaseModel):
    class Meta:
        table_name = 'table_privileges'

    id = AutoField()
    schema_name = TextField()
    table_name = TextField()
    principal = TextField()
    permission = TextField()
    grant = BooleanField()


def init(path):
    database = SqliteDatabase(os.path.expanduser(path))
    database_proxy.initialize(database)
    database_proxy.connect()
    database_proxy.create_tables([DatabasePrivilege, TablePrivilege])

    return database_proxy


def model_db_close():
    database_proxy.close()
