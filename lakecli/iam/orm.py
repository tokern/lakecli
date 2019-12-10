import os
from peewee import *

database_proxy = DatabaseProxy()


class BaseModel(Model):
    class Meta:
        database = database_proxy


class Schema(BaseModel):
    class Meta:
        table_name = 'schemata'

    id = AutoField()
    schema_name = TextField()
    location = TextField(null=True)


class Table(BaseModel):
    class Meta:
        table_name = 'tables'

    id = AutoField()
    table_schema = TextField()
    table_name = TextField()
    create_time = TextField(null=True)
    last_access_time = TextField(null=True)


class Column(BaseModel):
    class Meta:
        table_name = 'columns'

    id = AutoField()
    table_schema = TextField()
    table_name = TextField()
    column_name = TextField()
    data_type = TextField()
    is_partition_column = BooleanField()
    pii = TextField(null=True)
    ordinal = IntegerField()


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
    database_proxy.create_tables([DatabasePrivilege, TablePrivilege, Schema, Table, Column])

    return database_proxy


def model_db_close():
    database_proxy.close()
