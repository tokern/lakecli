from lakecli.iam.scanner import Scanner
from unittest import TestCase


def test_table_permissions():
    resource = {
        'Table': {
            'DatabaseName': 'taxidata',
            'Name': 'parq_trip_data'
        }
    }

    privilege = Scanner._new_privilege('SELECT', resource, False, "user/user")
    assert not privilege.grant
    assert privilege.permission == 'SELECT'
    assert privilege.schema_name == 'taxidata'
    assert privilege.table_name == 'parq_trip_data'


def test_table_with_cols_permissions():
    resource = {
        'TableWithColumns': {
            'DatabaseName': 'taxidata',
            'Name': 'parq_trip_data',
            'ColumnWildcard': {}
        }
    }

    privilege = Scanner._new_privilege('SELECT', resource, False, "user/user")
    assert not privilege.grant
    assert privilege.permission == 'SELECT'
    assert privilege.schema_name == 'taxidata'
    assert privilege.table_name == 'parq_trip_data'


def test_prinicpal():
    principal = Scanner._principal("arn:aws:iam::1111111:user/datalake_user")

    assert principal == "user/datalake_user"


class ScannerTests(TestCase):

#    database_list = {'DatabaseList': [{'Name': 'default', 'CreateTime': datetime.datetime(2019, 10, 27, 10, 39, 58, tzinfo=tzlocal()), 'CreateTableDefaultPermissions': [{'Principal': {'DataLakePrincipalIdentifier': 'IAM_ALLOWED_PRINCIPALS'}, 'Permissions': ['ALL']}]}, {'Name': 'sampledb', 'Description': 'Sample database', 'Parameters': {'CreatedBy': 'Athena', 'EXTERNAL': 'TRUE'}, 'CreateTime': datetime.datetime(2019, 10, 26, 10, 23, 10, tzinfo=tzlocal()), 'CreateTableDefaultPermissions': [{'Principal': {'DataLakePrincipalIdentifier': 'IAM_ALLOWED_PRINCIPALS'}, 'Permissions': ['ALL']}]}, {'Name': 'taxidata', 'CreateTime': datetime.datetime(2019, 11, 26, 12, 17, 49, tzinfo=tzlocal()), 'CreateTableDefaultPermissions': [{'Principal': {'DataLakePrincipalIdentifier': 'IAM_ALLOWED_PRINCIPALS'}, 'Permissions': ['ALL']}]}, {'Name': 'taxilake', 'LocationUri': 's3://tokerndev-datalake', 'CreateTime': datetime.datetime(2019, 11, 26, 21, 45, 50, tzinfo=tzlocal()), 'CreateTableDefaultPermissions': []}]
#    table_list = {'TableList': [{'Name': 'csv_misc', 'DatabaseName': 'taxidata', 'Owner': 'owner', 'CreateTime': datetime.datetime(2019, 12, 9, 16, 12, 43, tzinfo=tzlocal()), 'UpdateTime': datetime.datetime(2019, 12, 9, 17, 33, 20, tzinfo=tzlocal()), 'LastAccessTime': datetime.datetime(2019, 12, 9, 16, 12, 43, tzinfo=tzlocal()), 'Retention': 0, 'StorageDescriptor': {'Columns': [{'Name': 'locationid', 'Type': 'bigint'}, {'Name': 'borough', 'Type': 'string', 'Parameters': {'PII': 'PiiTypes.ADDRESS'}}, {'Name': 'zone', 'Type': 'string', 'Parameters': {'PII': 'PiiTypes.ADDRESS'}}, {'Name': 'service_zone', 'Type': 'string', 'Parameters': {'PII': 'PiiTypes.ADDRESS'}}], 'Location': 's3://nyc-tlc/misc/', 'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat', 'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat', 'Compressed': False, 'NumberOfBuckets': -1, 'SerdeInfo': {'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe', 'Parameters': {'field.delim': ','}}, 'BucketColumns': [], 'SortColumns': [], 'Parameters': {'CrawlerSchemaDeserializerVersion': '1.0', 'CrawlerSchemaSerializerVersion': '1.0', 'UPDATED_BY_CRAWLER': 'TaxiCrawler', 'areColumnsQuoted': 'false', 'averageRecordSize': '36', 'classification': 'csv', 'columnsOrdered': 'true', 'compressionType': 'none', 'delimiter': ',', 'exclusions': '["s3://nyc-tlc/misc/*foil*","s3://nyc-tlc/misc/shared*","s3://nyc-tlc/misc/uber*","s3://nyc-tlc/misc/*.html","s3://nyc-tlc/misc/*.zip","s3://nyc-tlc/misc/FOIL_*"]', 'objectCount': '1', 'recordCount': '342', 'sizeKey': '12322', 'skip.header.line.count': '1', 'typeOfData': 'file'}, 'StoredAsSubDirectories': False}, 'PartitionKeys': [], 'TableType': 'EXTERNAL_TABLE', 'Parameters': {'CrawlerSchemaDeserializerVersion': '1.0', 'CrawlerSchemaSerializerVersion': '1.0', 'UPDATED_BY_CRAWLER': 'TaxiCrawler', 'areColumnsQuoted': 'false', 'averageRecordSize': '36', 'classification': 'csv', 'columnsOrdered': 'true', 'compressionType': 'none', 'delimiter': ',', 'exclusions': '["s3://nyc-tlc/misc/*foil*","s3://nyc-tlc/misc/shared*","s3://nyc-tlc/misc/uber*","s3://nyc-tlc/misc/*.html","s3://nyc-tlc/misc/*.zip","s3://nyc-tlc/misc/FOIL_*"]', 'objectCount': '1', 'recordCount': '342', 'sizeKey': '12322', 'skip.header.line.count': '1', 'typeOfData': 'file'}, 'IsRegisteredWithLakeFormation': False}, {'Name': 'csv_trip_data', 'DatabaseName': 'taxidata', 'Owner': 'owner', 'CreateTime': datetime.datetime(2019, 12, 9, 16, 12, 43, tzinfo=tzlocal()), 'UpdateTime': datetime.datetime(2019, 12, 9, 16, 12, 43, tzinfo=tzlocal()), 'LastAccessTime': datetime.datetime(2019, 12, 9, 16, 12, 43, tzinfo=tzlocal()), 'Retention': 0, 'StorageDescriptor': {'Columns': [{'Name': 'dispatching_base_num', 'Type': 'string'}, {'Name': 'pickup_datetime', 'Type': 'string'}, {'Name': 'dropoff_datetime', 'Type': 'string'}, {'Name': 'pulocationid', 'Type': 'bigint'}, {'Name': 'dolocationid', 'Type': 'bigint'}, {'Name': 'sr_flag', 'Type': 'bigint'}, {'Name': 'hvfhs_license_num', 'Type': 'string'}], 'Location': 's3://nyc-tlc/trip data/', 'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat', 'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat', 'Compressed': False, 'NumberOfBuckets': -1, 'SerdeInfo': {'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe', 'Parameters': {'field.delim': ','}}, 'BucketColumns': [], 'SortColumns': [], 'Parameters': {'CrawlerSchemaDeserializerVersion': '1.0', 'CrawlerSchemaSerializerVersion': '1.0', 'UPDATED_BY_CRAWLER': 'TaxiCrawler', 'areColumnsQuoted': 'false', 'averageRecordSize': '70', 'classification': 'csv', 'columnsOrdered': 'true', 'compressionType': 'none', 'delimiter': ',', 'exclusions': '["s3://nyc-tlc/trip data/fhv_tripdata_2015*","s3://nyc-tlc/trip data/fhv_tripdata_2016*","s3://nyc-tlc/trip data/fhv_tripdata_2017*","s3://nyc-tlc/trip data/fhv_tripdata_2018*","s3://nyc-tlc/trip data/yellow*","s3://nyc-tlc/trip data/green*"]', 'objectCount': '11', 'recordCount': '120729113', 'sizeKey': '8731786276', 'skip.header.line.count': '1', 'typeOfData': 'file'}, 'StoredAsSubDirectories': False}, 'PartitionKeys': [], 'TableType': 'EXTERNAL_TABLE', 'Parameters': {'CrawlerSchemaDeserializerVersion': '1.0', 'CrawlerSchemaSerializerVersion': '1.0', 'UPDATED_BY_CRAWLER': 'TaxiCrawler', 'areColumnsQuoted': 'false', 'averageRecordSize': '70', 'classification': 'csv', 'columnsOrdered': 'true', 'compressionType': 'none', 'delimiter': ',', 'exclusions': '["s3://nyc-tlc/trip data/fhv_tripdata_2015*","s3://nyc-tlc/trip data/fhv_tripdata_2016*","s3://nyc-tlc/trip data/fhv_tripdata_2017*","s3://nyc-tlc/trip data/fhv_tripdata_2018*","s3://nyc-tlc/trip data/yellow*","s3://nyc-tlc/trip data/green*"]', 'objectCount': '11', 'recordCount': '120729113', 'sizeKey': '8731786276', 'skip.header.line.count': '1', 'typeOfData': 'file'}, 'CreatedBy': 'arn:aws:sts::172965158661:assumed-role/LakeFormationWorkflowRole/AWS-Crawler', 'IsRegisteredWithLakeFormation': False}]

    def test_parse_columns(self):
        column_list = [
            {'Name': 'locationid', 'Type': 'bigint'},
            {'Name': 'borough', 'Type': 'string', 'Parameters': {'PII': 'PiiTypes.ADDRESS'}},
            {'Name': 'zone', 'Type': 'string', 'Parameters': {'PII': 'PiiTypes.ADDRESS'}},
            {'Name': 'service_zone', 'Type': 'string', 'Parameters': {'PII': 'PiiTypes.ADDRESS'}}
        ]

        ordinal_number = 1
        parsed_columns, ordinal_number = Scanner.parse_columns(column_list, ordinal_number, False, 'schema', 'table')

        self.assertEqual(5, ordinal_number)

        self.assertEqual("schema", parsed_columns[0].table_schema)
        self.assertEqual("table", parsed_columns[0].table_name)
        self.assertEqual(1, parsed_columns[0].ordinal)
        self.assertEqual("locationid", parsed_columns[0].column_name)
        self.assertEqual("bigint", parsed_columns[0].data_type)
        self.assertIsNone(parsed_columns[0].pii)

        self.assertEqual("schema", parsed_columns[1].table_schema)
        self.assertEqual("table", parsed_columns[1].table_name)
        self.assertEqual(2, parsed_columns[1].ordinal)
        self.assertEqual("borough", parsed_columns[1].column_name)
        self.assertEqual("string", parsed_columns[1].data_type)
        self.assertEqual("PiiTypes.ADDRESS", parsed_columns[1].pii)
