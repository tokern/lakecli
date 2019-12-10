import os
import boto3
import logging

from lakecli.iam.orm import TablePrivilege, DatabasePrivilege, init, model_db_close, Table, Database, Schema, Column

LOGGER = logging.getLogger(__name__)


class Scanner:
    def __init__(self, aws_config, path):
        self.aws_config = aws_config
        self.path = os.path.expanduser(path)
        if os.path.exists(self.path):
            LOGGER.info("Remove old sqlite database at %s" % self.path)
            os.remove(self.path)

    @staticmethod
    def parse_columns(column_list, ordinal_no, is_partition_col, schema_name, table_name):
        LOGGER.debug(column_list)
        LOGGER.info("%d columns found in %s.%s" % (
            len(column_list), schema_name, table_name))
        columns = []
        for c in column_list:
            pii = c['Parameters']['PII'] if 'Parameters' in c and 'PII' in c['Parameters'] else None
            columns.append(Column(
                table_schema=schema_name,
                table_name=table_name,
                column_name=c['Name'],
                data_type=c['Type'],
                is_partition_column=is_partition_col,
                pii=pii,
                ordinal=ordinal_no
            ))
            ordinal_no += 1

        return columns, ordinal_no

    def scan(self):
        lake_client = boto3.client('lakeformation',
                                   region_name=self.aws_config.region,
                                   aws_access_key_id=self.aws_config.aws_access_key_id,
                                   aws_secret_access_key=self.aws_config.aws_secret_access_key)

        table_permissions = []
        next_token = ''

        while next_token is not None:
            result = lake_client.list_permissions(
                ResourceType='TABLE',
                NextToken=next_token
            )
            table_permissions += result['PrincipalResourcePermissions']
            next_token = result['NextToken'] if 'NextToken' in result else None

        LOGGER.debug(table_permissions)

        database_permissions = lake_client.list_permissions(
            ResourceType='DATABASE',
        )
        LOGGER.debug(database_permissions)

        table_privileges = []
        LOGGER.info("%d table permissions found." % len(table_permissions))
        for resource in table_permissions:
            for permission in resource['Permissions']:
                grant = permission in resource['PermissionsWithGrantOption']
                table_privileges.append(self._new_privilege(permission, resource['Resource'],
                                                            grant,
                                                            self._principal(
                                                                resource['Principal']['DataLakePrincipalIdentifier'])))

        db_privileges = []
        LOGGER.info("%d database permissions found." % len(database_permissions['PrincipalResourcePermissions']))
        for resource in database_permissions['PrincipalResourcePermissions']:
            for permission in resource['Permissions']:
                grant = permission in resource['PermissionsWithGrantOption']
                db_privileges.append(DatabasePrivilege(
                    schema_name=resource['Resource']['Database']['Name'],
                    principal=self._principal(resource['Principal']['DataLakePrincipalIdentifier']),
                    permission=permission, grant=grant
                ))

        glue_client = boto3.client('glue',
                                   region_name=self.aws_config.region,
                                   aws_access_key_id=self.aws_config.aws_access_key_id,
                                   aws_secret_access_key=self.aws_config.aws_secret_access_key)

        schemata = []
        tables = []
        columns = []

        schemata_response = glue_client.get_databases()
        LOGGER.debug(schemata_response)
        LOGGER.info("%d databases found " % len(schemata_response['DatabaseList']))
        for d in schemata_response['DatabaseList']:

            schemata.append(Schema(
                schema_name=d['Name'],
                location=d['LocationUri'] if 'LocationUri' in d else None
            ))

            tables_response = glue_client.get_tables(
                DatabaseName=d['Name']
            )
            LOGGER.debug(tables_response)
            LOGGER.info("%d tables found in %s" % (len(tables_response['TableList']), d['Name']))
            for t in tables_response['TableList']:
                tables.append(Table(
                    table_schema=d['Name'],
                    table_name=t['Name'],
                    create_time=t['CreateTime'],
                    last_access_time=t['LastAccessTime'] if 'LastAccessTime' in t else None,
                ))

                ordinal_no = 1
                if 'StorageDescriptor' in t:
                    for key in ('PartitionKeys', 'Columns'):
                        if key in t['StorageDescriptor']:
                            column_list = t['StorageDescriptor'][key]
                            parsed_columns, ordinal_no = Scanner.parse_columns(column_list,
                                                                               ordinal_no,
                                                                               key == 'PartitionKeys',
                                                                               d['Name'], t['Name'])
                            columns.extend(parsed_columns)

        connection = init(self.path)
        try:
            with connection.atomic():
                TablePrivilege.bulk_create(table_privileges, batch_size=100)
                DatabasePrivilege.bulk_create(db_privileges, batch_size=100)
                Schema.bulk_create(schemata, batch_size=100)
                Table.bulk_create(tables, batch_size=100)
                Column.bulk_create(columns, batch_size=100)

        finally:
            model_db_close()

    @staticmethod
    def _principal(arn):
        # http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html
        LOGGER.debug(arn)
        elements = arn.split(':', 5)
        if len(elements) != 6:
            LOGGER.warning("Unknown format for arn '%s'" % arn)
            return arn

        result = {
            'arn': elements[0],
            'partition': elements[1],
            'service': elements[2],
            'region': elements[3],
            'account': elements[4],
            'resource': elements[5],
            'resource_type': None
        }
        if '/' in result['resource']:
            result['resource_type'], result['resource'] = result['resource'].split('/', 1)
        elif ':' in result['resource']:
            result['resource_type'], result['resource'] = result['resource'].split(':', 1)
        return "%s/%s" % (result['resource_type'], result['resource'])

    @staticmethod
    def _new_privilege(permission, resource, grant, principal):
        LOGGER.debug(resource)
        if 'Table' in resource:
            return TablePrivilege(
                schema_name=resource['Table']['DatabaseName'],
                table_name=resource['Table']['Name'],
                principal=principal,
                permission=permission, grant=grant
            )
        elif 'TableWithColumns' in resource:
            if not resource['TableWithColumns']['ColumnWildcard']:
                return TablePrivilege(
                    schema_name=resource['TableWithColumns']['DatabaseName'],
                    table_name=resource['TableWithColumns']['Name'],
                    principal=principal,
                    permission=permission, grant=grant
                )
