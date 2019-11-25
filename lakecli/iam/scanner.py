import os
import boto3
import logging

from lakecli.iam.orm import TablePrivilege, DatabasePrivilege, init, model_db_close


LOGGER = logging.getLogger(__name__)


class Scanner:
    def __init__(self, aws_config, path):
        self.aws_config = aws_config
        self.path = os.path.expanduser(path)
        if os.path.exists(self.path):
            LOGGER.info("Remove old sqlite database at %s" % self.path)
            os.remove(self.path)

    def scan(self):
        lake_client = boto3.client('lakeformation',
                                   region_name=self.aws_config.region,
                                   aws_access_key_id=self.aws_config.aws_access_key_id,
                                   aws_secret_access_key=self.aws_config.aws_secret_access_key)

        table_permissions = lake_client.list_permissions(
            ResourceType='TABLE',
        )

        LOGGER.debug(table_permissions)

        database_permissions = lake_client.list_permissions(
            ResourceType='DATABASE',
        )
        LOGGER.debug(database_permissions)

        table_privileges = []
        LOGGER.info("%d table permissions found." % len(table_permissions['PrincipalResourcePermissions']))
        for resource in table_permissions['PrincipalResourcePermissions']:
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

        connection = init(self.path)
        try:
            with connection.atomic():
                TablePrivilege.bulk_create(table_privileges, batch_size=100)
                DatabasePrivilege.bulk_create(db_privileges, batch_size=100)
        finally:
            model_db_close()

    @staticmethod
    def _principal(arn):
        # http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html
        elements = arn.split(':', 5)
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
