import os
import boto3
from lakecli.iam.orm import TablePrivilege, DatabasePrivilege, init, model_db_close


class Scanner:
    def __init__(self, aws_config, path):
        self.aws_config = aws_config
        self.path = os.path.expanduser(path)
        if os.path.exists(self.path):
            os.remove(self.path)

    def scan(self):
        lake_client = boto3.client('lakeformation',
                                   region_name=self.aws_config.region,
                                   aws_access_key_id=self.aws_config.aws_access_key_id,
                                   aws_secret_access_key=self.aws_config.aws_secret_access_key)

        table_permissions = lake_client.list_permissions(
            ResourceType='TABLE',
        )

        database_permissions = lake_client.list_permissions(
            ResourceType='DATABASE',
        )

        table_privileges = []
        for resource in table_permissions['PrincipalResourcePermissions']:
            for permission in resource['Permissions']:
                grant = permission in resource['PermissionsWithGrantOption']
                if 'Table' in resource['Resource']:
                    table_privileges.append(TablePrivilege(
                        schema_name=resource['Resource']['Table']['DatabaseName'],
                        table_name=resource['Resource']['Table']['Name'],
                        principal=resource['Principal']['DataLakePrincipalIdentifier'],
                        permission=permission, grant=grant
                    ))

        db_privileges = []
        for resource in database_permissions['PrincipalResourcePermissions']:
            for permission in resource['Permissions']:
                grant = permission in resource['PermissionsWithGrantOption']
                db_privileges.append(DatabasePrivilege(
                    schema_name=resource['Resource']['Database']['Name'],
                    principal=resource['Principal']['DataLakePrincipalIdentifier'],
                    permission=permission, grant=grant
                ))

        connection = init(self.path)
        try:
            with connection.atomic():
                TablePrivilege.bulk_create(table_privileges, batch_size=100)
                DatabasePrivilege.bulk_create(db_privileges, batch_size=100)
        finally:
            model_db_close()
