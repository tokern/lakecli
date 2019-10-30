import sqlparse
import boto3
from abc import ABC, abstractmethod


class GrantOrRevoke(ABC):
    def __init__(self, privilege_type, aws_config, tokens):
        self.privilege_type = privilege_type
        self.aws_config = aws_config

        self.privilege = []
        self.schema_name = None
        self.table_name = None
        self.principal = None
        self.grant_option = None
        self._index = 0
        self._current_token = tokens.tokens[0]
        self._token_list = tokens

    def process(self):
        self._check_simple_keyword(self.privilege_type)
        self._check_and_set_permissions()
        self._check_simple_keyword("ON")
        self._check_table_or_database_clause()
        self._check_simple_keyword("TO")
        self.principal = self._get_name()[0]

    def _check_and_set_permissions(self):
        while self._current_token and not self._current_token.match(sqlparse.tokens.Keyword, "on"):
            self.privilege.append(self._current_token.value)
            self._index, self._current_token = self._token_list.token_next(self._index)

            if self._token_list[self._index].match(sqlparse.tokens.Punctuation, ','):
                self._index, self._current_token = self._token_list.token_next(self._index)

    def _check_simple_keyword(self, keyword):
        if not self._current_token or not self._current_token.match(sqlparse.tokens.Keyword, keyword):
            raise RuntimeError("%s Keyword not found at %d" % (keyword, self._index))
        self._index, self._current_token = self._token_list.token_next(self._index)

    def _check_table_or_database_clause(self):
        is_table = self._current_token.match(sqlparse.tokens.Keyword, "table")
        is_database = self._current_token.match(sqlparse.tokens.Keyword, "database")

        if not is_table and not is_database:
            raise RuntimeError("TABLE or DATABASE keyword not found at %d" % self._index)
        self._index, self._current_token = self._token_list.token_next(self._index)

        names = self._get_name()
        if is_database:
            if len(names) != 1:
                raise RuntimeError("Database name cannot have a namespace")
            self.schema_name = names[0]

        else:
            if len(names) != 2:
                raise RuntimeError("Database name required for a table")
            self.schema_name = names[0]
            self.table_name = names[1]

    def _get_name(self):
        if not self._current_token or self._current_token.ttype != sqlparse.tokens.String.Single:
            raise RuntimeError("Name not found at %d" % self._index)

        name_1 = self._current_token.value.strip("'")
        self._index, self._current_token = self._token_list.token_next(self._index)

        if not self._current_token or not self._current_token.match(sqlparse.tokens.Punctuation, '.'):
            return [name_1]

        self._index, self._current_token = self._token_list.token_next(self._index)

        if not self._current_token or self._current_token.ttype != sqlparse.tokens.String.Single:
            raise RuntimeError("Name not found at %d" % self._index)
        name_2 = self._current_token.value.strip("'")
        self._index, self._current_token = self._token_list.token_next(self._index)

        return [name_1, name_2]

    def _get_resource(self):
        if self.table_name is None:
            return {
                'Database': {
                    'Name': self.schema_name
                }
            }
        else:
            return {
                'Table': {
                    'DatabaseName': self.schema_name,
                    'Name': self.table_name
                }
            }

    def get_payload(self):
        return {
            'CatalogId': self.aws_config.account_id,
            'Principal': {
                'DataLakePrincipalIdentifier': 'arn:aws:iam::%s:%s' % (self.aws_config.account_id, self.principal)
                },
            'Resource': self._get_resource(),
            'Permissions': self.privilege
        }

    def _get_client(self):
        return boto3.client('lakeformation',
                            region_name=self.aws_config.region,
                            aws_access_key_id=self.aws_config.aws_access_key_id,
                            aws_secret_access_key=self.aws_config.aws_secret_access_key)

    @abstractmethod
    def execute(self):
        pass


class Grant(GrantOrRevoke):
    def __init__(self, aws_config, statement):
        super(Grant, self).__init__("GRANT", aws_config, statement)

    def execute(self):
        self._get_client().grant_permissions(
            CatalogId=self.aws_config.account_id,
            Resource=self._get_resource(),
            Permissions=self.privilege,
            Principal={
                'DataLakePrincipalIdentifier': 'arn:aws:iam::%s:%s' % (self.aws_config.account_id, self.principal)
            }
        )


class Revoke(GrantOrRevoke):
    def __init__(self, aws_config, statement):
        super(Revoke, self).__init__("REVOKE", aws_config, statement)

    def execute(self):
        self._get_client().revoke_permissions(
            CatalogId=self.aws_config.account_id,
            Resource=self._get_resource(),
            Permissions=self.privilege,
            Principal={
                'DataLakePrincipalIdentifier': 'arn:aws:iam::%s:%s' % (self.aws_config.account_id, self.principal)
            }
        )
