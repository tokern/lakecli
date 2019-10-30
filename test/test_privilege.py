from lakecli.privileges.grant_or_revoke import Grant, Revoke
import sqlparse


class TestAWSConfig:
    def __init__(self):
        self.account_id = "XXXXYYYY"


def test_simple_grant():
    statement = "GRANT SELECT ON database 'taxilake' to 'user'"
    grant = Grant(TestAWSConfig(), sqlparse.parse(statement)[0])
    grant.process()
    assert grant.privilege == ['SELECT']
    assert grant.schema_name == "taxilake"
    assert grant.principal == "user"


def test_two_privileges():
    statement = "GRANT SELECT, INSERT ON database 'taxilake' to 'user'"
    grant = Grant(TestAWSConfig(), sqlparse.parse(statement)[0])
    grant.process()
    assert grant.privilege == ['SELECT', 'INSERT']
    assert grant.schema_name == "taxilake"
    assert grant.principal == "user"


def test_table_privilege():
    statement = "GRANT SELECT, INSERT ON TABLE 'taxilake'.'parq_misc' to 'user'"
    grant = Grant(TestAWSConfig(), sqlparse.parse(statement)[0])
    grant.process()
    assert grant.privilege == ['SELECT', 'INSERT']
    assert grant.schema_name == "taxilake"
    assert grant.table_name == "parq_misc"
    assert grant.principal == "user"


def test_table_revoke():
    statement = "REVOKE SELECT, INSERT ON TABLE 'taxilake'.'parq_misc' to 'user'"
    revoke = Revoke(TestAWSConfig(), sqlparse.parse(statement)[0])
    revoke.process()
    assert revoke.privilege == ['SELECT', 'INSERT']
    assert revoke.schema_name == "taxilake"
    assert revoke.table_name == "parq_misc"
    assert revoke.principal == "user"


def test_grant_payload():
    statement = "GRANT SELECT, INSERT ON TABLE 'taxilake'.'parq_misc' to 'user'"
    grant = Grant(TestAWSConfig(), sqlparse.parse(statement)[0])
    grant.process()
    payload = grant.get_payload()
    assert payload['Permissions'] == ['SELECT', 'INSERT']
    assert payload['Principal']['DataLakePrincipalIdentifier'] == "arn:aws:iam::XXXXYYYY:user"
    assert payload['Resource']['Table']['Name'] == "parq_misc"
    assert payload['Resource']['Table']['DatabaseName'] == "taxilake"


