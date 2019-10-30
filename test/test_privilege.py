from lakecli.privileges.grant import Grant
import sqlparse


def test_simple_grant():
    statement = "GRANT SELECT ON database 'taxilake' to 'user'"
    grant = Grant(sqlparse.parse(statement)[0])
    grant.process()
    print(grant.__dict__)
    assert grant.privilege == ['SELECT']
    assert grant.schema_name == "'taxilake'"
    assert grant.principal == "'user'"

