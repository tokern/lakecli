from lakecli.iam.scanner import Scanner


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
