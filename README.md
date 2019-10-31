[![CircleCI](https://circleci.com/gh/tokern/lakecli.svg?style=svg)](https://circleci.com/gh/tokern/lakecli)
[![codecov](https://codecov.io/gh/tokern/lakecli/branch/master/graph/badge.svg)](https://codecov.io/gh/tokern/lakecli)
[![PyPI](https://img.shields.io/pypi/v/lakecli.svg)](https://pypi.python.org/pypi/lakecli)
[![image](https://img.shields.io/pypi/l/lakecli.svg)](https://pypi.org/project/lakecli/)
[![image](https://img.shields.io/pypi/pyversions/lakecli.svg)](https://pypi.org/project/lakecli/)

# Introduction

LakeCLI is a SQL interface (CLI) for managing [AWS Lake Formation](https://aws.amazon.com/lake-formation/) and 
[AWS Glue](https://aws.amazon.com/glue) permissions. 

# Features

LakeCLI provides an *information schema* and supports SQL GRANT/REVOKE statements. These features help administrators
* Use familiar SQL features to view and manage permissions
* Write scripts to automate on-boarding and removing permissions.
* Write scripts to monitor & alert permissions to ensure best practices and policies are followed.

## Information Schema
LakeCLI provides two tables:

1. *database_privileges*
2. *table_privileges*

### Database Privileges
| Column | Description |
|--------|-------------|
| id | Primary Key | 
| schema_name | Name of the Schema | 
| principal | AWS IAM Role or User |
| permission | Permission type (Described in a later section) |
| grant | Boolean. Describes if the principal is allowed to grant permission to others | 

### Table Privileges
| Column | Description |
|--------|-------------|
| id | Primary Key | 
| schema_name | Schema Name of the Table | 
| table_name  | Name of the Table |
| principal | AWS IAM Role or User |
| permission | Permission type (Described in a later section) |
| grant | Boolean. Describes if the principal is allowed to grant permission to others | 

## GRANT/REVOKE Statements

    GRANT/REVOKE { { PERMISSION TYPE }
        [, ...] }
        ON { [ TABLE | DATABASE ] name }
        TO role_specification

### Permission Types

* ALL
* SELECT
* ALTER
* DROP
* DELETE
* INSERT
* CREATE_DATABASE
* CREATE_TABLE
* DATA_LOCATION_ACCESS

# Examples

## Table Privileges

    \r:iamdb> SELECT * FROM table_privileges;
    +----+-------------+----------------+--------------+------------+-------+
    | id | schema_name | table_name     | principal    | permission | grant |
    +----+-------------+----------------+--------------+------------+-------+
    | 1  | taxidata    | raw_misc       | role/lakecli | ALL        | 1     |
    | 2  | taxidata    | raw_misc       | role/lakecli | ALTER      | 1     |
    | 3  | taxidata    | raw_misc       | role/lakecli | DELETE     | 1     |
    +----+-------------+----------------+--------------+------------+-------+

## Database Privileges

    \r:iamdb> SELECT * FROM database_privileges;
    +----+-------------+--------------------------------+--------------+-------+
    | id | schema_name | principal                      | permission   | grant |
    +----+-------------+--------------------------------+--------------+-------+
    | 9  | taxilake    | role/LakeFormationWorkflowRole | CREATE_TABLE | 1     |
    | 10 | taxilake    | role/LakeFormationWorkflowRole | DROP         | 1     |
    | 11 | default     | user/datalake_user             | ALTER        | 0     |
    | 12 | default     | user/datalake_user             | CREATE_TABLE | 0     |
    | 13 | default     | user/datalake_user             | DROP         | 0     |
    +----+-------------+--------------------------------+--------------+-------+

## GRANT

    \r:iamdb> grant SELECT ON TABLE 'taxidata'.'raw_misc' TO 'user/datalake_user';
    GRANT
    Time: 1.467s
    
## REVOKE

    \r:iamdb> revoke SELECT ON TABLE 'taxidata'.'raw_misc' TO 'user/datalake_user';
    REVOKE
    Time: 1.450s

# Quick Start

## Install

``` bash
$ pip install lakecli
```

## Config

A config file is automatically created at `~/.lakecli/lakeclirc` at first launch (run lakecli). 
See the file itself for a description of all available options.

Below 4 variables are required. 

``` text
# AWS credentials
aws_access_key_id = ''
aws_secret_access_key = ''
region = '' # e.g us-west-2, us-east-1
account_id = ''
```

or you can also use environment variables:

``` bash
$ export AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY_ID
$ export AWS_SECRET_ACCESS_KEY=YOUR_SECRET_ACCESS_KEY
$ export AWS_DEFAULT_REGION=us-west-2
$ export AWS_ACCOUNT_ID=ACCOUNT_ID
```

# Features

- Auto-completes as you type for SQL keywords as well as tables and columns in the database.
- Syntax highlighting.
- Smart-completion will suggest context-sensitive completion.
    - `SELECT * FROM <tab>` will only show table names.
    - `SELECT * FROM users WHERE <tab>` will only show column names.
- Pretty prints tabular data and various table formats.
- Some special commands. e.g. Favorite queries.
- Alias support. Column completions will work even when table names are aliased.

# Usages

```bash
$ lakecli --help
Usage: lakecli [OPTIONS]

  A Athena terminal client with auto-completion and syntax highlighting.

  Examples:
    - lakecli
    - lakecli my_database

Options:
  -e, --execute TEXT            Execute a command (or a file) and quit.
  -r, --region TEXT             AWS region.
  --aws-access-key-id TEXT      AWS access key id.
  --aws-secret-access-key TEXT  AWS secretaccess key.
  --aws-account-id TEXT         Amazon Account ID.
  --lake-cli-rc FILE            Location of lake_cli_rc file.
  --profile TEXT                AWS profile
  --scan / --no-scan
  --help                        Show this message and exit.
```

# Credits

LakeCLI is based on [AthenaCLI](https://github.com/dbcli/athenacli) and the excellent [DBCli](https://www.dbcli.com/) 
project. A big thanks to all of them for providing a great foundation to build SQL CLI projects.
