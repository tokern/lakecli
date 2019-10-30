# encoding: utf-8

import os
import logging
import sqlparse
import sqlite3

from lakecli.packages import special

logger = logging.getLogger(__name__)


class SQLExecute(object):
    TABLES_QUERY = '''
        SELECT name, sql FROM sqlite_master
        WHERE type='table'
        ORDER BY name;'''

    TABLE_COLUMNS_QUERY = '''
        SELECT 
            m.name as table_name, 
            p.name as column_name
        FROM 
            sqlite_master AS m
        JOIN 
            pragma_table_info(m.name) AS p
        ORDER BY 
        m.name, 
        p.cid
    '''

    def __init__(
        self,
        path
    ):
        self.path = os.path.expanduser(path)
        self.database = os.path.basename(path)
        self.connect()

    def connect(self, database=None):
        logger.info("Connect to %s " % self.path)
        conn = sqlite3.connect(self.path)

        if hasattr(self, 'conn'):
            self.conn.close()
        self.conn = conn

    def run(self, statement):
        '''Execute the sql in the database and return the results.

        The results are a list of tuples. Each tuple has 4 values
        (title, rows, headers, status).
        '''
        # Remove spaces and EOL
        statement = statement.strip()
        if not statement:  # Empty string
            yield (None, None, None, None)

        # Split the sql into separate queries and run each one.
        components = sqlparse.split(statement)

        for sql in components:
            # Remove spaces, eol and semi-colons.
            sql = sql.rstrip(';')

            # \G is treated specially since we have to set the expanded output.
            if sql.endswith('\\G'):
                special.set_expanded_output(True)
                sql = sql[:-2].strip()

            cur = self.conn.cursor()

            try:
                for result in special.execute(cur, sql):
                    yield result
            except special.CommandNotFound:  # Regular SQL
                cur.execute(sql)
                yield self.get_result(cur)

    def get_result(self, cursor):
        '''Get the current result's data from the cursor.'''
        title = headers = None

        # cursor.description is not None for queries that return result sets,
        # e.g. SELECT or SHOW.
        if cursor.description is not None:
            headers = [x[0] for x in cursor.description]
            rows = cursor.fetchall()
            status = '%d row%s in set' % (len(rows), '' if len(rows) == 1 else 's')
        else:
            logger.debug('No rows in result.')
            rows = None
            status = 'Query OK'
        return (title, rows, headers, status)

    def tables(self):
        '''Yields table names.'''
        with self.conn as cur:
            cur.execute(self.TABLES_QUERY)
            for row in cur:
                yield row

    def table_columns(self):
        '''Yields column names.'''
        with self.conn as cur:
            cur.execute(self.TABLE_COLUMNS_QUERY)
            for row in cur:
                yield row

    def databases(self):
        return [self.database]
