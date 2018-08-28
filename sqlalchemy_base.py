"""
Created by Jim Fellows Mar. 2018
SQLAlchemyCore ia a base SQLAlchemy Connection class that uses SQLAlchemy's Core component, allowing for SQL query
capabilities over a variety of DBAPIs

Class methods and properties were created to be useful for a wide range of database types.  Queries can be executed
using SQLAlchemy's SQL API Abstraction syntax or with a raw string mimicking the exact SQL syntax required
"""

import cx_Oracle
import pandas as pd
import pyodbc
import sqlalchemy

from sqlalchemy import create_engine, Table, MetaData


class SqlALchemyCore(object):

    def __init__(self, cnxn_string):
        """
        initiation func, takes cnxn string and creates connection engine
        :param cnxn_string: string defining dsn, uid, pwd etc to DBAPI
        """
        self.cnxn_string = cnxn_string
        self._meta = MetaData()
        try:
            self.engine = create_engine(
                self.cnxn_string,
                pool_recycle=300
            )
        except sqlalchemy.exc.ArgumentError as err:
            print(str(err) + '; please review ' + self.cnxn_string)
            raise err
        self.test_cnxn()

    @property
    def cnxn(self):
        return self._cnxn

    @cnxn.setter
    def cnxn(self, cnxn):
        self._cnxn = cnxn

    @property
    def query(self):
        return self._query

    @query.setter
    def query(self, obj):
        self._query = obj

    @property
    def results(self):
        return self._results

    @results.setter
    def results(self, df):
        self._results = df

    def open_cnxn(self):
        """
        open and return cnxn object
        """
        self.cnxn = self.engine.connect()
        print(self.__class__.__name__ + ' SQL Alchemy connection opened.')
        return self.cnxn

    def close_cnxn(self):
        """
        close out cnxn object
        """
        self.cnxn.close()
        print(self.__class__.__name__ + ' SQL Alchemy connection closed.')

    def test_cnxn(self):
        """
        open and close connection to test
        """
        try:
            self.open_cnxn()
            self.close_cnxn()
            print(self.__class__.__name__ + ' SQL Alchemy connection tested successfully.')
        except (pyodbc.DataError, sqlalchemy.exc.DatabaseError, cx_Oracle.DatabaseError) as err:
            print('Unable to connect to ' + self.__class__.__name__ + ' database via:\n' + self.cnxn_string)
            print(str(err))
            raise err

    def execute_query(self):
        """
        execute query explicitly using connection object
        set results property as dataframe of results, with query keys as column headers
        """
        try:
            qresult = self.cnxn.execute(self._query)
            print('\nThe following SQL query has been executed:\n', self._query, '\n')
        except (pyodbc.DataError, sqlalchemy.exc.ResourceClosedError, NotImplementedError) as e:
            print(str(e) + ': please check that null values do not exist and data types are consistent.')
            print(self._query)
            self.dispose_engine()
            raise e
        try:
            query_keys = [k[0] for k in qresult.cursor.description]
            self.results = pd.DataFrame(qresult.fetchall(), columns=query_keys)
            qresult.close()
        except AttributeError as e:
            qresult.close()
            print(e)

    def insert_values(self, table, keyval_list):
        """
        inserts list of dictionaries as records to table specified
        :param table: table to insert on
        :param keyval_list: a list of dictionaries where each dict is a row in the table, keys define cols
        """
        try:
            self.query = table.insert(keyval_list)
            self.execute_query()
        except (pyodbc.DataError, sqlalchemy.exc.DataError, BaseException, pyodbc.ProgrammingError) as e:
            print(str(e) + ': Please check that data types are consistent.')
            self.dispose_engine()
            for d in keyval_list:
                for k, v in d.items():
                    print(k, ':', v)
            raise e

    def insert_pandas_rows(self, table_str, df):
        """
        Use pandas functionality to insert rows to table
        :param table_str: name of target table
        :param df: dataframe used to insert record(s)
        """
        try:
            df.to_sql(
                table_str,
                con=self.engine,
                if_exists='append',
                index=False
            )
        except (pyodbc.DataError, sqlalchemy.exc.DataError, BaseException, pyodbc.ProgrammingError) as e:
            self.dispose_engine()
            print(e)
            raise e

    def print_cnxn_info(self):
        """
        print pertinent cnxn info
        """
        print('Cnxn Str: ' + str(self.cnxn_string))
        print('Engine pool size: ' + str(self.engine.pool.size()))
        print('Pools checked in: ' + str(self.engine.pool.checkedin()))
        print('Pools checked out: ' + str(self.engine.pool.checkedout()))
        print('Pool overflow: ' + str(self.engine.pool.overflow()))

    def dispose_engine(self):
        """
        Close cnxn and dispose engine
        """
        if not self.cnxn.closed:
            self.close_cnxn()
        self.engine.dispose()
        print(self.__class__.__name__ + ' SQL Alchemy engine disposed')
        
    def get_all_tables(self):
        """
        get name of all tables in schema and create table object for each
        :return: dictionary where table name is k and obj is v
        """
        self.t_dict = {}
        for t in self.engine.table_names():
            t_obj = Table(
                t,
                self._meta,
                autoload=True,
                autoload_with=self.engine
            )
            self.t_dict.update({t: t_obj})
        return self.t_dict


class MySqlDb(SqlALchemyCore):
    def __init__(self):
        """
        inherit SQL Alchemy parent class and initiate tables
        """
        SqlALchemyCore.__init__(
            self,
            'mysql://UID:PWD@HOST/DBNAME?host=HOST?port=PORT#?'
        )
        self.tables = self.get_all_tables()



