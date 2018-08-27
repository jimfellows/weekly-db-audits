
import glob
import os
import pandas as pd
import datetime


class SqlQuery(object):
    """
    takes filepath of SQL file and creates standard attributes
    """
    def __init__(self, filepath):
        self.filepath = filepath
        self.basename = os.path.basename(filepath)
        self.alias = os.path.splitext(self.basename)[0]
        self.ext = os.path.splitext(self.basename)[1]
        self.name = self.alias.replace(' ', '')
        self.query_str = open(filepath, 'r').read()
        self.df = 'UNQUERIED'
        self.rows = 0.0
        self.results = pd.DataFrame()
        self.summary = pd.DataFrame()
        self.success = False
        self.query_time = datetime.timedelta(seconds=0)


def get_sql_files(directory):
    """
    uses glob to get any .sql file in SQL directory
    :param directory: main dir
    :return: list of SQL file paths
    """
    sql_file_list = sorted([file for file in glob.iglob(directory+'\SQL\**\*.sql', recursive=True)])
    return sql_file_list


def get_sql_objs(directory):
    """
    uses above function to get sql files and create objs
    :param directory: main dir
    :return:
    """
    sql_file_list = get_sql_files(directory)
    return [SqlQuery(file) for file in sql_file_list]
