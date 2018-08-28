"""---------------------------------------------------------------------------------------------------------------------
Created by J. Fellows, Data/GIS Analyst, on 5/3/2018

Description
    Script grabs any SQL file placed in //SQL directory and queries text against Remedy.  Query results are saved
    to the //RESULTS directory.  If an error occurs within a given query, the file will not be saved and an error
    message will be written to the txt file in //ERRORLOG.  CSV and txt files edited are then attached to an email
    and sent using email info in //EMAILINFO//emailinfo.csv
"""

import cx_Oracle
import os
import pandas as pd
import sqlalchemy

from datetime import datetime
from dir_helpers import create_dir
from get_sql_files import get_sql_objs
from outlook_smtp_funcs import create_email, create_server
from sqlalchemy_base import MySqlDb


# define file-related vars
datestamp = datetime.strftime(datetime.now(), "%Y%m%d")
weekstamp = datetime.strftime(datetime.now(), "%A, %b %d %Y")
cwd = os.getcwd()
err_path = create_dir(cwd + '//ERRORLOG')
err_file_path = err_path + '//errorlog.txt'
results_path = create_dir(cwd + '//RESULTS')
email_info_path = create_dir(cwd + '//EMAILINFO')
results = []

# get email info from csv into dict
email_dict = pd.read_csv(
    email_info_path + '//emailinfo.csv'
).to_dict(
    orient='records'
)[0]

# define email-related vars
to_addr = email_dict['to']
from_addr = email_dict['from']
host = 'EMAILHOST'
port = 587
uid = email_dict['uid']
pwd = email_dict['pwd']
subj_string = 'Weekly SQL Audits - ' + weekstamp
body_string = 'The following SQL audits returned results for ' + weekstamp + ':\n'
print('Sending email from ' + from_addr + ' to ' + to_addr)

# define err string, clear existing error file
error_str = ''
error_txt_f = open(err_file_path, 'w+').close()
new_txt_f = open(err_file_path, 'w')

# create sql query objects from SQL directory
print('Getting SQL files from ' + cwd + ':\n')
sql_objs = get_sql_objs(cwd)
for obj in sql_objs:
    print(obj.alias)

# initiate Sqlalchemy object
db = MySqlDb()
with db.open_cnxn():
    # loop through objects and run queries
        try:
            start = datetime.now()
            print('\nQuerying ' + query.alias + '...')
            db.query = query.query_str
            db.execute_query()
            query.query_time = datetime.now() - start
            query.success = True
            query.results = db.results
            if query.results.empty:
                body_string = body_string + '\n' + query.alias + '\nRows returned: 0' + '\nQuery Time: ' \
                              + str(query.query_time) + '\n'
            else:
                body_string = body_string + '\n' + query.alias + '\nRows returned: ' + str(query.results.shape[0]) \
                              + '\nQuery Time: ' + str(query.query_time) + '\n'

            print(query.alias + ' complete, ' + str(db.results.shape[0]) + ' rows found.\nQuery Time: '
                  + str(query.query_time))

        except (cx_Oracle.DatabaseError, sqlalchemy.exc.DatabaseError) as err:
            print('Query failed for ' + query.alias + '\nQuery Time(s): 0:00:00')
            print(str(err))
            query.success = False
            error_str = '\n\n' + error_str + query.alias + ' failed, see error below:\n\n' + str(err) + '\n'
            body_string = body_string + '\n' + query.alias + '\nRows returned: ERROR PLEASE REVIEW SQL QUERY.' \
                          + '\nQuery Time: N/A\n'
            pass

        if not query.results.empty:
            path = results_path + '//' + query.name + '.csv'
            query.results.to_csv(
                path,
                index=False
            )
            # check file size in mbs; outlook can't send attachments over 10mbs
            fsize = os.path.getsize(path)*0.000001
            print(fsize, type(fsize))
            if fsize > 10.0:
                body_string = body_string + '\n' + 'File size ' + str(fsize) + 'mb too large for Outlook, refer to ' \
                              + results_path
            else:
                results.append(path)

db.dispose_engine()

if error_str:
    new_txt_f.write(error_str)
    new_txt_f.close()
    results.append(err_file_path)

server = create_server(host, port, uid, pwd)
msg = create_email(from_addr, to_addr, subj_string, body_string, results)
server.sendmail(from_addr, to_addr, msg.as_string())
server.quit()
