import sqlite3
import os

"""
This class gets the below connections:
sqlite3 database connection to movies db
i/p: config file from ConfigParser
o/p: connection and cursor object- object attribute
"""
class DbConnection:
    def __init__(self,conf):
        db_path= os.path.abspath(os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__),'..')),'..','data'))
        db_name= os.path.abspath('{}\{}'.format(db_path,conf.get('db_details','db_name')))
        print(db_name)
        self.conn = sqlite3.connect(db_name, isolation_level=None)
        self.cur= self.conn.cursor()