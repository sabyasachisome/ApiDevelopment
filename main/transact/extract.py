import pandas as pd
import os
import logging.config
import logging

"""
This class extracts the raw data from movies db
"""
class ExtarctData:
    logging.config.fileConfig(os.path.join(os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')), 'utilities', 'configs','logging.conf')))
    transform_extract = logging.getLogger('logger_transact')

    def __init__(self, conn, conf):
        self.conn= conn
        self.conf= conf

    def fetch_attributes(self):
        excel_path = os.path.abspath(os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')), '..', 'data'))
        excel_name = os.path.abspath('{}\{}'.format(excel_path, self.conf.get('db_details', 'attribute_excel_name')))
        attribute_df= pd.read_excel(excel_name)
        self.attribute_names= list(attribute_df.FIELD)

    def extract_data(self):
        movies_df= pd.read_sql("select * from movies", self.conn)
        movies_df= movies_df[self.attribute_names]
        return movies_df