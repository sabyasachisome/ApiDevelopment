from utilities import connection_details as cd
from transact import extract, insert
from transform import transform_data
from configparser import ConfigParser
import pandas as pd
import os
import logging.config

"""
This class is responsible for driving the data pipeline
"""
class Pipeline(cd.DbConnection):

    logging.config.fileConfig(os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'main')), 'utilities', 'configs', 'logging.conf'))

    conf = ConfigParser()
    conf.read(os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'main')), 'config.ini'))
    conf.optionxform = str

    def __init__(self):
        super(Pipeline, self).__init__(Pipeline.conf)

    def run_pipeline(self):
        logging.info('job run started')
        job_start = pd.to_datetime('now').strftime('%Y-%m-%d %H:%M')

        logging.info('extract run started')
        extract_process = extract.ExtarctData(self.conn, Pipeline.conf)
        extract_process.fetch_attributes()
        raw_data = extract_process.extract_data()
        logging.info('extract run completed')

        logging.info('transform run started')
        transform_process= transform_data.TransformData(Pipeline.conf, raw_data)
        transform_process.clean_data()
        transform_process.transform_data()
        enriched_movie_df, numeric_cols= transform_process.enrich_data()
        logging.info('transform run completed')

        logging.info('insert run started')
        insert_process= insert.InsertData(self.conn, self.cur)
        insert_process.insert_dataframe(enriched_movie_df, numeric_cols, raw_data, job_start)
        logging.info('insert run completed')
