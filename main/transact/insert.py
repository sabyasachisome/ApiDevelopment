from utilities import higher_order_functions as hoc
import pandas as pd
import logging.config
import logging
import os

class InsertData:
    logging.config.fileConfig(os.path.join(os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')), 'utilities', 'configs', 'logging.conf')))
    insert_logger = logging.getLogger('logger_transact')

    def __init__(self, conn, cur):
        self.conn = conn
        self.cur= cur

    def insert_dataframe(self,final_movie_df, cols, raw_movie_df, job_start):
        for feature in cols:
            final_movie_df[feature] = final_movie_df[feature].fillna(-1).astype(int).astype(str).replace("-1.0", "")

        InsertData.insert_logger.info('starting truncate load in clean table')
        self.cur.execute("""delete from movies_cleaned""")
        self.conn.commit()
        InsertData.insert_logger.info('truncate of clean table completed')
        InsertData.insert_logger.info('starting refresh of clean table with updated omdb data')
        rec_inserted = hoc.insert_data(final_movie_df, 'movies_cleaned', self.cur, self.conn)
        InsertData.insert_logger.info('Completed data truncate load into main clean table, {} records inserted'.format(rec_inserted))
        try:
            records_extracted_raw = len(raw_movie_df.index)
            records_ingested_cleaned = rec_inserted
            job_end = pd.to_datetime('now').strftime('%Y-%m-%d %H:%M')
            latest_movie_date = max(pd.to_datetime(final_movie_df.Released_Date.replace('N/A', '01 Jan 1800').replace('Movie not found!','01 Jan 1800')))

            dataa = [job_start, job_end, records_extracted_raw, records_ingested_cleaned, latest_movie_date]
            columnss = ['job_starttime', 'job_endtime', 'records_extracted_raw', 'records_ingested_cleaned',
                        'latest_movie_date']
            audit_dataframe = pd.DataFrame(data=[dataa], columns=columnss)
            audit_dataframe= audit_dataframe.astype(str)

            audit_rec_inserted = hoc.insert_data(audit_dataframe, 'audit_movies_cleaned', self.cur, self.conn)
        except Exception as e:
            InsertData.insert_logger.error(e)
            InsertData.insert_logger.error(e)
            InsertData.insert_logger.error(dataa.latest_movie_date)
            InsertData.insert_logger.error(dataa.head(1))
        InsertData.insert_logger.info('Completed data append load into audit table, {} record inserted'.format(audit_rec_inserted))


