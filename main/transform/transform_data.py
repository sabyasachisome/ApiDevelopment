import numpy as np
import pandas as pd
import requests
import json
import pandas as pd
from utilities import higher_order_functions as hoc
import logging.config
import logging
import os

"""
This class performs the below activity on the data
1. Cleaning of data
2. Enriching of data
"""
class TransformData:

    logging.config.fileConfig(os.path.join(os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')), 'utilities', 'configs','logging.conf')))
    transform_logger = logging.getLogger('logger_transform')

    def __init__(self, conf, movie_df):
        self.conf= conf
        self.numeric_cols= ['actor_1_facebook_likes', 'actor_2_facebook_likes',
      'actor_3_facebook_likes','budget', 'gross','num_critic_for_reviews',
      'num_voted_users', 'facenumber_in_poster','num_user_for_reviews',
      'title_year', 'imdb_score','duration']
        self.movie_df= movie_df

    def clean_data(self):
        self.clean_movie_df= self.movie_df.replace('NA',"").replace(r'^\s*$', np.nan, regex=True)

    """
    1. convert string columns with numeric data to float
    2. split the genre column into multiple genre columns
    """
    def transform_data(self):
        for feature in self.numeric_cols:
            self.clean_movie_df[feature] = self.clean_movie_df[feature].astype(float)

        newDf = pd.DataFrame([x.split('|') for x in self.clean_movie_df['genres'].tolist()]).fillna('')
        for col_num in newDf.columns:
            newDf.rename(columns={col_num: 'genre_id{}'.format(col_num)}, inplace=True)
        genre_cols = list(newDf.columns)
        new_movie_df = pd.concat([self.clean_movie_df, newDf], axis=1)
        key_cols = ['id', 'movie_title']
        melt_cols = key_cols + genre_cols
        new_movie_df = new_movie_df[melt_cols]
        binary_genre_df = new_movie_df.melt(id_vars=key_cols, var_name="genres")
        binary_genre_df = binary_genre_df.pivot_table(index=['id', 'movie_title'], columns='value', values='genres',
                                                      aggfunc='count').reset_index()
        binary_genre_df.drop(columns='', inplace=True)
        binary_genre_df.rename_axis(None, axis=1, inplace=True)
        binary_genre_df.fillna('False', inplace=True)
        binary_genre_df = binary_genre_df.astype(str)
        binary_genre_df = binary_genre_df.replace("1.0", 'True')
        binary_genre_df['id'] = binary_genre_df['id'].astype(int)

        final_df_with_bool_genres = pd.merge(self.clean_movie_df, binary_genre_df, on=['id', 'movie_title'], how='left')

        # TransformData.transform_logger.info(final_df_with_bool_genres.head(2))
        self.final_df_with_bool_genres= final_df_with_bool_genres

    """
    get the below items from the api:
    1. complete movie release date
    2. movie imDb id
    3. type: movie or series or documentary
    if not present: attaches the error response value
    """
    def enrich_data(self):
        apiKey= self.conf.get('omdb_api_details','apikey')

        # below line added for testing
        # self.final_df_with_bool_genres= self.final_df_with_bool_genres.head(20)
        enriched_movie_df = self.final_df_with_bool_genres.apply(lambda row: hoc.fetch_data_omdbapi(row,apiKey), axis=1)
        return enriched_movie_df, self.numeric_cols