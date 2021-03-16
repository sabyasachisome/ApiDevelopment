import requests
import json
import pandas as pd
"""
the functions pulls the below details from omdbapi:
imdbID
Released_Date
Type
i/p: dataframe row
o/p: dataframe row
"""
def fetch_data_omdbapi(row, apiKey= '273f5165'):
    apiKey= apiKey
    data_URL = 'http://www.omdbapi.com/?apikey='+apiKey
    year = ''
    movie= row['movie_title']
    paramss = {
        't':movie,
        'y':year
    }
    resp=requests.get(data_URL,params=paramss)
    dict_strn= json.loads(resp.text)
    if (dict_strn['Response']=='True'):
        row['imdbID']= dict_strn['imdbID']
        row['Released_Date']= dict_strn['Released']
        row['Type']= dict_strn['Type']
    else:
        row['imdbID']= dict_strn['Error']
        row['Released_Date']= dict_strn['Error']
        row['Type']= dict_strn['Error']
    return row

"""
completely automated insert function- applicable to all types databses
: in params: 
df_name: name of the dataframe
table_name: name of the table where the dataframe needs to be inserted
cur, conn: cursor and conmnection objects
: out params:
no of records inserted in the table
"""
def insert_data(df_name, table_name, cur, conn):
    columns= ','.join(['"'+str(i)+'"' for i in list(df_name.columns)])
    query_placeholders= ','.join(['?']  * len(df_name.columns))
    table_name= table_name
    insert_sql= 'insert into ' + table_name + '(' + columns + ') values (' +  query_placeholders + ')'
    rec_count=0
    commit_ctr=0
    conn= conn
    for index, data in df_name.iterrows():
        try:
            cur.execute(insert_sql, tuple(data))
            rec_count+=1
#             conn.commit()
            commit_ctr+=1
            if commit_ctr==500:
                conn.commit()
                commit_ctr=0
        except Exception as e:
            print(insert_sql, " index= ", index, "data= ", tuple(data))
            print(e)
        finally:
            conn.commit()
    return rec_count