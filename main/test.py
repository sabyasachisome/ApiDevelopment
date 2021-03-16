from configparser import ConfigParser
import sqlite3

conf= ConfigParser()
conf.read('config.ini')
conf.optionxform=str

print(conf.sections())
print(conf.get('db_details','db_path'))
# print(conf.get('omdb_api_details','apiKey'))

for each_section in self.conf.sections():
    print(each_section)
    for (each_key, each_val) in self.conf.items(each_section):
        print(each_key, ':', each_val)
    print()

# conn= sqlite3.connect('movies.db')
# cursor= conn.cursor()