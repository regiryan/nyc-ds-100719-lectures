import yelp_updated_func as y
import mysql.connector
import config
from mysql.connector import errorcode
import requests
import pandas as pd
import json


db_name = y.db_name
bus_url_params = y.my_url_params
# bus_table_tuple = y.bus_table_tuple
bus_table_query = y.bus_table_query
my_api_key = y.my_api_key


cnx = mysql.connector .connect(
    host = config.host,
    user = config.user,
    passwd = config.password,
    database = db_name
)

cursor = cnx.cursor(buffered=True)



# bus_table_tuple = y.create_tuple(response_dict_key)

y.all_results(bus_url_params , my_api_key, bus_table_query)
