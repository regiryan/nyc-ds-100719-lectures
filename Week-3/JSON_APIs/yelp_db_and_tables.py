# import modules
import yelp_updated_func as y
import json
# import mysql as m
import mysql.connector
import config
import requests
import pandas as pd

db_name = y.db_name
TABLES = y.TABLES

# Set up a connection to cloud (without selecting database)
cnx = mysql.connector.connect(
    host = config.host,
    user = config.user,
    passwd = config.password,
)
# Create cursor
cursor = cnx.cursor()

# Fucntion call to create a database
y.create_database(cursor, db_name)

# Fucntion call to select database
y.select_database(db_name)

# Fucntion call to create tables
y.create_tables(TABLES)
