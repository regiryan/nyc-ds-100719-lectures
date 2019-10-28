# Imrpot modules
import mysql.connector
import config
from mysql.connector import errorcode
import requests
import pandas as pd
import json

# Create a connection
cnx = mysql.connector.connect(
    host = config.host,
    user = config.user,
    passwd = config.password,
)
# Create cursor
cursor = cnx.cursor()



# Define variables
db_name = 'TEST0'
my_url_params = {'location':'Manhattan', 'categories':'massage, All', 'sort_by':'review_count', 'limit':50}
my_api_key = 'hCKY3B7kZZDUIkc7JiW5RM3RluzA562cAFztq2zGnaoax9bHYkZ8S_2vGkmg5FQ5snTcdhjN1TvMzjX89sLbLMmcN-n-APL3986KpquzNcSnhImY0GOrzXm25OaxXXYx'
# a tuple of column names from a yelp call correspondindg to columns in 'businesses' table
# bus_table_tuple = (business['id'], business['alias'], business['name'], business['review_count'], business['categories'][0]['alias'], business['rating'], business['location']['city'])
bus_table_query = """INSERT INTO businesses
    (business_id, alias, name, review_count, categories, rating, city)
    VALUES (%s, %s, %s, %s, %s, %s, %s)"""



# Function to create database (with catching errors)
def create_database(cursor, database):
    try:
        cursor.execute(
            "CREATE DATABASE {}".format(database))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        pass

# Function to select a database (if db does not exist create it)
def select_database(database):
    try:
        cursor.execute("USE {}".format(database))
    except mysql.connector.Error as err:
        print("Database {} does not exist.".format(database))
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            create_database(cursor, db_name)
            print("Database {} created successfully.".format(database))
            cnx.database = db_name
        else:
            print(err)
            exit(1)

# Function to create all tables in defined schema TABLES
def create_tables(tables_dict):
    for table_name in tables_dict:
        table_description = tables_dict[table_name]
        try:
            print("Creating table {}: ".format(table_name), end='')
            cursor.execute(table_description)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
        else:
            print("OK")

# Create var tuple for yelp reults ['businesses']
def create_bus_tuple(response_dict_key):
    return (response_dict_key['id'], response_dict_key['alias'], response_dict_key['name'], response_dict_key['review_count'],
            response_dict_key['categories'][0]['alias'], response_dict_key['rating'], response_dict_key['location']['city'])

# Function to get a json data file from yelp
def yelp_call(url_params, api_key):
    url = 'https://api.yelp.com/v3/businesses/search'
    headers = {'Authorization': 'Bearer {}'.format(api_key)}
    response = requests.get(url, headers=headers, params=url_params)
    data = response.json()
    return data

# Function to parse results based on a given tuple(list of column headers corresponding to a table)
def parse_results(results, table_tuple):
    parsed_results = []
    for business in results['businesses']:
        parsed_results.append(table_tuple)
    return parsed_results

# Function to insert results into a table based on the given query
def db_insert(parsed_results, table_qry):
    for n in range(0, len(results)):
        cursor.execute(table_qry, results[n])
        print(f'inserted a row for {n} row in results')

# Function tu pull results into a table
def all_results(url_params, api_key, table_qry):
    # Returns a json file with 50 entries
    data = yelp_call(url_params, api_key)
    # sets the upper limit of the loop to the total results for the entire query
    num = data['total']
    print('{} total matches found.'.format(num))
    data_key = data['businesses']
    print(str(type(data_key))
    print(f'this is the data_key')
    table_tuple = create_bus_tuple(data_key)
    print(f' this is the table tuple : {table_tuple}')
    cur = 0

    while cur < num and cur < 1000:
        url_params['offset'] = cur
        # Returns a json file with 50 entries
        results = yelp_call(url_params, api_key)
        print(f'Pulled 50 results')
        # Returns a list of tuples to insert to table
        parsed_results = parse_results(results, table_tuple)
        print(f'parsed results and saved them to a list')
        try:
            # inserts into database by looping through a list of tuples(rows and a specified table query)
            print(f'inserting the 50 redults into my table')
            db_insert(parsed_results, table_qry)
        except:
            Exception
            print(Exception)
        else:
            cur += 50
        # time.sleep(1) #Wait a second
#          for yelp you dont really need to wait a second



# Define schema for tables
TABLES = {}

TABLES['Businesses'] = ("""
CREATE TABLE IF NOT EXISTS businesses (
    business_id VARCHAR(255) NOT NULL PRIMARY KEY,
    alias VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    review_count SMALLINT NOT NULL,
    categories TEXT NOT NULL,
    rating FLOAT NOT NULL,
    city VARCHAR(255) NOT NULL
)  ENGINE=INNODB;
""")

TABLES['Reviews'] = ("""
CREATE TABLE IF NOT EXISTS reviews (
    business_id VARCHAR(255) NOT NULL PRIMARY KEY,
    total_reviews SMALLINT NOT NULL,
    review_text TEXT NOT NULL,
    time_created DATE NOT NULL,
    username VARCHAR(255) NOT NULL
 ) ENGINE=INNODB;
""")
