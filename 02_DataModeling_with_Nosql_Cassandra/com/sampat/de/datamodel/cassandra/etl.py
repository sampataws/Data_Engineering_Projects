# Import Python packages
import os
import glob
import csv
import logging


# Get your current folder and subfolder event data
from pathlib import Path

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

project_home = Path(__file__).parent.parent.parent.parent.parent.parent

# create file handler which logs even debug messages
fh = logging.FileHandler(str(project_home)+'/log/app_log.log','w+')
fh.setLevel(logging.DEBUG)
# create console handler with a higher log level
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)

logger.info("The project home is -> {}".format(project_home))

filepath = str(project_home) + "/data/event_data/"

# Create a for loop to create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):
    # join the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(root, '*'))
    logger.info(file_path_list)

# initiating an empty list of rows that will be generated from each file
full_data_rows_list = []

# for every filepath in the file path list
for f in file_path_list:

    # reading csv file
    with open(f, 'r', encoding='utf8', newline='') as csvfile:
        # creating a csv reader object
        csvreader = csv.reader(csvfile)
        logger.info("CSV file -> {} successfully read !!!".format(csvfile))
        next(csvreader)

        # extracting each data row one by one and append it
        for line in csvreader:
            full_data_rows_list.append(line)

# logging total number of rows available
logger.info("The total number of rows to be processed are : {}".format(len(full_data_rows_list)))
# uncomment the code below if you would like to check to see what the list of event data rows will look like
#print(full_data_rows_list)

# creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
# Apache Cassandra tables
csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding='utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist', 'firstName', 'gender', 'itemInSession', 'lastName', 'length', \
                     'level', 'location', 'sessionId', 'song', 'userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

# check the number of rows in your csv file
with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    logger.info("The total number of rows created in new data event file is -> {}".format(sum(1 for line in f)))

"""
THE EVENT DATA FILE CONSISTS OF THE FOLLOWING COLUMNS:

* artist
* firstName of user
* gender of user
* item number in session
* last name of user
* length of the song
* level (paid or free song)
* location of the user
* sessionId
* song title
* userId

"""

#sample data from the event_datafile_new.csv

import pandas as pd
event_df = pd.read_csv("event_datafile_new.csv")
logger.info("Below are sample event data")
logger.info(event_df.head(5))


""" CREATING CASSANDRA CLUSTER """

# This should make a connection to a Cassandra instance your local machine
# (127.0.0.1)

from cassandra.cluster import Cluster
cluster = Cluster()

# To establish connection and begin executing queries, need a session
session = cluster.connect()

"""Create KeySpace """
logger.info("Creating a KeySpace with name -> sparkify_db")
try:
    session.execute('''
    CREATE KEYSPACE IF NOT EXISTS sparkify_db
    WITH REPLICATION =
    {'class': 'SimpleStrategy', 'replication_factor': 1}
    ''')
except Exception as e:
    print(e)

"""Set KeySpace"""

logger.info("Setting the current session kyespace to -> sparkify_db")
session.set_keyspace('sparkify_db')

"""
With Apache Cassandra database tables are modelled on the queries we want to run.¶

-- In this notebook, we look at 3 hypothetical scenarios. For each scenario, we model a data table after the query we plan to execute

1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4

2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182

3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
"""

"""IMPLEMENTATION OF CASE 1"""

## TO-DO: Query 1:  Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4

# Sample query that is expected to run is as mentioned below :

'''
CASE1:
-----
SELECT artist, song_title, song_length FROM session_songs_item_details WHERE sessionId = 338 AND itemInSession = 4
'''

logger.info("Creating table with name -> session_songs_item_details ")

query = """ 
            CREATE TABLE IF NOT EXISTS session_songs_item_details 
            (sessionId int, itemInSession int, artist text, song_title text, song_length float,
            PRIMARY KEY(sessionId, itemInSession)) 
        """
logger.info("Executing query for session_songs_item_details table -> {}".format(query))
try:
    session.execute(query)
except Exception as e:
    logger.exception(e)

file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
## Assign the INSERT statements into the `query` variable
        insert_query = "INSERT INTO session_songs_item_details(sessionId, itemInSession, artist, song_title, song_length)"
        insert_query = insert_query + "VALUES (%s, %s, %s, %s, %s)"
        ## Assign which column element should be assigned for each column in the INSERT statement.
        session.execute(insert_query, (int(line[8]), int(line[3]), line[0], line[9], float(line[5])))

select_query_for_case_1 = """
                            SELECT artist, song_title, song_length FROM session_songs_item_details
                            WHERE sessionId = %s 
                            AND itemInSession = %s 
                          """

## Add in the SELECT statement to verify the data was entered into the table
try:
    rows = session.execute(select_query_for_case_1, (338, 4))
except Exception as e:
    logger.exception(e)

for row in rows:
    logger.info("Artist: " + row.artist, "\nSong Title: " + row.song_title, "\nSong Length: " + str(row.song_length))
    print("Artist: " + row.artist, "\nSong Title: " + row.song_title, "\nSong Length: " + str(row.song_length))

'''
CASE2:
-----
SELECT artist_name, song_title, user_first_name, user_last_name FROM user_session_details WHERE user_id = 10 AND session_id = 182

'''

logger.info("Creating table with name -> user_session_details ")

query = """ 
            CREATE TABLE IF NOT EXISTS user_session_details
            (user_id INT, session_id INT, item_in_session INT,artist_name TEXT, song_title TEXT, user_first_name TEXT,user_last_name TEXT,
            PRIMARY KEY((user_id, session_id), item_in_session)) 
        """
logger.info("Executing query for user_songs_details table -> {}".format(query))

try:
    session.execute(query)
except Exception as e:
    logger.exception(e)

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
## Assign the INSERT statements into the `query` variable
        insert_query = "INSERT INTO user_session_details(user_id, session_id, item_in_session, artist_name,song_title, user_first_name, user_last_name)"
        insert_query = insert_query + "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        ## Assign which column element should be assigned for each column in the INSERT statement.
        session.execute(insert_query, (int(line[10]), int(line[8]), int(line[3]), line[0], line[9], line[1], line[4]))

select_query_for_case_2 = """ SELECT artist_name, song_title, user_first_name, user_last_name FROM user_session_details
                              WHERE user_id = %s
                              AND session_id = %s 
                          """
try:
    rows = session.execute(select_query_for_case_2, (10, 182))
except Exception as e:
    logger.exception(e)
for row in rows:
    logger.info("Artist: " + row.artist_name, "\nSong Title: " + row.song_title, "\nUser First Name: " + row.user_first_name,
          "\nUser Last Name: " + row.user_last_name)
    print("Artist: " + row.artist_name, "\nSong Title: " + row.song_title, "\nUser First Name: " + row.user_first_name,
          "\nUser Last Name: " + row.user_last_name)

'''
CASE3:
-----
SELECT user_first_name, user_last_name FROM user_song_details WHERE song_title = 'All Hands Against His Own'

'''

logger.info("Creating table with name -> user_song_details ")

query = """ 
            CREATE TABLE IF NOT EXISTS user_song_details
            (song_title TEXT, user_id INT, user_first_name TEXT, user_last_name TEXT, 
                                        PRIMARY KEY ((song_title), user_id)) 
        """
logger.info("Executing query for user_songs_details table -> {}".format(query))

try:
    session.execute(query)
except Exception as e:
    logger.exception(e)

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        insert_query = "INSERT INTO user_songs_details(song_title, user_id, user_first_name, user_last_name)"
        insert_query = insert_query + "VALUES (%s, %s, %s, %s)"
        ## Assign which column element should be assigned for each column in the INSERT statement.
        session.execute(insert_query, (line[9], int(line[10]), line[1], line[4]))

select_query_for_case_3 = """ 
                            SELECT user_first_name, user_last_name FROM user_songs_details WHERE song_title = %s; 
                          """
logger.info("The SELECT query is as -> {}".format(select_query_for_case_3))

try:
    rows = session.execute(select_query_for_case_3, ('All Hands Against His Own', ))
except Exception as e:
    logger.exception(e)

for row in rows:
    logger.info("User First Name: "+row.user_first_name, "\nUser Last Name: "+row.user_last_name)
    print("User First Name: "+row.user_first_name, "\nUser Last Name: "+row.user_last_name)

# DROPPING tables

logger.info("Dropping tabless [session_songs_item_details,user_session_details,user_songs_details]")

session.execute("DROP TABLE IF EXISTS session_songs_item_details")
session.execute("DROP TABLE IF EXISTS user_session_details")
session.execute("DROP TABLE IF EXISTS user_songs_details")

logger.info("Closing session")

session.shutdown()
cluster.shutdown()
