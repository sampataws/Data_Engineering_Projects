import os
import glob
import psycopg2
import pandas as pd
from pathlib import Path
from com.sampat.de.datamodel.postgres.songs.sql_queries import *


def process_song_file(cur, filepath):
    """
    Process songs files and insert records into the Postgres database.

    """

    # open song file
    df = pd.DataFrame([pd.read_json(filepath, typ='series', convert_dates=False)])

    for value in df.values:
        num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year = value

        # insert artist record
        artist_data = (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
        cur.execute(artist_table_insert, artist_data)

        # insert song record
        song_data = (song_id, title, artist_id, year, duration)
        cur.execute(song_table_insert, song_data)

    print(f"Records inserted for file {filepath}")


def process_log_file(cur, filepath):
    """Reads user activity log file row by row, filters by NexSong, selects needed fields, transforms them and inserts
    them into time, user and songplay tables.

    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')

    # insert time data records
    time_data = []
    for line in t:
        time_data.append([line, line.hour, line.day, line.week, line.month, line.year, line.day_name()])
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame.from_records(time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (
        index, pd.to_datetime(row.ts, unit='ms'), int(row.userId), row.level, songid, artistid, row.sessionId,
        row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Driver function to load data from songs and event log files into Postgres database.

    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def create_conn(hostname="127.0.0.1",databasename="postgres",username="postgres",password="password",port="5432"):
    conn = psycopg2.connect(host=hostname, dbname=databasename,user=username, password=password,port=port)
    return conn



def main():
    conn = create_conn(databasename="sparkifydb")
    cur = conn.cursor()

    project_home = Path(__file__).parent.parent.parent.parent.parent.parent.parent  # This is your Project home


    process_data(cur, conn, filepath='{}/data/song_data'.format(project_home), func=process_song_file)
    process_data(cur, conn, filepath='{}/data/log_data'.format(project_home), func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()