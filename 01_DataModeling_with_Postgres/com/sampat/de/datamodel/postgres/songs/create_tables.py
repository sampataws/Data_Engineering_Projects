import psycopg2
from com.sampat.de.datamodel.postgres.songs.sql_queries import create_table_queries, drop_table_queries


def create_database(hostname="127.0.0.1",databasename="studentdb",username="student",password="student",port="5432"):
    '''Connect to Default database'''
    conn = psycopg2.connect(database=databasename, user=username, password=password, host=hostname)
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()

    # connect to sparkify database
    conn = psycopg2.connect(host=hostname,dbname="sparkifydb",user=username, password=password,port=port)
    cur = conn.cursor()

    return cur, conn


def drop_tables(cur, conn):
    '''Drops all tables created on the database'''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''Created tables defined on the sql_queries script: [songplays, users, songs, artists, time]'''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """ Function to drop and re create sparkifydb database and all related tables.
        Usage: python create_tables.py
    """
    cur, conn = create_database()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()