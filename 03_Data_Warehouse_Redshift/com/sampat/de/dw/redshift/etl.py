import configparser
import psycopg2
from com.sampat.de.dw.redshift.sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
        Description: Copies data in json format in S3 to staging tables in redshift.
        Arguments:
            cur: the cursor object.
            conn: connection object to redshift.
        Returns:
            None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
       Description: ETL from staging tables to songplays fact and its dimension
                    tables in redshift.
       Arguments:
           cur: the cursor object.
           conn: connection object to redshift.
       Returns:
           None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
        Description:
            - Establishes connection with the sparkify database and gets
            cursor to it (on AWS redshift cluster created earlier).

            - Loads staging tables from raw log and song files to redshift database

            - From staging tables, perform ETL to songplays fact and its dimension
              tables in redshift using SQL

        Returns:
            None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()