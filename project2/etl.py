import configparser
import os
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Loads data into the staging tables."""
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Inserts data from staging tables into analytics tables."""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            config['REDSHIFT']['ENDPOINT'],
            config['REDSHIFT']['DB_NAME'],
            config['REDSHIFT']['DB_USER'],
            config['REDSHIFT']['DB_PASSWORD'],
            config['REDSHIFT']['DB_PORT']
        )
    )
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
