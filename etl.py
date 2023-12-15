import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries



def load_staging_tables(cur, conn):
    """
    Load data from staging tables in the database using the provided SQL queries.
    Parameters:
    - cur: cursor object
    - conn: connection object
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert data from the staging tables in the database using the provided SQL queries.
    Parameters:
    - cur: cursor object
    - conn: connection object
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Main ETL processes that connect to the databases,
    load_staging_tables, insert_tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    #Connect to our database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    #Load to our database
    load_staging_tables(cur, conn)
    #Insert to our database
    insert_tables(cur, conn)
    #Close our database connection
    conn.close()


if __name__ == "__main__":
    main()