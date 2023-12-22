import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


        
def drop_tables(cur, conn):
    """
    Drop tables in the database using the provided SQL queries.
    Parameters:
    - cur: cursor object
    - conn: connection object
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        


def create_tables(cur, conn):
    """
    Create tables in the database using the provided SQL queries.
    Parameters:
    - cur: cursor object
    - conn: connection object
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """
    Main script to connect the database also drop existing tables, and create new tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
        # Connect to the database
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()

        # Drop existing tables
        drop_tables(cur, conn)

        # Create new tables
        create_tables(cur, conn)
        

if __name__ == "__main__":
    main()
