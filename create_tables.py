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
    Main ETL processes that connect to the databases,
    drop_tables, create_tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    try:    
        #Connect to our database
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
        
        #Drop tables
        drop_tables(cur, conn)
            
        #Create tables
        create_tables(cur, conn)

   except Exception as e:
      print("Error:", e)

   finally:
        #Close our database connection
         if conn is not None:  
             conn.close()


if __name__ == "__main__":
    main()
