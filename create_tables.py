import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """ 
    This procedure drops all tables on the AWS redshift database which has been provided as an argument.
    
    OUTPUTS:
    * cur the cursor variable
    * conn the database connection
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error: Issue dropping table: " + query)
            print(e)

    print("Tables have been dropped successfully.")


def create_tables(cur, conn):
     """
    This procedure creates all tables on the AWS redshift database which has been provided as an argument.

    INPUTS:
    * cur the cursor variable
    * conn the database connection
    """
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error: Issue creating table: " + query)
            print(e)
            
    print("Tables have been created successfully.")

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # connect to redshift database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    # close connection to redshift database
    conn.close()


if __name__ == "__main__":
    main()
