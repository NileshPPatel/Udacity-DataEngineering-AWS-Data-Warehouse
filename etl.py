import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This procedure loads data from S3 to staging tables on Redshift
      
    INPUTS:
    * cur the cursor variable
    * conn the database connection
    """
    print("Loading data from S3 to AWS Reshift tables.")
    for query in copy_table_queries:
        print('------------------')
        print('Processing query: {}'.format(query))
        cur.execute(query)
        conn.commit()
        print('------------------')
        print('{} processed OK.'.format(query))
        
    print('All data loaded from S3 to Redshift.')


def insert_tables(cur, conn):
    """
    This procedure loads data from staging tables to analytics tables on Redshift
      
    INPUTS:
    * cur the cursor variable
    * conn the database connection 
    """
    print("Loading data from staging tables to analytics tables.")
    for query in insert_table_queries:
        print('------------------')
        print('Processing query: {}'.format(query))
        cur.execute(query)
        conn.commit()
        print('------------------')
        print('{} processed OK.'.format(query))

    print('All data loaded from staging tables to analytics tables.')

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor() 
    print("AWS Redshift connection established OK.")
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
