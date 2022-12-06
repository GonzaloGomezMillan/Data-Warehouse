import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    This function load the staging table to S3, so they can transformed to be loaded into Redshift.
    '''
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    This function load the data stored in the staging tables into the tables which has been already created for the DWH
    in Redshift.
    '''
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    '''
    Function which runs the complete code to load the data into the staging tables and afterwards to load this data into
    the Redshift DWH.
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    
    print('Code successfully run.')

    conn.close()
    
    print('Connection closed.')


if __name__ == "__main__":
    main()