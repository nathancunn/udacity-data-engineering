import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Copies the data from the s3 bin specified 
    in dwh.cfg to staging tables

    Args:
        cur : A psycopg2 cursor object
        conn : A psycopg2 connection object
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Populates the star-schema table with the data 
    from the staging tables

    Args:
        cur : A psycopg2 cursor object
        conn : A psycopg2 connection object
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Populates the staging tables and then
    populates the star-schema table with the data 
    from the staging tables
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