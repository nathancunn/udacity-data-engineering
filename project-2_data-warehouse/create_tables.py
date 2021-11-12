import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_schemas(cur, conn):
    '''
    Function to drop schemas. This function uses the variable 'drop_schemas_queries' defined in the 'sql_queries.py' file.
    Parameters:
        - curr: Cursor for a database connection
        - conn: Database connection
    Outputs:
        None
    '''
    for query in drop_schemas_queries:
        cur.execute(query)
        conn.commit()

def drop_tables(cur, conn):
    """
    Drops all tables, as specified in `drop_table_queries`

    Args:
        cur : A psycopg2 cursor object
        conn : A psycopg2 connection object
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates the tables specified in `create_table_queries`

    Args:
        cur : A psycopg2 cursor object
        conn : A psycopg2 connection object
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This drops all schemas if they exist, and then creates them and the following tables:
    - staging_events, 
    - staging_songs, 
    - songplay, 
    - user, 
    - song, 
    - artist, 
    - time
    A cluster must be running, with configuration information stored in dwh.cfg
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()