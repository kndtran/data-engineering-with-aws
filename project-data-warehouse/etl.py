import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Load tables from S3 to Redshift.

    Parameters
    ----------
    cur: database cursor
    conn: database connection
    
    Returns
    -------
    None

    """
    for idx, query in enumerate(copy_table_queries):
        print(idx, len(copy_table_queries), query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Load tables staging to dimension and fact tables.

    Parameters
    ----------
    cur: database cursor
    conn: database connection
    
    Returns
    -------
    None

    """
    for idx, query in enumerate(insert_table_queries):
        print(idx, len(insert_table_queries), query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()