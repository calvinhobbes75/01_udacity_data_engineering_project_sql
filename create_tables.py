import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """ This function connect to the postgres database and create the sparkify database used

    :return: cur database cursor
    :rtype: cursor
    :return: conn database connection
    :rtype: db connection
    """ 
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """ This function drops existing tables in sparkify database

    :param cur: database cursor
    :type cur: cursor
    :param conn: database connection
    :type conn: db connection
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """ This function creates tables for sparkify database

    :param cur: database cursor
    :type cur: cursor
    :param conn: database connection
    :type conn: db connection
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()