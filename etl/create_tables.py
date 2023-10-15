import psycopg2
from config.init import conn, cur
from etl.sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error: Issue dropping table: " + query)
            print(e)
    print("Tables dropped if exists successfully.")

def create_tables(cur, conn):
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error: Issue creating table: " + query)
            print(e)
    print("Tables created successfully.")

def main():
    drop_tables(cur, conn)
    create_tables(cur, conn)


if __name__ == "__main__":
    main()
