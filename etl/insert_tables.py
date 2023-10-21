from config.init import conn, cur
from etl.sql_queries import insert_table_queries
import psycopg2


def insert_tables(cur, conn):
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error: Issue creating table: " + query)
            print(e)
    print('INSERT ALL TABLES SUCCESSFULLY')

def main():
    insert_tables(cur, conn)


if __name__ == "__main__":
    main()
