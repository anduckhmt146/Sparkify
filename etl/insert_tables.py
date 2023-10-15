from config.init import conn, cur
from etl.sql_queries import insert_table_queries


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
    print('INSERT ALL TABLES SUCCESSFULLY')

def main():
    insert_tables(cur, conn)


if __name__ == "__main__":
    main()
