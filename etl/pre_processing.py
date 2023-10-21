from config.init import conn, cur
from etl.sql_queries import pre_processing
import psycopg2


def preprocessing(cur, conn):
    for query in pre_processing:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error in query: " + query)
            print(e)
    print('PREPROCESSING TABLES SUCCESSFULLY')

def main():
    preprocessing(cur, conn)


if __name__ == "__main__":
    main()
