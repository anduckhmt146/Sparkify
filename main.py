from etl.create_tables import main as create_table
from etl.insert_staging import main as insert_staging
from etl.insert_tables import main as insert_tables

def main():
    create_table()
    insert_staging()

    
    insert_tables()

if __name__ == "__main__":
    main()