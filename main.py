from etl.create_tables import main as create_table
from etl.insert_staging import main as insert_staging
from etl.insert_tables import main as insert_tables
from etl.pre_processing import main as pre_processing

def main():
    create_table()
    insert_staging()
    insert_tables()
    pre_processing()

if __name__ == "__main__":
    main()