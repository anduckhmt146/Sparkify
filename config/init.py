import psycopg2
from decouple import config

# Define a dictionary with the configuration
db_config = {
    "database": config("DATABASE"),
    "user": config("USERNAME"),
    "password": config("PASSWORD"),
    "host": config("HOST"),
    "port": config("PORT"),
}

# Establish a database connection
conn = psycopg2.connect(**db_config)
cur = conn.cursor()