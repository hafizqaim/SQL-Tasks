import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

DB_CONFIG = {
    'user': 'qaim.ali',
    'host': 'localhost',
    'port': '5432',
    'database': 'movie_db' 
}

SCHEMA_FILE = 'Users/qaim.ali/schema.sql'

def create_database():
    db_name = DB_CONFIG['database']
    conn = None
    try:
        temp_config = DB_CONFIG.copy()
        temp_config['database'] = 'postgres'

        conn = psycopg2.connect(**temp_config)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()

        cursor.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s"), [db_name])
        exists = cursor.fetchone()
        
        if not exists:
            print(f"Database '{db_name}' does not exist. Creating it now...")
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
            print(f"Database '{db_name}' created successfully.")
        else:
            print(f"Database '{db_name}' already exists.")
            
        cursor.close()

    except psycopg2.Error as e:
        print(f"Error while trying to create database: {e}")
        exit(1)
    finally:
        if conn is not None:
            conn.close()

def execute_sql_from_file(filepath):
    create_database()
    
    conn = None
    try:
        print(f"\nConnecting to database '{DB_CONFIG['database']}'...")
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        with open(filepath, 'r') as sql_file:
            sql_commands = sql_file.read()
            print(f"Executing SQL commands from '{filepath}'...")
            cursor.execute(sql_commands)

        conn.commit()
        print("\nAll tables created successfully!")

    except psycopg2.Error as e:
        print(f"An error occurred: {e}")
    finally:
        if conn is not None:
            cursor.close()
            conn.close()
            print("PostgreSQL connection is closed.")

if __name__ == "__main__":
    execute_sql_from_file(SCHEMA_FILE)
