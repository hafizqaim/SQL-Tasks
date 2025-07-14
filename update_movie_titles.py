import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

DB_CONFIG = {
    'user': 'qaim.ali',
    'host': 'localhost',
    'port': '5432',
    'database': 'movie_db'
}

METADATA_FILE = 'Users/qaim.ali/Downloads/movies/movies_metadata.csv' 

conn = None
try:
    print(f"Reading movie metadata from '{METADATA_FILE}'...")
    df = pd.read_csv(METADATA_FILE, low_memory=False)
    
    print("Cleaning and preparing data...")
    df['id'] = pd.to_numeric(df['id'], errors='coerce')
    df.dropna(subset=['id', 'title'], inplace=True)
    df['id'] = df['id'].astype(int)
    
    df.drop_duplicates(subset='id', keep='first', inplace=True)
    
    df = df[['id', 'title']]
    
    print(f"Found {len(df)} valid, unique movie titles to process.")

    print(f"Connecting to database '{DB_CONFIG['database']}'...")
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    print("Preparing to update titles in the database...")
    
    cursor.execute("""
        CREATE TEMP TABLE temp_titles (
            id INT PRIMARY KEY,
            title VARCHAR(255) NOT NULL
        );
    """)
    print("Temporary table 'temp_titles' created.")

    update_data = list(df.itertuples(index=False, name=None))
    execute_values(
        cursor,
        "INSERT INTO temp_titles (id, title) VALUES %s",
        update_data
    )
    print(f"Inserted {len(update_data)} records into the temporary table.")

    update_query = """
        UPDATE movies
        SET title = temp_titles.title
        FROM temp_titles
        WHERE movies.id = temp_titles.id;
    """
    print("Executing final update from temporary table...")
    cursor.execute(update_query)
    
    conn.commit()
    print(f"\nSuccessfully updated {cursor.rowcount} movie titles!")

except FileNotFoundError:
    print(f"Error: The file '{METADATA_FILE}' was not found.")
except Exception as e:
    print(f"A critical error occurred: {e}")
    if conn:
        conn.rollback()
finally:
    if conn is not None:
        cursor.close()
        conn.close()
        print("PostgreSQL connection is closed.")
