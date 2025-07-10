import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from psycopg2.extensions import register_adapter, AsIs
from faker import Faker
import numpy

DB_CONFIG = {
    'user': 'qaim.ali',
    'host': 'localhost',
    'port': '5432',
    'database': 'movie_db'
}

RATINGS_FILE = 'Users/qaim.ali/Downloads/movies/ratings.csv' 

def register_numpy_types():
    def addapt_numpy_float64(numpy_float64):
        return AsIs(numpy_float64) 
    def addapt_numpy_int64(numpy_int64):
        return AsIs(numpy_int64)
    register_adapter(numpy.float64, addapt_numpy_float64)
    register_adapter(numpy.int64, addapt_numpy_int64)


def populate_users_and_ratings():
    register_numpy_types()
    
    conn = None
    try:
        print(f"Reading data from '{RATINGS_FILE}'...")
        df = pd.read_csv(RATINGS_FILE, dtype={'userId': int, 'movieId': int, 'rating': float, 'timestamp': int})
        print(f"Found {len(df)} rating entries to process.")

        print(f"Connecting to database '{DB_CONFIG['database']}'...")
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        print("\nSynchronizing movies with ratings data...")
        cursor.execute("SELECT id FROM movies")
        existing_movie_ids = {row[0] for row in cursor.fetchall()}
        print(f"Found {len(existing_movie_ids)} movies already in the database.")

        ratings_movie_ids = set(df['movieId'].unique())
        print(f"Found {len(ratings_movie_ids)} unique movies in the ratings file.")

        missing_movie_ids = ratings_movie_ids - existing_movie_ids
        
        if missing_movie_ids:
            print(f"Found {len(missing_movie_ids)} movies in ratings that are not in the movies table. Inserting them now...")
            movies_to_insert = [(movie_id, f"Title for movie {movie_id}") for movie_id in missing_movie_ids]
            execute_values(
                cursor,
                "INSERT INTO movies (id, title) VALUES %s ON CONFLICT (id) DO NOTHING",
                movies_to_insert
            )
            print("Missing movies have been added with placeholder titles.")
        else:
            print("All movies from ratings file are already present in the database.")


        print("\nPreparing to populate 'users' table...")
        unique_user_ids = df['userId'].unique()
        fake = Faker()
        users_to_insert = []
        for user_id in unique_user_ids:
            name = fake.name()
            email = f"{name.lower().replace(' ', '.')}{user_id}@example.com"
            users_to_insert.append((user_id, name, email))
        
        print(f"Found {len(users_to_insert)} unique users. Inserting into database...")
        execute_values(
            cursor,
            'INSERT INTO "users" (id, name, email) VALUES %s ON CONFLICT (id) DO NOTHING',
            users_to_insert
        )
        print("'users' table populated successfully.")

        print("\nPreparing to populate 'ratings' table...")
        ratings_to_insert = list(df[['userId', 'movieId', 'rating', 'timestamp']].itertuples(index=False, name=None))
        print(f"Inserting {len(ratings_to_insert)} ratings into database. This may take a moment...")
        execute_values(
            cursor,
            "INSERT INTO ratings (user_id, movie_id, rating, timestamp) VALUES %s",
            ratings_to_insert
        )
        print("'ratings' table populated successfully.")

        conn.commit()
        print("\nData population for users and ratings completed!")

    except FileNotFoundError:
        print(f"Error: The file '{RATINGS_FILE}' was not found.")
    except Exception as e:
        print(f"A critical error occurred: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn is not None:
            cursor.close()
            conn.close()
            print("PostgreSQL connection is closed.")

if __name__ == "__main__":
    populate_users_and_ratings()
