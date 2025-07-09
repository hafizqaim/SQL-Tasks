import pandas as pd
import psycopg2
import ast

DB_CONFIG = {
    'user': 'qaim.ali',
    'host': 'localhost',
    'port': '5432',
    'database': 'movie_db'
}

CREDITS_FILE = 'Users/qaim.ali/Downloads/movies/credits.csv' 

def populate_data():
    conn = None
    try:
        print(f"Reading data from '{CREDITS_FILE}'...")
        df = pd.read_csv(CREDITS_FILE, on_bad_lines='warn')
        print(f"Found {len(df)} movies to process.")

        print(f"Connecting to database '{DB_CONFIG['database']}'...")
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        for index, row in df.iterrows():
            movie_id = row['id']
            
            if not isinstance(movie_id, (int, float)) or pd.isna(movie_id):
                print(f"Skipping row {index} due to invalid movie ID: {movie_id}")
                continue
            
            movie_id = int(movie_id)
            print(f"Processing movie ID: {movie_id}")

            movie_title = f"Title for movie {movie_id}"
            try:
                cursor.execute(
                    "INSERT INTO movies (id, title) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING",
                    (movie_id, movie_title)
                )
            except Exception as e:
                print(f"  - Error inserting into movies table for ID {movie_id}: {e}")
                conn.rollback()
                continue

            try:
                cast_data = ast.literal_eval(row['cast'])
                for member in cast_data:
                    cursor.execute(
                        """INSERT INTO "cast" (movie_id, cast_id, character_name, credit_id, gender, name, cast_order, profile_path)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                        (movie_id, member.get('cast_id'), member.get('character'), member.get('credit_id'), 
                         member.get('gender'), member.get('name'), member.get('order'), member.get('profile_path'))
                    )
            except (ValueError, SyntaxError) as e:
                print(f"  - Could not parse cast data for movie ID {movie_id}. Error: {e}")
            except Exception as e:
                print(f"  - Error inserting into cast table for movie ID {movie_id}: {e}")
                conn.rollback()

            try:
                crew_data = ast.literal_eval(row['crew'])
                for member in crew_data:
                    cursor.execute(
                        """INSERT INTO crew (movie_id, credit_id, department, gender, job, name, profile_path)
                           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                        (movie_id, member.get('credit_id'), member.get('department'), member.get('gender'),
                         member.get('job'), member.get('name'), member.get('profile_path'))
                    )
            except (ValueError, SyntaxError) as e:
                print(f"  - Could not parse crew data for movie ID {movie_id}. Error: {e}")
            except Exception as e:
                print(f"  - Error inserting into crew table for movie ID {movie_id}: {e}")
                conn.rollback()

        conn.commit()
        print("\nData population for movies, cast, and crew completed successfully!")

    except FileNotFoundError:
        print(f"Error: The file '{CREDITS_FILE}' was not found.")
    except Exception as e:
        print(f"A critical error occurred: {e}")
    finally:
        if conn is not None:
            cursor.close()
            conn.close()
            print("PostgreSQL connection is closed.")

if __name__ == "__main__":
    populate_data()
