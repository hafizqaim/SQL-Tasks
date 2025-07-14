import psycopg2
import pandas as pd

# --- Database Connection Details for PostgreSQL ---
DB_CONFIG = {
    'user': 'qaim.ali',
    'host': 'localhost',
    'port': '5432',
    'database': 'movie_db'
}

def run_query(query, params=None):
    """
    Connects to the database, executes a given query, and returns the results
    as a pandas DataFrame for easy viewing.
    """
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        df = pd.read_sql_query(query, conn, params=params)
        return df
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    finally:
        if conn:
            conn.close()

def run_commit_query(query, params=None):
    """
    Connects to the database, executes a command that modifies data (UPDATE, DELETE),
    and commits the change.
    """
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute(query, params)
        conn.commit()
        print("Commit successful.")
    except Exception as e:
        print(f"An error occurred during commit query: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

# --- SELECT Queries ---

def get_top_rated_movies(min_ratings=1000):
    print(f"\n--- 1. Finding Top 10 Highest-Rated Movies (with at least {min_ratings} ratings) ---")
    query = """
        SELECT m.title, COUNT(r.rating) AS num_ratings, AVG(r.rating) AS avg_rating
        FROM movies m JOIN ratings r ON m.id = r.movie_id
        GROUP BY m.title HAVING COUNT(r.rating) > %s
        ORDER BY avg_rating DESC LIMIT 10;
    """
    return run_query(query, params=(min_ratings,))

def get_most_active_users():
    print("\n--- 2. Finding Top 10 Most Active Users ---")
    query = """
        SELECT u.name, u.email, COUNT(r.user_id) AS ratings_count
        FROM "users" u JOIN ratings r ON u.id = r.user_id
        GROUP BY u.id, u.name, u.email ORDER BY ratings_count DESC LIMIT 10;
    """
    return run_query(query)

def get_cast_of_movie(movie_title):
    print(f"\n--- 3. Finding Cast of '{movie_title}' ---")
    query = """
        SELECT c.name, c.character_name, c.cast_order
        FROM "cast" c JOIN movies m ON c.movie_id = m.id
        WHERE LOWER(m.title) = LOWER(%s) ORDER BY c.cast_order LIMIT 15;
    """
    return run_query(query, params=(movie_title,))

def get_director_of_movie(movie_title):
    print(f"\n--- 4. Finding Director(s) of '{movie_title}' ---")
    query = """
        SELECT cr.name, cr.job FROM crew cr JOIN movies m ON cr.movie_id = m.id
        WHERE LOWER(m.title) = LOWER(%s) AND cr.job = 'Director';
    """
    return run_query(query, params=(movie_title,))

def search_movies_by_actor(actor_name):
    print(f"\n--- 5. Finding Movies Starring '{actor_name}' ---")
    query = """
        SELECT m.title, c.character_name FROM movies m JOIN "cast" c ON m.id = c.movie_id
        WHERE c.name = %s ORDER BY m.title;
    """
    return run_query(query, params=(actor_name,))

def find_movies_directed_by_actor(actor_name):
    print(f"\n--- 6. Finding Movies Directed by '{actor_name}' ---")
    query = """
        SELECT m.title FROM movies m JOIN crew cr ON m.id = cr.movie_id
        WHERE cr.job = 'Director' AND cr.name = %s;
    """
    return run_query(query, params=(actor_name,))

def get_user_by_id(user_id):
    return run_query('SELECT id, name, email FROM "users" WHERE id = %s;', params=(user_id,))

def get_specific_rating(user_id, movie_id):
    return run_query("SELECT * FROM ratings WHERE user_id = %s AND movie_id = %s;", params=(user_id, movie_id))

# --- UPDATE, DELETE, and INSERT Queries ---

def update_user_email(user_id, new_email):
    print(f"\n--- UPDATING email for user ID {user_id} ---")
    run_commit_query('UPDATE "users" SET email = %s WHERE id = %s', (new_email, user_id))

def delete_rating(user_id, movie_id):
    print(f"\n--- DELETING rating for user {user_id} on movie {movie_id} ---")
    run_commit_query("DELETE FROM ratings WHERE user_id = %s AND movie_id = %s", (user_id, movie_id))

def insert_specific_rating(user_id, movie_id, rating, timestamp):
    """Inserts a specific rating into the database."""
    print(f"\n--- INSERTING rating for user {user_id} on movie {movie_id} ---")
    query = "INSERT INTO ratings (user_id, movie_id, rating, timestamp) VALUES (%s, %s, %s, %s);"
    run_commit_query(query, (user_id, movie_id, rating, timestamp))

