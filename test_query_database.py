import unittest
import pandas as pd
import os

from query_database import (
    get_top_rated_movies,
    get_most_active_users,
    get_cast_of_movie,
    get_director_of_movie,
    search_movies_by_actor,
    find_movies_directed_by_actor,
    update_user_email,
    get_user_by_id,
    delete_rating,
    get_specific_rating,
    insert_specific_rating
)

EXPECTED_RESULTS_DIR = 'expected_results'

class TestDatabaseQueries(unittest.TestCase):

    def assert_dataframes_equal(self, actual_df, expected_df):
        """Helper function to compare two DataFrames."""
        self.assertIsNotNone(actual_df, "Query returned None, expected a DataFrame.")
        actual_df = actual_df.sort_values(by=list(actual_df.columns)).reset_index(drop=True)
        expected_df = expected_df.sort_values(by=list(expected_df.columns)).reset_index(drop=True)
        pd.testing.assert_frame_equal(actual_df, expected_df, check_dtype=False)

    def load_expected_results(self, filename):
        """Loads a CSV and handles missing values correctly."""
        expected_file = os.path.join(EXPECTED_RESULTS_DIR, filename)
        return pd.read_csv(expected_file).fillna('')

    def test_top_rated_movies(self):
        print("\nRunning test: Top Rated Movies")
        actual_results = get_top_rated_movies()
        expected_results = self.load_expected_results('expected_top_movies.csv')
        self.assert_dataframes_equal(actual_results, expected_results)
        print("Test PASSED.")

    def test_most_active_users(self):
        print("\nRunning test: Most Active Users")
        actual_results = get_most_active_users()
        expected_results = self.load_expected_results('expected_most_active_users.csv')
        self.assert_dataframes_equal(actual_results, expected_results)
        print("Test PASSED.")

    def test_toy_story_cast(self):
        print("\nRunning test: Toy Story Cast")
        actual_results = get_cast_of_movie('Toy Story')
        expected_results = self.load_expected_results('expected_toy_story_cast.csv')
        self.assert_dataframes_equal(actual_results, expected_results)
        print("Test PASSED.")

    def test_toy_story_director(self):
        print("\nRunning test: Toy Story Director")
        actual_results = get_director_of_movie('Toy Story')
        expected_results = self.load_expected_results('expected_toy_story_director.csv')
        self.assert_dataframes_equal(actual_results, expected_results)
        print("Test PASSED.")

    def test_tom_hanks_movies(self):
        print("\nRunning test: Tom Hanks Movies")
        actual_results = search_movies_by_actor('Tom Hanks')
        expected_results = self.load_expected_results('expected_tom_hanks_movies.csv')
        self.assert_dataframes_equal(actual_results, expected_results)
        print("Test PASSED.")

    def test_movies_directed_by_actor(self):
        """Tests a JOIN to find movies directed by a specific actor."""
        print("\nRunning test: Movies Directed by Tom Hanks")
        actual_results = find_movies_directed_by_actor('Tom Hanks')
        expected_results = self.load_expected_results('expected_movies_directed_by_tom_hanks.csv')
        self.assert_dataframes_equal(actual_results, expected_results)
        print("Test PASSED.")

    def test_update_user_email(self):
        """Tests an UPDATE operation by changing a user's email and verifying it."""
        print("\nRunning test: Update User Email")
        user_id_to_test = 1
        original_user_data = get_user_by_id(user_id_to_test)
        self.assertFalse(original_user_data.empty, "Could not fetch original user data.")
        original_email = original_user_data['email'].iloc[0]
        new_email = 'test.update@example.com'

        try:
            update_user_email(user_id_to_test, new_email)
            updated_user_data = get_user_by_id(user_id_to_test)
            self.assertEqual(updated_user_data['email'].iloc[0], new_email)
            print("Update verification PASSED.")
        finally:
            print("Cleaning up: reverting email change...")
            update_user_email(user_id_to_test, original_email)
            reverted_user_data = get_user_by_id(user_id_to_test)
            self.assertEqual(reverted_user_data['email'].iloc[0], original_email)
            print("Cleanup successful.")

    def test_delete_rating(self):
        """
        Tests a DELETE operation by removing a specific rating.
        This test is now self-contained and repeatable.
        """
        print("\nRunning test: Delete Rating")
        user_id = 1
        movie_id = 110
        rating_to_test = (1.0, 1425941529)

        rating_before = get_specific_rating(user_id, movie_id)
        if rating_before.empty:
            print("Test setup: Inserting rating to be deleted...")
            insert_specific_rating(user_id, movie_id, rating_to_test[0], rating_to_test[1])

        try:
            delete_rating(user_id, movie_id)
            rating_after = get_specific_rating(user_id, movie_id)
            self.assertTrue(rating_after.empty, "Rating was not deleted successfully.")
            print("Delete verification PASSED.")
        finally:
            print("Cleaning up: re-inserting deleted rating...")
            insert_specific_rating(user_id, movie_id, rating_to_test[0], rating_to_test[1])
            rating_reverted = get_specific_rating(user_id, movie_id)
            self.assertFalse(rating_reverted.empty, "Cleanup failed: rating was not re-inserted.")
            print("Cleanup successful.")

if __name__ == '__main__':
    unittest.main()
