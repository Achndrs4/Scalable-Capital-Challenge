import unittest

import adapter
import duckdb


class MyTestCase(unittest.TestCase):
    def setUp(self):
        self.adapter = adapter.DuckDBAdapter()
        # Define the SQL file path with the ETL logic
        sql_file_path = 'sql/load_data.sql'  # Path to the SQL file containing the ETL logic

        # Optionally define the JSON file path
        json_file_path = 'data/dataset.json'  # Provide the actual path to the JSON file (optional)
        self.adapter.run_etl(sql_file_path, json_file_path)

    # sanity check
    def testQ1a(self):
        q1 = self.adapter.dataframe_from_sql_file("sql/question_1_a.sql")
        self.assertEqual(len(q1), 10)
    # only one of our records is actually from this timeframe
    def testQ1b(self):
        q1b = self.adapter.dataframe_from_sql_file("sql/question_1_b.sql")
        self.assertEqual(len(q1b), 1)
    # TODO: Test the other functions

    def tearDown(self):
        self.adapter.close()
if __name__ == '__main__':
    unittest.main()
