import duckdb

from adapter import DuckDBAdapter
import logging

if __name__ == "__main__":
    # Initialize the adapter with the configuration file
    adapter = DuckDBAdapter(config_file='config.yml')

    # Query the Database we just created in the ETL Step
    try:
        print(adapter.dataframe_from_sql_file("sql/question_1_a.sql"))
        print(adapter.dataframe_from_sql_file("sql/question_1_b.sql"))
        print(adapter.dataframe_from_sql_file("sql/question_1_c.sql"))
        print(adapter.dataframe_from_sql_file("sql/question_2.sql"))
        print(adapter.dataframe_from_sql_file("sql/question_3.sql"))
    except duckdb.Error:
        logging.error("Unexpected Error")
    # Close the connection
    finally:
        adapter.close()