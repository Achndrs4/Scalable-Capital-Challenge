import logging

import duckdb

from adapter import DuckDBAdapter

if __name__ == "__main__":
    # Initialize the adapter with the configuration file
    adapter = DuckDBAdapter(config_file='config.yml')

    # Define the SQL file path with the ETL logic
    sql_file_path = 'sql/load_data.sql'  # Path to the SQL file containing the ETL logic

    # Optionally define the JSON file path
    json_file_path = 'data/dataset.json'  # Provide the actual path to the JSON file (optional)

    # Run the ETL process
    try:
        adapter.run_etl(sql_file_path, json_file_path)
    except duckdb.Error:
        logging.error("Unexpected Error")
    # Close the connection
    finally:
        adapter.close()