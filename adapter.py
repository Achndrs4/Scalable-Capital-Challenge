import logging
import duckdb
import yaml

class DuckDBAdapter:
    def __init__(self, config_file: str = None):
        """
        Initialize the DuckDBAdapter with a connection to a disk-based database
        and optional configuration.

        :param config_file: Path to a YAML configuration file. Defaults to None.
        """
        self.config = self.load_config(config_file) if config_file else {}
        self.db_path = self.config.get('db_file_location', 'test.db')
        self.connection = duckdb.connect(database=self.db_path)

        # Apply configurations (like threads, memory limit, etc.) from the loaded YAML file
        self.apply_config()

    def load_config(self, config_file: str):
        """
        Load configuration from a YAML file.

        :param config_file: Path to the YAML file containing configuration settings.
        :return: Dictionary with configuration settings.
        """
        try:
            with open(config_file, 'r') as file:
                config = yaml.safe_load(file)
                print(f"Loaded configuration: {config}")
                return config
        except Exception as e:
            print(f"Error loading config file: {e}")
            return {}

    def apply_config(self):
        """
        Apply configuration settings (like thread count, memory limit, etc.)
        to the DuckDB connection.
        """
        if self.config:
            # Apply general execution parameters
            threads = self.config.get('threads')
            if threads:
                logging.info(f"Setting DuckDB to use {threads} threads.")
                self.connection.execute(f"SET threads={threads}")

            # Apply memory-related parameters
            memory_limit = self.config.get('memory_limit')
            if memory_limit:
                logging.info(f"Setting DuckDB memory limit to {memory_limit}.")
                self.connection.execute(f"SET memory_limit= '{memory_limit}'")

            # Enable or disable the progress bar
            enable_progress_bar = self.config.get('enable_progress_bar')
            if enable_progress_bar is not None:
                logging.info(f"Setting DuckDB enable_progress_bar to {enable_progress_bar}.")
                self.connection.execute(f"SET enable_progress_bar={str(enable_progress_bar).upper()}")

    def execute_query(self, query: str):
        """
        Executes a single query on the DuckDB connection.

        :param query: SQL query string to be executed.
        :return: The result of the query.
        """
        if result := self.connection.execute(query):
            return result
        return None


    def read_and_execute_sql(self, sql_content: str):
        """
        Shared function to read SQL content and send it to the DuckDB database.

        :param sql_content: SQL string to be executed.
        :return: The result of the executed query.
        """
        try:
            queries = sql_content.split(';')  # Split the SQL string by semicolons to separate multiple queries
            results = []
            for query in queries:
                query = query.strip()
                if query:  # Make sure the query is not empty
                    logging.info(f"Executing query: {query}")
                    result = self.execute_query(query)
                    results.append(result)
            return results
        except Exception as e:
            logging.error(f"Error executing SQL queries: {e}")
            return None

    def load_json_file(self, file_path: str, json_file_path: str = None):
        """
        Executes SQL queries from a file, optionally passing the JSON file path.

        :param file_path: Path to the SQL file containing the queries.
        :param json_file_path: (Optional) Path to the JSON file that should be passed into the SQL. If None, it's not used.
        :return: The result of the last executed query in the file.
        """
        try:
            with open(file_path, 'r') as file:
                sql_content = file.read()

            # If a JSON file path is provided, replace the placeholder with the actual path
            if json_file_path:
                sql_content = sql_content.replace("{json_file_path}", f"{json_file_path}")
            else:
                # If no JSON file path is passed, remove the placeholder
                sql_content = sql_content.replace("{json_file_path}", "")

            return self.read_and_execute_sql(sql_content)
        except Exception as e:
            logging.error(f"Error reading or executing SQL file: {e}")
            return None

    def dataframe_from_sql_file(self, file_path: str, json_file_path: str = None):
        """
        Reads an SQL file, executes the queries, and prints the result as a DataFrame.

        :param file_path: Path to the SQL file containing the queries.
        :param json_file_path: (Optional) Path to the JSON file that should be passed into the SQL. If None, it's not used.
        """
        result = None
        try:
            with open(file_path, 'r') as file:
                sql_content = file.read()

            # If a JSON file path is provided, replace the placeholder with the actual path
            if json_file_path:
                sql_content = sql_content.replace("{json_file_path}", f"{json_file_path}")
            else:
                # If no JSON file path is passed, remove the placeholder
                sql_content = sql_content.replace("{json_file_path}", "")

            # Execute the queries and print the result as a DataFrame
            queries = sql_content.split(';')
            for query in queries:
                query = query.strip()
                if query:
                    # If we have not yet started to collect results
                    if result is None:
                        result = self.connection.execute(query).fetchdf()
                    else:
                        result = result.append(self.connection.execute(query).fetchdf())
            return result
        except Exception as e:
            logging.error(f"Error executing SQL file and printing DataFrame: {e}")

    def close(self):
        """
        Close the DuckDB connection.
        """
        if self.connection:
            self.connection.close()

    def run_etl(self, sql_file_path: str, json_file_path: str = None):
        """
        Run the ETL process: Transform the data based on the SQL file and optionally using a JSON file.

        :param sql_file_path: The path to the SQL file containing the ETL logic.
        :param json_file_path: (Optional) The path to the JSON file. If None, JSON processing is skipped.
        """
        logging.info(f"Running ETL process with SQL file: {sql_file_path}")
        self.load_json_file(sql_file_path, json_file_path)  # Execute queries from the SQL file
        logging.info("ETL process completed successfully.")