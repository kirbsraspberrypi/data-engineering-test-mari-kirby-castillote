import logging
import sqlite3
import pandas as pd


class FileSaver:
    """Class to handle saving data to files."""

    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)

    def save_to_csv(self, data: pd.DataFrame, file_path: str):
        """Save a DataFrame to a CSV file."""
        try:
            data.to_csv(file_path, index=False)
            self.logger.info(f"Data successfully saved to CSV: {file_path}")
        except Exception as e:
            self.logger.error(f"Error saving data to CSV: {e}")
            raise

    def save_to_sqlite(self, data: pd.DataFrame, db_path: str, table_name: str = "data"):
        """Save a DataFrame to an SQLite database."""
        try:
            conn = sqlite3.connect(db_path)
            data.to_sql(table_name, conn, if_exists='replace', index=False)
            conn.close()
            self.logger.info(f"Data successfully saved to SQLite database: {db_path}")
        except Exception as e:
            self.logger.error(f"Error saving data to SQLite: {e}")
            raise
