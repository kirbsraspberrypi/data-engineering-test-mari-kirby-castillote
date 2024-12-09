import logging
import pandas as pd

class DataProcessor:
    """Class to handle data validation and transformation."""

    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)

    def validate_data(self, data: pd.DataFrame, sales_columns: list):
        """Validate and clean the data."""
        try:
            for col in sales_columns:
                data[col] = (
                    data[col].str.replace(',', '', regex=True)  # Remove commas
                    .fillna(0)                                  # Fill missing values with 0
                    .astype(float)                              # Convert to float
                )
            self.logger.info("Data validation and cleaning completed.")
            return data
        except Exception as e:
            self.logger.error(f"Error during data validation: {e}")
            raise

    def transform_data(self, data: pd.DataFrame, sales_columns: list):
        """Transform data to aggregate total sales by country."""
        try:
            data['Total Sales'] = data[sales_columns].sum(axis=1)
            transformed_data = data[['Country', 'Total Sales']].sort_values(by='Total Sales', ascending=False)
            self.logger.info("Data transformation completed.")
            return transformed_data
        except Exception as e:
            self.logger.error(f"Error during data transformation: {e}")
            raise
