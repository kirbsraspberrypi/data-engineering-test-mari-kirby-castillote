import logging
import pandas as pd


class DataProcessor:
    """Class to handle data validation, sanitation, and transformation."""

    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)

    def sanitize_data(self, data: pd.DataFrame):
        """Sanitize the data to ensure consistency and remove invalid entries."""
        try:
            # Remove leading/trailing whitespaces in column names and values
            data.columns = data.columns.str.strip()
            data = data.apply(lambda col: col.str.strip() if col.dtype == "object" else col)

            # Standardize case for categorical columns (e.g., country names)
            if 'Country' in data.columns:
                data['Country'] = data['Country'].str.title()  # Title case (e.g., "united states" -> "United States")

            # Drop duplicate rows
            data = data.drop_duplicates()

            # Remove rows where 'Country' or sales columns are entirely missing
            if 'Country' in data.columns:
                data = data[data['Country'].notna()]  # Keep rows where 'Country' is not NaN

            self.logger.info("Data sanitation completed.")
            return data
        except Exception as e:
            self.logger.error(f"Error during data sanitation: {e}")
            raise

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

    def transform_total_and_average(self, data: pd.DataFrame, sales_columns: list):
        """Calculate total and average sales for each country."""
        try:
            data['Total Sales'] = data[sales_columns].sum(axis=1)
            data['Average Sales'] = data[sales_columns].mean(axis=1)
            self.logger.info("Total and average sales calculated.")
            return data
        except Exception as e:
            self.logger.error(f"Error during total and average transformation: {e}")
            raise

    def transform_growth(self, data: pd.DataFrame, sales_columns: list):
        """Calculate year-over-year growth for each country."""
        try:
            for i in range(len(sales_columns) - 1):
                year1 = sales_columns[i]
                year2 = sales_columns[i + 1]
                growth_column = f"Growth {year1[:4]}-{year2[:4]}"
                data[growth_column] = (
                    (data[year2] - data[year1]) / data[year1].replace(0, 1) * 100
                )  # Avoid division by zero
            self.logger.info("Year-over-year growth calculated.")
            return data
        except Exception as e:
            self.logger.error(f"Error during growth transformation: {e}")
            raise
