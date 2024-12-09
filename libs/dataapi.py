import logging
import pandas as pd


class DataProcessor:
    """Class to handle data validation, sanitation, and transformation."""

    def __init__(self, logger=None):
        # Initialize the DataProcessor with an optional logger
        self.logger = logger or logging.getLogger(__name__)

    def sanitize_data(self, data: pd.DataFrame):
        """
        Sanitize the data to ensure consistency and remove invalid entries.
        - Removes leading/trailing whitespaces in column names and string values.
        - Standardizes the case of categorical columns like 'Country'.
        - Drops duplicate rows.
        - Ensures required columns like 'Country' are present.
        """
        try:
            # Log a warning if the DataFrame is empty and return it
            if data.empty:
                self.logger.warning("Empty DataFrame provided to sanitize.")
                return data

            # Check for the presence of required columns like 'Country'
            required_columns = ['Country']
            for col in required_columns:
                if col not in data.columns:
                    raise KeyError(f"Missing required column: {col}")

            # Remove leading/trailing whitespaces in column names
            data.columns = data.columns.str.strip()

            # Remove leading/trailing whitespaces in string values
            data = data.apply(lambda col: col.str.strip() if col.dtype == "object" else col)

            # Standardize case for categorical columns like 'Country' (e.g., title case)
            if 'Country' in data.columns:
                data['Country'] = data['Country'].str.title()

            # Drop duplicate rows to avoid redundant data
            data = data.drop_duplicates()

            # Remove rows with missing 'Country' values
            data = data[data['Country'].notna()]

            self.logger.info("Data sanitation completed.")
            return data
        except Exception as e:
            # Log errors that occur during sanitation
            self.logger.error(f"Error during data sanitation: {e}")
            raise

    def validate_data(self, data: pd.DataFrame, sales_columns: list):
        """
        Validate and clean the data.
        - Converts sales columns to numeric values.
        - Fills missing values with 0 in sales columns.
        """
        try:
            # Process each sales column to remove commas, fill NaNs, and convert to float
            for col in sales_columns:
                data[col] = (
                    data[col].str.replace(',', '', regex=True)
                    .fillna(0)
                    .astype(float)
                )
            self.logger.info("Data validation and cleaning completed.")
            return data
        except Exception as e:
            # Log any errors during data validation
            self.logger.error(f"Error during data validation: {e}")
            raise

    def transform_total_and_average(self, data: pd.DataFrame, sales_columns: list):
        """
        Calculate total and average sales for each country.
        - Adds 'Total Sales' and 'Average Sales' columns to the dataset.
        """
        try:
            # Add a column for total sales by summing across sales columns
            data['Total Sales'] = data[sales_columns].sum(axis=1)

            # Add a column for average sales by averaging across sales columns
            data['Average Sales'] = data[sales_columns].mean(axis=1)

            self.logger.info("Total and average sales calculated.")
            return data
        except Exception as e:
            # Log any errors during the transformation
            self.logger.error(f"Error during total and average transformation: {e}")
            raise

    def transform_growth(self, data: pd.DataFrame, sales_columns: list):
        """
        Calculate year-over-year growth for each country.
        - Adds growth columns for each consecutive pair of sales years.
        """
        try:
            # Calculate year-over-year growth between consecutive sales columns
            for i in range(len(sales_columns) - 1):
                year1 = sales_columns[i]
                year2 = sales_columns[i + 1]
                growth_column = f"Growth {year1[:4]}-{year2[:4]}"
                # Calculate growth percentage, avoiding division by zero
                data[growth_column] = (
                    (data[year2] - data[year1]) / data[year1].replace(0, 1) * 100
                )
            self.logger.info("Year-over-year growth calculated.")
            return data
        except Exception as e:
            # Log any errors during growth transformation
            self.logger.error(f"Error during growth transformation: {e}")
            raise
