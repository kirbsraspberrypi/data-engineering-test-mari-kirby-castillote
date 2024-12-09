import logging
import pandas as pd
from pathlib import Path
from libs.dataapi import DataProcessor
from libs.savefileapi import FileSaver

# Resolve project base directory dynamically to make the script portable across environments.
BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / 'static/input'  # Directory for input files.
LOGS_DIR = BASE_DIR / 'static/logs'  # Directory for log files.
OUTPUT_DIR = BASE_DIR / 'static/output'  # Directory for output files.
LOG_FILE = LOGS_DIR / 'pipeline.log'  # Log file to track pipeline execution.

# Setup logging for the pipeline to monitor execution and capture errors.
LOGS_DIR.mkdir(parents=True, exist_ok=True)  # Ensure the log directory exists.
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Create a logger instance for logging pipeline activities.
logger = logging.getLogger(__name__)

def main():
    # Define file paths for input and output to maintain organized directory structure.
    input_file = INPUT_DIR / 'scraped_data.csv'  # Input file containing raw data.
    output_total_csv = OUTPUT_DIR / 'transformed_total_and_average.csv'  # Output CSV for total and average sales.
    output_growth_csv = OUTPUT_DIR / 'transformed_growth.csv'  # Output CSV for year-over-year growth.
    output_db = OUTPUT_DIR / 'transformed_data.db'  # Output SQLite database file.

    # Ensure directories for input and output exist to prevent runtime errors.
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Initialize helper classes for processing and saving data.
    data_processor = DataProcessor(logger)  # For sanitizing, validating, and transforming data.
    file_saver = FileSaver(logger)  # For saving processed data to CSV and SQLite.

    try:
        # Step 1: Extract - Read raw data from the input CSV file.
        if not input_file.exists():
            raise FileNotFoundError(f"Input file not found: {input_file}")
        raw_data = pd.read_csv(input_file)  # Load raw data into a pandas DataFrame.
        logger.info("Successfully read raw data.")

        # Step 2: Sanitize - Clean the data by removing duplicates, invalid entries, and standardizing formats.
        sanitized_data = data_processor.sanitize_data(raw_data)
        logger.info("Data sanitation completed.")

        # Step 3: Validate - Ensure sales columns are numeric and fill missing values with defaults.
        sales_columns = [col for col in sanitized_data.columns if 'Sales' in col]  # Identify sales columns dynamically.
        validated_data = data_processor.validate_data(sanitized_data, sales_columns)
        logger.info("Data validation completed.")

        # Step 4a: Transform - Calculate total and average sales for each row.
        total_avg_data = data_processor.transform_total_and_average(validated_data, sales_columns)
        file_saver.save_to_csv(total_avg_data, output_total_csv)  # Save total and average sales to CSV.

        # Step 4b: Transform - Calculate year-over-year growth based on consecutive sales years.
        growth_data = data_processor.transform_growth(validated_data, sales_columns)
        file_saver.save_to_csv(growth_data, output_growth_csv)  # Save year-over-year growth to CSV.

        # Step 5: Load - Save the fully processed data to an SQLite database.
        file_saver.save_to_sqlite(growth_data, output_db, table_name="transformed_data")
        logger.info("Pipeline execution completed successfully.")
    except Exception as e:
        # Catch and log any errors during the pipeline execution.
        logger.error(f"Pipeline execution failed: {e}")
        raise


# Entry point for executing the data pipeline script.
if __name__ == '__main__':
    main()
