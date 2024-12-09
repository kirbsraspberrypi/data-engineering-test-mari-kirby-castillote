import logging
import pandas as pd
from pathlib import Path
from libs.dataapi import DataProcessor
from libs.savefileapi import FileSaver

# Resolve project base directory dynamically
BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / 'static/input'
LOGS_DIR = BASE_DIR / 'static/logs'
OUTPUT_DIR = BASE_DIR / 'static/output'
LOG_FILE = LOGS_DIR / 'pipeline.log'

# Setup logging
LOGS_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def main():
    # Define file paths
    input_file = INPUT_DIR / 'scraped_data.csv'
    output_total_csv = OUTPUT_DIR / 'transformed_total_and_average.csv'
    output_growth_csv = OUTPUT_DIR / 'transformed_growth.csv'
    output_db = OUTPUT_DIR / 'transformed_data.db'

    # Ensure directories exist
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Initialize helper classes
    data_processor = DataProcessor(logger)
    file_saver = FileSaver(logger)

    try:
        # Step 1: Extract - Read raw data
        if not input_file.exists():
            raise FileNotFoundError(f"Input file not found: {input_file}")
        raw_data = pd.read_csv(input_file)
        logger.info("Successfully read raw data.")

        # Step 2: Sanitize - Clean the data
        sanitized_data = data_processor.sanitize_data(raw_data)
        logger.info("Data sanitation completed.")

        # Step 3: Validate - Ensure data integrity
        sales_columns = [col for col in sanitized_data.columns if 'Sales' in col]
        validated_data = data_processor.validate_data(sanitized_data, sales_columns)
        logger.info("Data validation completed.")

        # Step 4a: Transform - Calculate total and average sales
        total_avg_data = data_processor.transform_total_and_average(validated_data, sales_columns)
        file_saver.save_to_csv(total_avg_data, output_total_csv)

        # Step 4b: Transform - Calculate year-over-year growth
        growth_data = data_processor.transform_growth(validated_data, sales_columns)
        file_saver.save_to_csv(growth_data, output_growth_csv)

        # Step 5: Load - Save processed data to SQLite
        file_saver.save_to_sqlite(growth_data, output_db, table_name="transformed_data")
        logger.info("Pipeline execution completed successfully.")
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        raise


if __name__ == '__main__':
    main()
