import logging
from pathlib import Path
from libs.dataapi import DataProcessor
from libs.savefileapi import FileSaver

# Setup logging
BASE_DIR = Path('data-engineering-test-mari-kirby-castillote')
INPUT_DIR = BASE_DIR / 'static/input'
LOGS_DIR = BASE_DIR / 'static/logs'
OUTPUT_DIR = BASE_DIR / 'static/output'
LOG_FILE = LOGS_DIR / 'pipeline.log'

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
    output_csv = OUTPUT_DIR / 'transformed_data.csv'
    output_db = OUTPUT_DIR / 'transformed_data.db'

    # Ensure directories exist
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Initialize helper classes
    data_processor = DataProcessor(logger)
    file_saver = FileSaver(logger)

    try:
        # Read raw data
        raw_data = pd.read_csv(input_file)
        logger.info("Successfully read raw data.")

        # Validate and transform data
        sales_columns = [col for col in raw_data.columns if 'Sales' in col]
        validated_data = data_processor.validate_data(raw_data, sales_columns)
        transformed_data = data_processor.transform_data(validated_data, sales_columns)

        # Save outputs
        file_saver.save_to_csv(transformed_data, output_csv)
        file_saver.save_to_sqlite(transformed_data, output_db, table_name="transformed_data")
        logger.info("Pipeline execution completed successfully.")
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")

if __name__ == '__main__':
    main()
