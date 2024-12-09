import os
import sys
import pytest
import logging
import sqlite3
import pandas as pd
from pathlib import Path

# Add the project base directory to sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
LIBS_DIR = BASE_DIR / 'libs'
sys.path.insert(0, str(LIBS_DIR))

from dataapi import DataProcessor
from savefileapi import FileSaver

# Setup test directories
TEST_DIR = BASE_DIR / 'tests'
INPUT_DIR = TEST_DIR / 'input'
OUTPUT_DIR = TEST_DIR / 'output'
LOG_DIR = TEST_DIR / 'logs'

# Create directories for testing
INPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Logger mock for testing
logger = logging.getLogger(__name__)

@pytest.fixture
def mock_data():
    """Fixture for providing mock sales data."""
    data = pd.DataFrame({
        'Country': ['USA', '  Canada ', 'UK', None, 'Australia'],
        '2000 Sales': ['1,000', '2,000', None, '500', '1,500'],
        '2001 Sales': ['1,200', '2,100', '3,000', '600', '1,800']
    })
    return data

@pytest.fixture
def processor():
    """Fixture for initializing the DataProcessor."""
    return DataProcessor(logger)

@pytest.fixture
def saver():
    """Fixture for initializing the FileSaver."""
    return FileSaver(logger)

def test_empty_dataset(processor):
    """Test behavior with an empty dataset."""
    empty_data = pd.DataFrame()
    sanitized = processor.sanitize_data(empty_data)
    assert sanitized.empty  # Ensure output is also empty

def test_missing_columns(processor, mock_data):
    """Test behavior when columns are missing."""
    mock_data.drop(columns=['Country'], inplace=True)
    with pytest.raises(KeyError, match="Missing required column: Country"):
        processor.sanitize_data(mock_data)

def test_incorrect_data_types(processor, mock_data):
    """Test behavior with incorrect data types in numeric columns."""
    mock_data['2000 Sales'] = ['abc', 'def', None, '500', '1,500']
    with pytest.raises(ValueError):
        processor.validate_data(mock_data, ['2000 Sales', '2001 Sales'])

def test_total_and_average_output(processor, saver, mock_data):
    """Verify total and average calculations produce correct output."""
    sales_columns = ['2000 Sales', '2001 Sales']
    sanitized = processor.sanitize_data(mock_data)
    validated = processor.validate_data(sanitized, sales_columns)
    transformed = processor.transform_total_and_average(validated, sales_columns)
    output_file = OUTPUT_DIR / 'test_total_and_average.csv'
    saver.save_to_csv(transformed, output_file)

    # Reload the saved file and validate contents
    saved_data = pd.read_csv(output_file)
    assert 'Total Sales' in saved_data.columns
    assert 'Average Sales' in saved_data.columns
    assert not saved_data.empty

def test_growth_output(processor, saver, mock_data):
    """Verify growth calculations produce correct output."""
    sales_columns = ['2000 Sales', '2001 Sales']
    sanitized = processor.sanitize_data(mock_data)
    validated = processor.validate_data(sanitized, sales_columns)
    transformed = processor.transform_growth(validated, sales_columns)
    output_file = OUTPUT_DIR / 'test_growth.csv'
    saver.save_to_csv(transformed, output_file)

    # Reload the saved file and validate contents
    saved_data = pd.read_csv(output_file)
    assert 'Growth 2000-2001' in saved_data.columns
    assert not saved_data.empty

def test_sqlite_output(saver, mock_data):
    """Verify data is correctly saved to SQLite."""
    db_file = OUTPUT_DIR / 'test_output.db'
    saver.save_to_sqlite(mock_data, db_file, table_name='test_table')

    # Connect to the database and validate contents
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='test_table';")
    assert cursor.fetchone() is not None  # Table exists

    # Validate table content
    loaded_data = pd.read_sql_query("SELECT * FROM test_table", conn)
    assert not loaded_data.empty  # Data exists in the table
    conn.close()

def test_file_overwrite(saver, mock_data):
    """Test behavior when output file already exists."""
    file_path = OUTPUT_DIR / 'test_overwrite.csv'
    # Save first file
    saver.save_to_csv(mock_data, file_path)
    assert file_path.exists()

    # Save again to ensure overwrite works
    new_data = pd.DataFrame({'Country': ['New Zealand'], '2000 Sales': [1000], '2001 Sales': [1100]})
    saver.save_to_csv(new_data, file_path)
    saved_data = pd.read_csv(file_path)
    assert len(saved_data) == 1  # Ensure new data overwrote old data

def test_log_messages(processor, mock_data, caplog):
    """Verify appropriate log messages are generated."""
    with caplog.at_level(logging.INFO):
        processor.sanitize_data(mock_data)
    assert "Data sanitation completed." in caplog.text  # Log message exists
