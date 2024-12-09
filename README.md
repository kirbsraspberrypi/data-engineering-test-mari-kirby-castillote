# Data Engineering Test - Mari Kirby Castillote

## Introduction

This repository demonstrates the implementation of a Python-based data pipeline designed to evaluate a candidate's skills in data engineering. The pipeline reads raw sales data, cleans and validates it, applies various transformations, and outputs the processed data into structured formats such as CSV files and a SQLite database. This project adheres to best practices, ensuring code modularity, maintainability, and thorough testing.

---

## Objective

The objective is to design and implement a complete data pipeline that:
1. Reads raw data from a CSV file.
2. Sanitizes and validates the data for consistency and accuracy.
3. Performs the following transformations:
   - Calculates total and average sales for each row.
   - Computes year-over-year growth.
4. Outputs the processed data into:
   - CSV files.
   - A SQLite database for persistent storage.
5. Includes robust unit tests to ensure the pipeline works as expected.

---

## Dataset

The pipeline processes a sample dataset, `scraped_data.csv`, located in the `static/input` directory. The dataset includes:
- `Country`: Names of countries.
- `2000 Sales`, `2001 Sales`: Sales data for respective years.
- Additional year-specific sales columns.

A sample dataset is included in this repository for demonstration purposes.

---

## Directory Structure

```
data-engineering-test-mari-kirby-castillote/
│
├── static/
│   ├── input/          # Directory for raw input files (e.g., scraped_data.csv).
│   ├── logs/           # Directory for logs generated during pipeline execution.
│   └── output/         # Directory for output files (CSV and SQLite database).
│
├── libs/
│   ├── dataapi.py      # Contains the DataProcessor class for data transformations.
│   └── savefileapi.py  # Contains the FileSaver class for saving data.
│
├── tests/
│   ├── test_data_pipeline.py  # Unit tests for the data pipeline.
│   └── input/                 # Input files for testing.
│
├── data_pipeline.py    # Main script to execute the data pipeline.
├── requirements.txt    # List of dependencies for the project.
└── README.md           # Documentation for the project.
```

---

# Setup and Usage

## 1. Environment Setup

### Clone the repository:

```
git clone https://github.com/kirbsraspberrypi/data-engineering-test-mari-kirby-castillote.git
cd data-engineering-test-mari-kirby-castillote/
```

# Create and activate a virtual environment:

### For Linux/Mac:

```
python3 -m venv env
source env/bin/activate
```

### For Windows:

```
python3 -m venv env
env\Scripts\activate
```

# Install the dependencies:

```
pip3 install -r requirements.txt
```

## 2. Running the Pipeline

- Place the raw input file `scraped_data.csv` in the `static/input` directory. You can find the [scraped_data.csv](https://www.kaggle.com/datasets/sukhmandeepsinghbrar/total-worldwide-passenger-cars-sales) here.
- Execute the pipeline:

```
python3 data_pipeline.py
```

## Output:

- Logs: Saved in `static/logs/pipeline.log`.
- Processed CSV files:

  - `static/output/transformed_total_and_average.csv`: Contains total and average sales per country.

  - `static/output/transformed_growth.csv`: Contains year-over-year growth percentages.

- SQLite Database:

  - `static/output/transformed_data.db`: Contains the final processed data.

## 3. Running Tests

Run the unit tests to validate the pipeline:

```
pytest
```

---

# Implementation Details

## 1. Design Decisions

### Modular Code:

- **`DataProcessor`**:
  - Handles data cleaning, validation, and transformations.

- **`FileSaver`**:
  - Manages saving data to CSV and SQLite database.

### Error Handling:
- Ensures graceful failure with detailed logs for debugging.

### Logging:
- Captures all key steps in the pipeline for traceability.

### Testing:
- Includes tests for edge cases, transformations, and data outputs.

---

## 2. Key Functions

### DataProcessor (`dataapi.py`)

- **`sanitize_data`**:

  - Removes invalid entries, trims whitespaces, and standardizes formats.
- **`validate_data`**:

  - Converts sales columns to numeric format and handles missing values.
- **`transform_total_and_average`**:

  - Adds total and average sales columns.
- **`transform_growth`**:

  - Calculates year-over-year growth percentages.

### FileSaver (`savefileapi.py`)

- **`save_to_csv`**:

  - Saves processed data to CSV files.
- **`save_to_sqlite`**:

  - Saves processed data to a SQLite database.

### Unit Tests (`test_data_pipeline.py`)

- Verifies the correctness of data processing and transformations.
- Ensures data outputs match expected results.

---

## Reference

- [Sample Data: Total Worldwide Passenger Cars Sales](https://www.kaggle.com/datasets/sukhmandeepsinghbrar/total-worldwide-passenger-cars-sales)

# End
