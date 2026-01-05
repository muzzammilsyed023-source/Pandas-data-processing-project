📊 Employee Data Cleaning Pipeline (Pandas)

A Python–Pandas project that automates the cleaning and transformation of employee CSV data to produce analytics-ready datasets suitable for downstream ETL and reporting workflows.

## Project Structure
PANDAS-API-PROJECT/
│
├── pandas_project.py # Main data processing script
├── data/
│ └── employees.csv # Input dataset
├── output/
│ └── final_clean_data.csv # Processed output
└── README.md # Project documentation

🔑 Key Steps

Read raw employee data from CSV files

Enforce correct data types for key columns

Handle missing values using department-level and overall salary averages

Remove duplicate employee records

Apply business filters (salary threshold and valid departments)

Sort cleaned data and generate a department-level summary

📂 Outputs

final_clean_data.csv – Cleaned, filtered, and sorted employee-level data

department_summary.csv – Aggregated department-wise salary metrics and employee counts

🛠️ Tech Stack

Python · Pandas · Git · GitHub
