# Code for dataset join validation
import duckdb
import os
import pandas as pd

# Get path to the folder where this script is located
base_folder = os.path.dirname(os.path.abspath(__file__))

# CSVs are in subfolder 'CS1'
csv_folder = os.path.join(base_folder, "CS1")

# Connect to DuckDB (in-memory)
con = duckdb.connect()

# Loop through months Jan–Dec
month_nums = range(1, 13)
loaded_months = []

for month in month_nums:
    month_str = f"{month:02}"  # Format: 01, 02, ..., 12
    file_name = f"2024{month_str}-divvy-tripdata.csv"
    file_path = os.path.join(csv_folder, file_name)
    
    if os.path.exists(file_path):
        table_name = f"m{month_str}"
        con.execute(f"""
            CREATE TABLE {table_name} AS 
            SELECT * FROM read_csv_auto('{file_path}', header=True)
        """)
        loaded_months.append(table_name)
        print(f"✅ Loaded {file_name} into table {table_name}")
    else:
        print(f"⚠️ File not found: {file_path}")

# Combine all data
if loaded_months:
    union_query = " UNION ALL ".join([f"SELECT * FROM {table}" for table in loaded_months])
    con.execute(f"CREATE OR REPLACE TABLE all_data AS {union_query}")
    print(f"\n✅ Combined {len(loaded_months)} tables into 'all_data'")

    # Example analysis
    result = con.execute("""
        SELECT member_casual, COUNT(*) AS ride_count
        FROM all_data
        GROUP BY member_casual
        ORDER BY ride_count DESC
    """).fetchdf()

    print("\n📊 Ride counts by user type:")
    print(result)

else:
    print("❌ No valid CSV files found in folder.")