import pandas as pd
import os
import random

# 1. Configuration: Define the output directory for generated files
FOLDER_NAME = "my_test_files_for_upload"
os.makedirs(FOLDER_NAME, exist_ok=True)

print(f"🚀 Initializing bulk data generation in '{FOLDER_NAME}'...")

# 2. Generate 500 Synthetic Datasets (Simulating Batch Data)
for i in range(100):
    # Construct Synthetic Data Logic
    data = {
        'Transaction_ID': [f"TXN-{i}-{x}" for x in range(100)], # Simulating 100 transactions per file
        'Amount': [random.randint(100, 5000) for _ in range(100)],
        'Product_Name': [random.choice(['Laptop', 'Mouse', 'Keyboard']) for _ in range(100)],
        'Region': [random.choice(['North', 'South', 'East', 'West']) for _ in range(100)]
    }
    
    df = pd.DataFrame(data)
    
    # Persist the dataframe to a CSV file
    file_path = os.path.join(FOLDER_NAME, f"demo_file_{i}.csv")
    df.to_csv(file_path, index=False)

print(f"✅ Success! 500 Synthetic CSV files are ready in '{FOLDER_NAME}'.")