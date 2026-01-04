import os
import random
import logging
from datetime import datetime
import pandas as pd
from config import RAW_DATA_DIR  # Importing from config

def generate_single_file(file_index: int) -> str:
    """Generates a single dummy CSV file."""
    try:
        products = ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Server']
        regions = ['North', 'South', 'East', 'West', 'Central']
        
        data = {
            'Transaction_ID': [f'TXN-{file_index}-{i}' for i in range(50)],
            'Date': [datetime.now().strftime("%Y-%m-%d") for _ in range(50)],
            'Product_Name': [random.choice(products) for _ in range(50)],
            'Amount': [random.randint(100, 5000) for _ in range(50)],
            'Region': [random.choice(regions) for _ in range(50)]
        }
        
        df = pd.DataFrame(data)
        file_path = os.path.join(RAW_DATA_DIR, f"data_{file_index}.csv")
        df.to_csv(file_path, index=False)
        return file_path
    except Exception as e:
        logging.error(f"Failed to generate file {file_index}: {e}")
        return None

def generate_bulk_data(num_files: int, status_callback=None):
    """
    Orchestrates bulk data generation.
    status_callback: Optional function to update UI progress.
    """
    logging.info(f"Starting generation of {num_files} files.")
    generated_count = 0
    
    for i in range(num_files):
        generate_single_file(i)
        generated_count += 1
        
        # Agar UI ne koi callback function bheja hai to update karo
        if status_callback and i % 10 == 0:
            status_callback((i + 1) / num_files, f"Generating Data... ({i+1}/{num_files})")
            
    return generated_count