import json
import os
import random
import logging

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration: Output Directory
OUTPUT_DIR = "json_dummy_datasets"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def generate_synthetic_json_data(file_count=50, records_per_file=100):
    """
    Generates synthetic JSON datasets for testing pipeline ingestion capabilities.
    
    Args:
        file_count (int): Number of JSON files to generate.
        records_per_file (int): Number of records per JSON file.
    """
    logging.info(f"Starting generation of {file_count} synthetic JSON files in '{OUTPUT_DIR}'...")

    for i in range(file_count):
        data_packet = []
        
        for x in range(records_per_file):
            record = {
                'Transaction_ID': f"TXN-{i}-{x}",
                'Amount': random.randint(100, 5000),
                'Product_Name': random.choice(['Laptop', 'Mouse', 'Keyboard', 'Monitor']),
                'Region': random.choice(['North', 'South', 'East', 'West']),
                'Timestamp': "2023-10-27T10:00:00Z"
            }
            data_packet.append(record)
        
        file_path = os.path.join(OUTPUT_DIR, f"dataset_{i}.json")
        
        with open(file_path, 'w') as f:
            json.dump(data_packet, f, indent=4)

    logging.info(f"✅ Successfully generated {file_count} JSON datasets.")

if __name__ == "__main__":
    generate_synthetic_json_data()