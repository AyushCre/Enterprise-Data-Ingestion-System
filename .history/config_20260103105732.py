import os
import logging

# --- Project Configuration ---

# Directory Paths
RAW_DATA_DIR = "raw_data"
PROCESSED_DATA_DIR = "processed_data"

# Create directories if they don't exist
os.makedirs(RAW_DATA_DIR, exist_ok=True)
os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

# Logging Configuration
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s', 
    level=logging.INFO
)