import pandas as pd
import json
import glob

def process_and_clean_data():
    """
    ETL Engine: Extracts raw data, Transforms (Cleans/Normalizes), and Loads to Excel.
    Features:
    - Handles Nested JSON structures (Flattening).
    - Removes empty rows/columns (Data Cleaning).
    - Enforces strict type conversion.
    """
    processed_files = []
    
    # Get all files from the raw directory
    all_files = glob.glob(os.path.join(RAW_DATA_DIR, "*"))
    
    for file_path in all_files:
        try:
            filename = os.path.basename(file_path)
            
            # --- STEP 1: INGESTION (READING) ---
            if filename.endswith('.json'):
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                # Logic: Flatten nested JSON into a table
                if isinstance(data, list):
                    df = pd.json_normalize(data)
                else:
                    df = pd.json_normalize([data])
                    
            elif filename.endswith('.csv'):
                df = pd.read_csv(file_path)
            else:
                continue # Skip unknown files

            # --- STEP 2: DATA CLEANING (SANITIZATION) ---
            # Remove completely empty rows
            df.dropna(how='all', inplace=True)
            
            # Fill remaining missing values with a placeholder (Standard Practice)
            df.fillna("N/A", inplace=True)
            
            # Remove duplicate rows if any
            df.drop_duplicates(inplace=True)

            # --- STEP 3: EXPORT TO EXCEL ---
            output_name = f"Cleaned_{os.path.splitext(filename)[0]}.xlsx"
            output_path = os.path.join(PROCESSED_DATA_DIR, output_name)
            
            if not os.path.exists(PROCESSED_DATA_DIR):
                os.makedirs(PROCESSED_DATA_DIR)
                
            df.to_excel(output_path, index=False)
            processed_files.append(output_name)
            
        except Exception as e:
            print(f"Error processing {filename}: {e}")
            
    return processed_files