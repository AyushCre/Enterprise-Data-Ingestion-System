import pandas as pd
import os
import glob

# --- CONFIGURATION ---
RAW_DATA_DIR = "raw_data"
PROCESSED_DATA_DIR = "processed_data"

def process_files():
    """
    Reads CSV files from raw_data, converts them to Excel, 
    and saves them in processed_data.
    """
    if not os.path.exists(PROCESSED_DATA_DIR):
        os.makedirs(PROCESSED_DATA_DIR)
        
    # Get all CSV files
    all_files = glob.glob(os.path.join(RAW_DATA_DIR, "*.csv"))
    
    if not all_files:
        return "No valid CSV files found to process."
    
    converted_count = 0
    
    for file_path in all_files:
        try:
            # Read CSV
            df = pd.read_csv(file_path)
            
            # Simple Data Cleaning (Empty rows remove karna)
            df.dropna(how='all', inplace=True)
            
            # Output File Name
            filename = os.path.basename(file_path)
            output_name = filename.replace('.csv', '.xlsx')
            output_path = os.path.join(PROCESSED_DATA_DIR, output_name)
            
            # Convert to Excel
            df.to_excel(output_path, index=False)
            converted_count += 1
            
        except Exception as e:
            return f"Error processing {filename}: {str(e)}"
            
    return f"🎉 Success! {converted_count} files converted to Excel."