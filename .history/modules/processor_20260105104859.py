import os
import time
import pandas as pd
import logging
from concurrent.futures import ProcessPoolExecutor, ascompleted
from typing import List
from config import PROCESSEDDATADIR

os.makedirs(PROCESSEDDATADIR, exist_ok=True)

def transformation_logic(filepath: str) -> bool:
    """
    Read CSV/JSON, Calculate Tax, Save CSV.
    """
    try:
        if not os.path.exists(filepath):
            return False
        
        if filepath.endswith('.json'):
            df = pd.read_json(filepath)
        else:
            df = pd.read_csv(filepath)
        
        time.sleep(0.05)  # Artificial delay to show parallel power
        
        if 'Amount' in df.columns:
            df['Tax_Amount'] = df['Amount'] * 0.18
            df['Total_Amount'] = df['Amount'] + df['Tax_Amount']
        
        # Add source filename for merged table tracking
        df['Source_File'] = os.path.basename(filepath)
        
        filename = os.path.basename(filepath)
        if filename.endswith('.json'):
            filename = filename.replace('.json', '.csv')
        
        save_path = os.path.join(PROCESSEDDATADIR, f"processed_{filename}")
        df.to_csv(save_path, index=False)
        
        return True
    except Exception as e:
        logging.error(f"Error processing {filepath}: {e}")
        return False

def process_sequential(files: List[str], progress_callback=None) -> float:
    """Standard loop processing."""
    start_time = time.time()
    total = len(files)
    
    for i, filepath in enumerate(files):
        transformation_logic(filepath)
        if progress_callback:
            progress_callback((i + 1) / total, f"Sequential Processing {i+1}/{total}")
    
    return time.time() - start_time

def process_parallel(files: List[str], progress_callback=None) -> float:
    """Optimized Parallel processing."""
    start_time = time.time()
    total = len(files)
    completed = 0
    
    with ProcessPoolExecutor() as executor:
        futures = {executor.submit(transformation_logic, f): f for f in files}
        
        for future in ascompleted(futures):
            completed += 1
            if progress_callback:
                progress_callback(completed / total, f"Parallel Processing {completed}/{total}")
    
    return time.time() - start_time

def create_merged_table() -> str:
    """
    Merge all processed CSV files into one consolidated table.
    Adds source_file column for tracking which file each row came from.
    Returns path to merged file.
    """
    merged_dfs = []
    processed_files = [f for f in os.listdir(PROCESSEDDATADIR) if f.startswith('processed_') and f.endswith('.csv')]
    
    if not processed_files:
        logging.warning("No processed files found for merging.")
        return None
    
    for csv_file in sorted(processed_files):
        file_path = os.path.join(PROCESSEDDATADIR, csv_file)
        try:
            df = pd.read_csv(file_path)
            merged_dfs.append(df)
        except Exception as e:
            logging.error(f"Error reading {csv_file} for merge: {e}")
    
    if not merged_dfs:
        return None
    
    # Concatenate all DataFrames
    merged_df = pd.concat(merged_dfs, ignore_index=True)
    
    # Save merged file
    merged_path = os.path.join(PROCESSEDDATADIR, "MERGED_ALL.csv")
    merged_df.to_csv(merged_path, index=False)
    
    logging.info(f"Merged table created: {merged_path} with {len(merged_df)} total rows")
    
    return merged_path
