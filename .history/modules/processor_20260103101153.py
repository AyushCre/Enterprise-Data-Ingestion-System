import os
import time
import pandas as pd
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List
from config import PROCESSED_DATA_DIR

def transformation_logic(file_path: str) -> bool:
    """Read CSV, Calculate Tax, Save CSV."""
    try:
        if not os.path.exists(file_path):
            return False
            
        df = pd.read_csv(file_path)
        # Business Logic: 18% Tax
        df['Tax_Amount'] = df['Amount'] * 0.18
        df['Total_Amount'] = df['Amount'] + df['Tax_Amount']
        
        filename = os.path.basename(file_path)
        save_path = os.path.join(PROCESSED_DATA_DIR, f"processed_{filename}")
        df.to_csv(save_path, index=False)
        return True
    except Exception as e:
        logging.error(f"Error processing {file_path}: {e}")
        return False

def process_sequential(files: List[str], progress_callback=None) -> float:
    """Standard loop processing."""
    start_time = time.time()
    total = len(files)
    
    for i, file_path in enumerate(files):
        transformation_logic(file_path)
        if progress_callback and i % 10 == 0:
            progress_callback((i + 1) / total, f"Sequential Processing: {i+1}/{total}")
            
    return time.time() - start_time

def process_parallel(files: List[str], progress_callback=None) -> float:
    """Optimized Parallel processing."""
    start_time = time.time()
    total = len(files)
    completed = 0
    
    with ProcessPoolExecutor() as executor:
        futures = {executor.submit(transformation_logic, f): f for f in files}
        
        for future in as_completed(futures):
            completed += 1
            if progress_callback and completed % 10 == 0:
                progress_callback(completed / total, f"Parallel Processing: {completed}/{total}")

    return time.time() - start_time