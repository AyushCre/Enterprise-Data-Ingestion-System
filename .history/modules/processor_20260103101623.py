import os
import time
import pandas as pd
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List
from config import PROCESSED_DATA_DIR

def transformation_logic(file_path: str) -> bool:
    try:
        if not os.path.exists(file_path): return False
            
        df = pd.read_csv(file_path)
        
        # --- SIMULATING HEAVY WORK ---
        # Ye line add karo taaki process thoda heavy ho jaye
        # Tabhi Parallel processing ka faayda dikhega
        time.sleep(0.05) 
        # -----------------------------

        # Business Logic
        if 'Amount' in df.columns:
            df['Tax_Amount'] = df['Amount'] * 0.18
            df['Total_Amount'] = df['Amount'] + df['Tax_Amount']
        
        filename = os.path.basename(file_path)
        save_path = os.path.join(PROCESSED_DATA_DIR, f"processed_{filename}")
        df.to_csv(save_path, index=False)
        return True
    except Exception as e:
        logging.error(f"Error processing {file_path}: {e}")
        return False