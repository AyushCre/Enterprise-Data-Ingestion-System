import pandas as pd
import os
import random

# 1. Folder ka naam jahan files banengi
FOLDER_NAME = "my_test_files_for_upload"
os.makedirs(FOLDER_NAME, exist_ok=True)

print(f"🚀 {FOLDER_NAME} folder me files bana raha hu...")

# 2. 50 Files create karte hain
for i in range(500):
    # Dummy Data Logic
    data = {
        'Transaction_ID': [f"TXN-{i}-{x}" for x in range(100)], # 100 rows per file
        'Amount': [random.randint(100, 5000) for _ in range(100)],
        'Product_Name': [random.choice(['Laptop', 'Mouse', 'Keyboard']) for _ in range(100)],
        'Region': [random.choice(['North', 'South', 'East', 'West']) for _ in range(100)]
    }
    
    df = pd.DataFrame(data)
    
    # File save karo
    file_path = os.path.join(FOLDER_NAME, f"demo_file_{i}.csv")
    df.to_csv(file_path, index=False)

print(f"✅ Success! 50 Files '{FOLDER_NAME}' folder me ready hain.")