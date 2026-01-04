# make_dummy_files.py (Sirf test files banane ke liye)
import pandas as pd
import os
import random

os.makedirs("my_test_uploads", exist_ok=True)

print("Files bana raha hu...")
for i in range(50):
    df = pd.DataFrame({
        'Transaction_ID': range(100),
        'Amount': [random.randint(100, 5000) for _ in range(100)],
        'Product_Name': ['Item' for _ in range(100)]
    })
    df.to_csv(f"my_test_uploads/file_{i}.csv", index=False)

print("Done! 'my_test_uploads' folder check karo.")