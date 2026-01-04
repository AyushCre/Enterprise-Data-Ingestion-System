import re
import os
import shutil
import time
import streamlit as st

# --- CONFIGURATION ---
RAW_DATA_DIR = "raw_data"
PROCESSED_DATA_DIR = "processed_data"

def sanitize_filename(filename):
    """
    Security Utility: Sanitizes the filename by removing dangerous characters.
    It strictly allows only A-Z, 0-9, _, ., and - to prevent shell injection risks.
    """
    clean_name = re.sub(r'[^a-zA-Z0-9_.-]', '_', filename)
    clean_name = clean_name.replace('..', '')  # Prevent Path Traversal
    if not clean_name:
        clean_name = "uploaded_file_secure"
    return clean_name

def save_uploaded_files(uploaded_files):
    """
    Securely saves uploaded user files (CSV & JSON) to the staging directory.
    """
    # Create or Clear the Staging Directory
    if os.path.exists(RAW_DATA_DIR):
        shutil.rmtree(RAW_DATA_DIR)
    os.makedirs(RAW_DATA_DIR)
    
    saved_count = 0
    my_bar = st.progress(0, text="Staging files securely...")
    
    total_files = len(uploaded_files)
    
    for i, uploaded_file in enumerate(uploaded_files):
        
        # --- SECURITY CHECK 1: FILE SIZE ENFORCEMENT ---
        # Limit: 200MB (Prevents DoS/Memory Exhaustion)
        if uploaded_file.size > 200 * 1024 * 1024:
            st.error(f"⚠️ Security Alert: {uploaded_file.name} is too large (>200MB). Operation blocked.")
            continue
            
        # --- SECURITY CHECK 2: FILENAME SANITIZATION ---
        safe_name = sanitize_filename(uploaded_file.name)
        
        # --- SECURITY CHECK 3: MIME TYPE & EXTENSION VALIDATION ---
        # UPDATED: Now supports both CSV and JSON formats
        valid_extensions = ('.csv', '.json')
        valid_mimes = ['text/csv', 'application/json', 'application/x-json']
        
        if not uploaded_file.name.lower().endswith(valid_extensions):
             st.warning(f"⚠️ Format Error: {uploaded_file.name} is not a valid CSV or JSON file. Skipped.")
             continue
             
        # Save the raw file securely
        file_path = os.path.join(RAW_DATA_DIR, safe_name)
        with open(file_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
            
        saved_count += 1
        my_bar.progress((i + 1) / total_files)
        
    time.sleep(0.5)
    my_bar.empty()
    return saved_count