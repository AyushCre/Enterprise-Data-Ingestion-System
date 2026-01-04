import streamlit as st
import os
import shutil
import re
import time
import pandas as pd
import json

# --- CONFIGURATION ---
RAW_DATA_DIR = "raw_data"
PROCESSED_DATA_DIR = "processed_data"

def sanitize_filename(filename):
    """
    Security Utility: Removes dangerous characters to prevent hacking.
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
        
        # --- SECURITY CHECK 1: FILE SIZE ---
        if uploaded_file.size > 200 * 1024 * 1024:
            st.error(f"⚠️ Security Alert: {uploaded_file.name} is too large (>200MB). Blocked.")
            continue
            
        # --- SECURITY CHECK 2: FILENAME SANITIZATION ---
        safe_name = sanitize_filename(uploaded_file.name)
        
        # --- SECURITY CHECK 3: FORMAT VALIDATION ---
        valid_extensions = ('.csv', '.json')
        if not uploaded_file.name.lower().endswith(valid_extensions):
             st.warning(f"⚠️ Format Error: {uploaded_file.name} is not valid. Skipped.")
             continue
             
        file_path = os.path.join(RAW_DATA_DIR, safe_name)
        with open(file_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
            
        saved_count += 1
        my_bar.progress((i + 1) / total_files)
        
    time.sleep(0.5)
    my_bar.empty()
    return saved_count

def process_and_clean_data():
    """
    Converts JSON/CSV to Excel and Cleans Data.
    """
    if not os.path.exists(PROCESSED_DATA_DIR):
        os.makedirs(PROCESSED_DATA_DIR)
        
    files = os.listdir(RAW_DATA_DIR)
    processed_count = 0
    
    for filename in files:
        file_path = os.path.join(RAW_DATA_DIR, filename)
        try:
            # --- JSON & CSV LOGIC ---
            if filename.endswith('.json'):
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                # Handle Nested JSON
                if isinstance(data, list):
                    df = pd.json_normalize(data)
                else:
                    df = pd.json_normalize([data])
            elif filename.endswith('.csv'):
                df = pd.read_csv(file_path)
            else:
                continue

            # --- DATA CLEANING ---
            df.dropna(how='all', inplace=True) # Empty rows hatao
            df.fillna("N/A", inplace=True)     # Empty cells bharo
            
            output_name = f"Cleaned_{os.path.splitext(filename)[0]}.xlsx"
            output_path = os.path.join(PROCESSED_DATA_DIR, output_name)
            df.to_excel(output_path, index=False)
            processed_count += 1
            
        except Exception as e:
            st.error(f"Error processing {filename}: {e}")
            
    return processed_count

# --- YE HAI WO FUNCTION JO TUMHARA MISSING THA ---
def render_dashboard():
    """
    Main UI Function called by main.py
    """
    st.title("🛡️ Secure Data Transformation Engine")
    st.markdown("### Enterprise-Grade JSON/CSV to Excel Converter")
    
    # File Uploader
    uploaded_files = st.file_uploader("Upload Secured Data", 
                                      type=['csv', 'json'], 
                                      accept_multiple_files=True)
    
    if uploaded_files:
        # Step 1: Secure Save
        count = save_uploaded_files(uploaded_files)
        
        if count > 0:
            st.success(f"✅ {count} Files Uploaded Securely (Sanitized & Checked).")
            
            # Step 2: Process Button
            if st.button("🚀 Process & Convert to Excel"):
                with st.spinner("Normalizing Nested JSON & Cleaning Data..."):
                    p_count = process_and_clean_data()
                st.success(f"🎉 Success! {p_count} files converted to Clean Excel.")
                st.balloons()