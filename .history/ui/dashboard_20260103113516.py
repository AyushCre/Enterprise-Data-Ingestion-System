import streamlit as st
import os
import shutil
import re
import time

# --- CONFIGURATION ---
RAW_DATA_DIR = "raw_data"

def sanitize_filename(filename):
    """
    Security Utility: Sanitizes the filename by removing dangerous characters.
    It strictly allows only A-Z, 0-9, _, ., and - to prevent shell injection risks.
    """
    clean_name = re.sub(r'[^a-zA-Z0-9_.-]', '_', filename)
    clean_name = clean_name.replace('..', '')
    if not clean_name:
        clean_name = "uploaded_file.csv"
    return clean_name

def save_uploaded_files(uploaded_files):
    """
    Securely saves uploaded user files (CSV ONLY) to the staging directory.
    """
    if os.path.exists(RAW_DATA_DIR):
        shutil.rmtree(RAW_DATA_DIR)
    os.makedirs(RAW_DATA_DIR)
    
    saved_count = 0
    my_bar = st.progress(0, text="Staging files securely...")
    
    total_files = len(uploaded_files)
    for i, uploaded_file in enumerate(uploaded_files):
        
        # --- SECURITY CHECK 1: FILE SIZE ---
        if uploaded_file.size > 200 * 1024 * 1024:
            st.error(f"⚠️ Security Alert: {uploaded_file.name} is too large (>200MB). Skipped.")
            continue
            
        # --- SECURITY CHECK 2: FILENAME SANITIZATION ---
        safe_name = sanitize_filename(uploaded_file.name)
        
        # --- SECURITY CHECK 3: CONTENT TYPE (CSV Only) ---
        if uploaded_file.type != "text/csv" and not uploaded_file.name.endswith('.csv'):
             st.warning(f"⚠️ Security Alert: {uploaded_file.name} is not a valid CSV. Skipped.")
             continue

        file_path = os.path.join(RAW_DATA_DIR, safe_name)
        with open(file_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        saved_count += 1
        my_bar.progress((i + 1) / total_files)
        
    time.sleep(0.5)
    my_bar.empty()
    return saved_count

def render_dashboard():
    """
    Main UI Layout
    """
    st.title("🛡️ Secure CSV to Excel Converter")
    st.markdown("### Enterprise-Grade Data Transformation")
    
    uploaded_files = st.file_uploader("Upload CSV Files", type=['csv'], accept_multiple_files=True)
    
    if uploaded_files:
        count = save_uploaded_files(uploaded_files)
        if count > 0:
            st.success(f"✅ {count} Files Uploaded Securely.")
            
            # Button to trigger processing (Logic processor.py se connect hoga)
            if st.button("Convert to Excel"):
                from modules.processor import process_files
                with st.spinner("Converting files..."):
                    result_msg = process_files()
                st.success(result_msg)