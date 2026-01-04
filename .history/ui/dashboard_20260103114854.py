import streamlit as st
import pandas as pd
import os
import shutil
import time
import re
from config import RAW_DATA_DIR, PROCESSED_DATA_DIR
from modules.processor import process_sequential, process_parallel

def save_uploaded_files(uploaded_files):
    """
    Helper function to save uploaded user files into the raw_data directory.
    """
    if os.path.exists(RAW_DATA_DIR):
        shutil.rmtree(RAW_DATA_DIR)
    os.makedirs(RAW_DATA_DIR)
    
    saved_count = 0
    my_bar = st.progress(0, text="Staging files...")
    
    total_files = len(uploaded_files)
    for i, uploaded_file in enumerate(uploaded_files):
        file_path = os.path.join(RAW_DATA_DIR, uploaded_file.name)
        with open(file_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        saved_count += 1
        my_bar.progress((i + 1) / total_files)
        
    time.sleep(0.5)
    my_bar.empty()
    return saved_count

def render_dashboard():
    """
    Main function to render the Streamlit UI.
    """
    st.set_page_config(page_title="High-Perf Data Pipeline", page_icon="⚡", layout="wide")
    
    st.markdown("""
        <style>
        .metric-card { border-left: 5px solid #00CC96; background-color: #f0f2f6; }
        </style>
    """, unsafe_allow_html=True)

    st.title("⚡ Enterprise Batch Data Pipeline")
    st.markdown("**High-Performance Computing Demo**")
    st.markdown("---")

    # --- Sidebar ---
    with st.sidebar:
        st.header("⚙️ Configuration")
        if st.button("🧹 Clear Workspace"):
            if os.path.exists(RAW_DATA_DIR):
                shutil.rmtree(RAW_DATA_DIR)
                os.makedirs(RAW_DATA_DIR)
            if os.path.exists(PROCESSED_DATA_DIR):
                shutil.rmtree(PROCESSED_DATA_DIR)
                os.makedirs(PROCESSED_DATA_DIR)
            st.toast("Workspace Cleared!", icon="🗑️")

    # --- SECTION 1: UPLOAD ---
    st.subheader("1. Data Ingestion")
    # CHANGE 1: Accept CSV and JSON both
    uploaded_files = st.file_uploader("Upload Data Files", type=['csv', 'json'], accept_multiple_files=True)
    
    if uploaded_files:
        if st.button(f"📥 Stage {len(uploaded_files)} Files"):
            count = save_uploaded_files(uploaded_files)
            st.success(f"Staged {count} files ready for processing.")

    st.markdown("---")

    # --- SECTION 2: PROCESSING ---
    st.subheader("2. Processing Engine")
    
    files = []
    if os.path.exists(RAW_DATA_DIR):
        # CHANGE 2: Look for both .csv and .json files
        files = [os.path.join(RAW_DATA_DIR, f) for f in os.listdir(RAW_DATA_DIR) 
                 if f.endswith(('.csv', '.json'))]
    
    if not files:
        st.warning("⚠️ No files found. Please upload first.")
    else:
        st.write(f"Queue Size: **{len(files)} files**")
        
        col_actions1, col_actions2 = st.columns(2)
        
        with col_actions1:
            prod_run = st.button("🚀 Run Production Mode (Parallel Only)", type="primary", use_container_width=True)
            
        with col_actions2:
            comp_run = st.button("⚔️ Run Benchmark Comparison", use_container_width=True)

        if prod_run:
            st.markdown("### ⚡ Production Execution")
            bar = st.progress(0)
            t_par = process_parallel(files, lambda p, t: bar.progress(p, text=t))
            st.balloons()
            st.success(f"✅ Job Completed in **{t_par:.2f} seconds** using Parallel Architecture.")

        if comp_run:
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("### ⚡ Parallel (Optimized)")
                bar1 = st.progress(0)
                t2 = process_parallel(files, lambda p, t: bar1.progress(p, text=t))
                st.metric("Execution Time", f"{t2:.2f} s")

            with col2:
                st.markdown("### 🐢 Sequential (Legacy)")
                bar2 = st.progress(0)
                t1 = process_sequential(files, lambda p, t: bar2.progress(p, text=t))
                st.metric("Execution Time", f"{t1:.2f} s", delta=f"+{t1-t2:.2f} s (Slower)")

            st.divider()
            if t2 > 0:
                speedup = t1 / t2
                st.success(f"🚀 **Architecture Win:** Parallel Processing was **{speedup:.1f}x Faster** than Sequential.")

    st.markdown("---")
    
    # --- SECTION 3: DATA INSPECTOR ---
    st.subheader("3. Data Quality Inspector")
    st.markdown("Inspect processed data without opening raw files.")
    
    processed_files = []
    if os.path.exists(PROCESSED_DATA_DIR):
        processed_files = [f for f in os.listdir(PROCESSED_DATA_DIR) if f.endswith('.csv')]
    
    def natural_keys(text):
        return [int(c) if c.isdigit() else c for c in re.split(r'(\d+)', text)]
    
    processed_files.sort(key=natural_keys)

    if processed_files:
        selected_file = st.selectbox(
            "Select or Type a filename (e.g. type '488'):", 
            processed_files
        )
        
        if selected_file:
            file_path = os.path.join(PROCESSED_DATA_DIR, selected_file)
            df_preview = pd.read_csv(file_path)
            
            st.dataframe(df_preview.head(100), use_container_width=True)
            st.caption(f"Showing first 100 rows of {len(df_preview)} records from {selected_file}.")
    else:
        st.info("No processed data available yet. Please run a job above to generate data.")