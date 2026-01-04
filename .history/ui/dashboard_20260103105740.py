import streamlit as st
import pandas as pd
import os
import shutil
import time
# Importing paths from config
from config import RAW_DATA_DIR, PROCESSED_DATA_DIR
from modules.processor import process_sequential, process_parallel

def save_uploaded_files(uploaded_files):
    """
    Helper function to save uploaded user files into the raw_data directory.
    """
    # Clear existing data to avoid mixing old and new batches
    if os.path.exists(RAW_DATA_DIR):
        shutil.rmtree(RAW_DATA_DIR)
    os.makedirs(RAW_DATA_DIR)
    
    saved_count = 0
    # Progress bar for uploading
    my_bar = st.progress(0, text="Staging files...")
    
    total_files = len(uploaded_files)
    for i, uploaded_file in enumerate(uploaded_files):
        # Save bytes to actual file
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
    
    # Custom CSS for professional styling
    st.markdown("""
        <style>
        .metric-card { border-left: 5px solid #00CC96; background-color: #f0f2f6; }
        </style>
    """, unsafe_allow_html=True)

    st.title("⚡ Enterprise Batch Data Pipeline")
    st.markdown("**High-Performance Computing Demo**")
    st.markdown("---")

    # --- Sidebar Configuration ---
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

    # --- SECTION 1: DATA INGESTION (Upload) ---
    st.subheader("1. Data Ingestion")
    uploaded_files = st.file_uploader("Upload CSV Files", type=['csv'], accept_multiple_files=True)
    
    if uploaded_files:
        if st.button(f"📥 Stage {len(uploaded_files)} Files"):
            count = save_uploaded_files(uploaded_files)
            st.success(f"Staged {count} files ready for processing.")

    st.markdown("---")

    # --- SECTION 2: PROCESSING ENGINE (Benchmark) ---
    st.subheader("2. Processing Engine")
    
    # Check for available files
    files = []
    if os.path.exists(RAW_DATA_DIR):
        files = [os.path.join(RAW_DATA_DIR, f) for f in os.listdir(RAW_DATA_DIR) if f.endswith('.csv')]
    
    if not files:
        st.warning("⚠️ No files found. Please upload first.")
    else:
        st.write(f"Queue Size: **{len(files)} files**")
        
        col_actions1, col_actions2 = st.columns(2)
        
        # BUTTON 1: Production Mode (Parallel Only)
        with col_actions1:
            prod_run = st.button("🚀 Run Production Mode (Parallel Only)", type="primary", use_container_width=True)
            
        # BUTTON 2: Benchmark Comparison
        with col_actions2:
            comp_run = st.button("⚔️ Run Benchmark Comparison", use_container_width=True)

        # Logic for Production Mode
        if prod_run:
            st.markdown("### ⚡ Production Execution")
            bar = st.progress(0)
            t_par = process_parallel(files, lambda p, t: bar.progress(p, text=t))
            st.balloons()
            st.success(f"✅ Job Completed in **{t_par:.2f} seconds** using Parallel Architecture.")

        # Logic for Comparison Mode
        if comp_run:
            col1, col2 = st.columns(2)
            
            # Parallel Run (First)
            with col1:
                st.markdown("### ⚡ Parallel (Optimized)")
                bar1 = st.progress(0)
                t2 = process_parallel(files, lambda p, t: bar1.progress(p, text=t))
                st.metric("Execution Time", f"{t2:.2f} s")

            # Sequential Run (Second)
            with col2:
                st.markdown("### 🐢 Sequential (Legacy)")
                bar2 = st.progress(0)
                t1 = process_sequential(files, lambda p, t: bar2.progress(p, text=t))
                st.metric("Execution Time", f"{t1:.2f} s", delta=f"+{t1-t2:.2f} s (Slower)")

            # ROI Calculation
            st.divider()
            if t2 > 0:
                speedup = t1 / t2
                st.success(f"🚀 **Architecture Win:** Parallel Processing was **{speedup:.1f}x Faster** than Sequential.")

    st.markdown("---")
    
    # --- SECTION 3: DATA QUALITY INSPECTOR (NEW FEATURE) ---
    st.subheader("3. Data Quality Inspector")
    st.markdown("Inspect processed data without opening raw CSV files.")
    
    # List processed files
    processed_files = []
    if os.path.exists(PROCESSED_DATA_DIR):
        processed_files = [f for f in os.listdir(PROCESSED_DATA_DIR) if f.endswith('.csv')]
    
    if processed_files:
        # Dropdown to select a file
        selected_file = st.selectbox("Select a processed file to inspect:", processed_files)
        
        if selected_file:
            file_path = os.path.join(PROCESSED_DATA_DIR, selected_file)
            # Read and display data
            df_preview = pd.read_csv(file_path)
            
            st.dataframe(df_preview.head(100), use_container_width=True)
            st.caption(f"Showing first 100 rows of {len(df_preview)} records from {selected_file}.")
    else:
        st.info("No processed data available yet. Please run a job above to generate data.")