import streamlit as st
import pandas as pd
import os
import shutil
import time
from config import RAW_DATA_DIR
from modules.processor import process_sequential, process_parallel

def save_uploaded_files(uploaded_files):
    """Helper to save uploaded files to disk"""
    # Pehle purana data saaf karo
    if os.path.exists(RAW_DATA_DIR):
        shutil.rmtree(RAW_DATA_DIR)
    os.makedirs(RAW_DATA_DIR)
    
    saved_count = 0
    progress_text = "Saving files to workspace..."
    my_bar = st.progress(0, text=progress_text)
    
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
    st.set_page_config(page_title="High-Perf Data Pipeline", page_icon="⚡", layout="wide")
    
    st.markdown("""
        <style>
        .metric-card { border-left: 5px solid #ff4b4b; background-color: #f0f2f6; }
        </style>
    """, unsafe_allow_html=True)

    st.title("⚡ Enterprise Batch Data Pipeline")
    st.markdown("**Upload multiple CSV files to benchmark processing speed.**")
    st.markdown("---")

    # --- Sidebar ---
    with st.sidebar:
        st.header("⚙️ Configuration")
        st.info("Upload CSV files containing columns like 'Amount', 'Product_Name', etc.")
        
        if st.button("🧹 Clear Workspace"):
            if os.path.exists(RAW_DATA_DIR):
                shutil.rmtree(RAW_DATA_DIR)
                os.makedirs(RAW_DATA_DIR)
            st.toast("Workspace Cleared!", icon="🗑️")

    # --- SECTION 1: DATA UPLOAD (YE NAYA HAI) ---
    st.subheader("1. Data Ingestion (Upload Files)")
    
    # Allow multiple files
    uploaded_files = st.file_uploader(
        "Drop your CSV files here (Select multiple)", 
        type=['csv'], 
        accept_multiple_files=True
    )
    
    if uploaded_files:
        if st.button(f"📥 Process {len(uploaded_files)} Files", use_container_width=True):
            count = save_uploaded_files(uploaded_files)
            st.success(f"Successfully staged {count} files in '{RAW_DATA_DIR}' for processing.")

    st.markdown("---")

    # --- SECTION 2: BENCHMARK (Ye wahi rahega) ---
    st.subheader("2. Performance Benchmark")
    
    # Check if files exist in folder
    files = [os.path.join(RAW_DATA_DIR, f) for f in os.listdir(RAW_DATA_DIR) if f.endswith('.csv')]
    
    if not files:
        st.warning("⚠️ No files found. Please upload CSVs above.")
    else:
        st.write(f"Ready to process **{len(files)}** files.")
        
        if st.button("⚔️ Run Comparison Test", type="primary", use_container_width=True):
            col1, col2 = st.columns(2)
            
            # Sequential
            with col1:
                st.markdown("### 🐢 Sequential")
                bar1 = st.progress(0)
                t1 = process_sequential(files, lambda p, t: bar1.progress(p, text=t))
                st.metric("Execution Time", f"{t1:.2f} s")

            # Parallel
            with col2:
                st.markdown("### ⚡ Parallel")
                bar2 = st.progress(0)
                t2 = process_parallel(files, lambda p, t: bar2.progress(p, text=t))
                st.metric("Execution Time", f"{t2:.2f} s", delta=f"{t1-t2:.2f} s") # Green if faster

            # ROI Logic
            st.divider()
            if t2 > 0 and t1 > t2:
                speedup = t1 / t2
                st.success(f"🚀 **Success:** Parallel processing is **{speedup:.1f}x faster**!")
            elif t2 >= t1:
                st.info("ℹ️ **Note:** For small datasets/files, Parallel overhead might make it slower. Try larger files to see the benefit.")