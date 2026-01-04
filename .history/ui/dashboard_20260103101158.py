import streamlit as st
import pandas as pd
import os
import shutil
from config import RAW_DATA_DIR
from modules.generator import generate_bulk_data
from modules.processor import process_sequential, process_parallel

def render_dashboard():
    # --- UI Setup ---
    st.set_page_config(page_title="High-Perf Data Pipeline", page_icon="⚡", layout="wide")
    
    # CSS Styling
    st.markdown("""
        <style>
        .metric-card { border-left: 5px solid #ff4b4b; background-color: #f0f2f6; }
        </style>
    """, unsafe_allow_html=True)

    st.title("⚡ Enterprise Batch Data Pipeline")
    st.markdown("**Architecture Demo:** Comparing Sequential vs Parallel Execution")
    st.markdown("---")

    # --- Sidebar ---
    with st.sidebar:
        st.header("⚙️ Configuration")
        num_files = st.slider("Dataset Size", 100, 2000, 500, 100)
        
        if st.button("Reset Environment"):
            # Clean folders
            if os.path.exists(RAW_DATA_DIR):
                shutil.rmtree(RAW_DATA_DIR)
                os.makedirs(RAW_DATA_DIR)
            st.toast("Environment Cleared!", icon="🧹")

    # --- Main Section 1: Generation ---
    st.subheader("1. Data Ingestion")
    if st.button("🚀 Generate Raw Data", use_container_width=True):
        progress_bar = st.progress(0, text="Starting...")
        # Passing a lambda function as a callback to update UI
        generate_bulk_data(num_files, lambda p, t: progress_bar.progress(p, text=t))
        st.success(f"Generated {num_files} files in '{RAW_DATA_DIR}'")

    st.markdown("---")

    # --- Main Section 2: Benchmark ---
    st.subheader("2. Performance Benchmark")
    if st.button("⚔️ Run Comparison", type="primary", use_container_width=True):
        files = [os.path.join(RAW_DATA_DIR, f) for f in os.listdir(RAW_DATA_DIR) if f.endswith('.csv')]
        
        if not files:
            st.error("No data found! Generate data first.")
            return

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
            st.metric("Execution Time", f"{t2:.2f} s", delta=f"-{t1-t2:.2f} s")

        # ROI Metrics
        st.divider()
        st.subheader("3. Efficiency ROI")
        if t2 > 0:
            speedup = t1 / t2
            st.info(f"🚀 **Results:** Parallel processing is **{speedup:.1f}x faster** than the traditional approach.")