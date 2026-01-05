import streamlit as st
import pandas as pd
import os
import shutil
import time
import re
import zipfile
from config import RAW_DATA_DIR, PROCESSED_DATA_DIR
from modules.processor import process_sequential, process_parallel
from modules.security import SecurityInspector

# --- Helper Functions ---

def natural_sort_key(text):
    """
    Helper function for natural sorting (e.g., TXN-2 comes before TXN-10).
    """
    return [int(c) if c.isdigit() else c for c in re.split(r'(\d+)', str(text))]

def save_uploaded_files(uploaded_files):
    """
    Saves uploaded files to disk after security checks.
    """
    if os.path.exists(RAW_DATA_DIR):
        shutil.rmtree(RAW_DATA_DIR)
    os.makedirs(RAW_DATA_DIR)
    
    saved_count = 0
    rejected_count = 0
    progress_bar = st.progress(0, text="Initializing Security Protocols...")
    total_files = len(uploaded_files)
    
    for i, uploaded_file in enumerate(uploaded_files):
        is_safe, reason = SecurityInspector.inspect_file(uploaded_file)
        
        if is_safe:
            file_path = os.path.join(RAW_DATA_DIR, uploaded_file.name)
            with open(file_path, "wb") as f:
                f.write(uploaded_file.getbuffer())
            saved_count += 1
            status_text = f"Scanning & Staging: {uploaded_file.name} (Clean)"
        else:
            rejected_count += 1
            status_text = f"⚠️ BLOCKED: {uploaded_file.name} - {reason}"
            print(f"SECURITY ALERT: {status_text}") 
            
        progress_bar.progress((i + 1) / total_files, text=status_text)
        
    time.sleep(0.5)
    progress_bar.empty()
    
    if rejected_count > 0:
        st.error(f"Security Alert: {rejected_count} files were blocked due to malicious patterns.")
        
    return saved_count

def create_merged_zip():
    """
    Memory-Efficient Merging (Chunking Method):
    Reads files one by one, writes to disk, and clears RAM immediately.
    Allows creating massive (1GB+) CSVs without crashing the system.
    """
    # 1. Get all processed files
    all_files = [os.path.join(PROCESSED_DATA_DIR, f) for f in os.listdir(PROCESSED_DATA_DIR) if f.endswith('.csv')]
    
    if not all_files:
        return None

    # 2. Sort files to maintain serial order (e.g., file_1, file_2...)
    all_files.sort(key=lambda x: natural_sort_key(os.path.basename(x)))

    master_csv_name = "Enterprise_Consolidated_Data.csv"
    
    # 3. Process Chunk-by-Chunk
    first_file = True
    
    # Open the master file once in write mode
    with open(master_csv_name, 'w', encoding='utf-8') as outfile:
        for file_path in all_files:
            try:
                # Load ONLY one small file into RAM
                df_chunk = pd.read_csv(file_path)
                
                # Sort inside the chunk if needed
                if 'Transaction_ID' in df_chunk.columns:
                     df_chunk.sort_values(
                        by='Transaction_ID', 
                        key=lambda x: x.map(natural_sort_key), 
                        inplace=True
                    )
                
                # Write to disk (Header included only for the first chunk)
                df_chunk.to_csv(outfile, index=False, header=first_file)
                
                first_file = False 
                
                # Free up RAM immediately
                del df_chunk 
                
            except Exception as e:
                print(f"Skipping corrupt file {file_path}: {e}")

    # 4. Create ZIP from the Master CSV
    zip_filename = "Enterprise_Full_Report.zip"
    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(master_csv_name)
    
    # Remove the temporary raw CSV to save space
    if os.path.exists(master_csv_name):
        os.remove(master_csv_name)
    
    return zip_filename

# --- Main Dashboard Logic ---

def render_dashboard():
    st.set_page_config(page_title="Enterprise Data Pipeline", page_icon="⚡", layout="wide")
    
    st.markdown("""
        <style>
        .metric-card { border-left: 5px solid #00CC96; background-color: #f0f2f6; }
        </style>
    """, unsafe_allow_html=True)

    st.title("⚡ Enterprise Batch Data Pipeline")
    st.markdown("**High-Performance Computing & Data Ingestion System**")
    st.markdown("---")

    # Sidebar
    with st.sidebar:
        st.header("⚙️ System Configuration")
        if st.button("🧹 Reset Workspace"):
            if os.path.exists(RAW_DATA_DIR):
                shutil.rmtree(RAW_DATA_DIR)
                os.makedirs(RAW_DATA_DIR)
            if os.path.exists(PROCESSED_DATA_DIR):
                shutil.rmtree(PROCESSED_DATA_DIR)
                os.makedirs(PROCESSED_DATA_DIR)
            st.toast("Workspace reset successfully.", icon="🗑️")

    # Module 1: Ingestion
    st.subheader("1. Data Ingestion Module")
    uploaded_files = st.file_uploader(
        "Upload Source Files (CSV/JSON)", 
        type=['csv', 'json'], 
        accept_multiple_files=True
    )
    
    if uploaded_files:
        if st.button(f"📥 Stage {len(uploaded_files)} Files"):
            count = save_uploaded_files(uploaded_files)
            if count > 0:
                st.success(f"Successfully staged {count} secure files for processing.")
            else:
                st.warning("No files were staged. All files failed security validation.")

    st.markdown("---")

    # Module 2: Processing
    st.subheader("2. Processing Orchestration Engine")
    
    files = []
    if os.path.exists(RAW_DATA_DIR):
        files = [os.path.join(RAW_DATA_DIR, f) for f in os.listdir(RAW_DATA_DIR) 
                 if f.endswith(('.csv', '.json'))]
    
    if not files:
        st.warning("⚠️ No data found in staging area. Please upload files to proceed.")
    else:
        st.write(f"Queue Status: **{len(files)} items pending**")
        
        col_prod, col_benchmark = st.columns(2)
        with col_prod:
            prod_run = st.button("🚀 Execute Production Pipeline (Parallel)", type="primary", use_container_width=True)
        with col_benchmark:
            comp_run = st.button("⚔️ Run Performance Benchmark", use_container_width=True)

        if prod_run:
            st.markdown("### ⚡ Execution Status")
            progress_bar = st.progress(0)
            execution_time = process_parallel(files, lambda p, t: progress_bar.progress(p, text=t))
            st.balloons()
            st.success(f"✅ Pipeline completed in **{execution_time:.2f} seconds** using Parallel Architecture.")

        if comp_run:
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("### ⚡ Parallel (Optimized)")
                bar1 = st.progress(0)
                t_par = process_parallel(files, lambda p, t: bar1.progress(p, text=t))
                st.metric("Latency", f"{t_par:.4f} s")
            with col2:
                st.markdown("### 🐢 Sequential (Legacy)")
                bar2 = st.progress(0)
                t_seq = process_sequential(files, lambda p, t: bar2.progress(p, text=t))
                
                diff = t_seq - t_par
                if diff > 0:
                    delta_msg = f"+{diff:.4f} s (Slower)"
                    delta_color_mode = "inverse" 
                else:
                    delta_msg = f"{diff:.4f} s (Faster)"
                    delta_color_mode = "inverse"
                st.metric("Latency", f"{t_seq:.4f} s", delta=delta_msg, delta_color=delta_color_mode)

            st.divider()
            if t_par > 0:
                speedup_factor = t_seq / t_par
                if speedup_factor >= 1.0:
                    st.success(f"🚀 **Performance Win:** Parallel architecture achieved a **{speedup_factor:.1f}x speedup** over legacy sequential processing.")
                else:
                    st.warning(f"⚠️ **Performance Insight:** Parallel processing was slower ({speedup_factor:.2f}x).")

    st.markdown("---")
    
    # Module 3: Analytics & Export
    st.subheader("3. Analytics & Export Module")
    
    processed_files = []
    if os.path.exists(PROCESSED_DATA_DIR):
        processed_files = [f for f in os.listdir(PROCESSED_DATA_DIR) if f.endswith('.csv')]
    
    processed_files.sort(key=natural_sort_key)

    if processed_files:
        # Merged Download Section
        with st.container():
            col_bulk_info, col_bulk_btn = st.columns([3, 1])
            with col_bulk_info:
                st.info(f"✅ **Batch Ready:** {len(processed_files)} processed files available for consolidation.")
                st.caption("Data will be consolidated and downloaded as a single Master Dataset.")
            
            with col_bulk_btn:
                # Generate Zip on Click
                zip_path = create_merged_zip()
                if zip_path:
                    with open(zip_path, "rb") as f:
                        st.download_button(
                            label="📦 Download Merged Data (ZIP)",
                            data=f,
                            file_name="Enterprise_Full_Report.zip",
                            mime="application/zip",
                            type="primary",
                            use_container_width=True
                        )
        st.divider()

        # Individual File Inspection
        col_sel, col_down = st.columns([3, 1])
        with col_sel:
            selected_file = st.selectbox("Inspect Individual Artifact:", processed_files)

        if selected_file:
            file_path = os.path.join(PROCESSED_DATA_DIR, selected_file)
            try:
                df_preview = pd.read_csv(file_path)
                tab1, tab2 = st.tabs(["📄 Data View", "📊 Visual Analytics"])
                
                with tab1:
                    st.dataframe(df_preview.head(100), use_container_width=True)
                
                with tab2:
                    if 'Region' in df_preview.columns and 'Total_Amount' in df_preview.columns:
                        st.markdown("#### Regional Revenue Distribution")
                        chart_data = df_preview.groupby('Region')['Total_Amount'].sum()
                        st.bar_chart(chart_data, color="#00CC96")
                        
                        m1, m2, m3 = st.columns(3)
                        m1.metric("Total Revenue", f"${df_preview['Total_Amount'].sum():,.2f}")
                        m2.metric("Avg Transaction", f"${df_preview['Total_Amount'].mean():,.2f}")
                        m3.metric("Total Transactions", len(df_preview))
                    else:
                        st.info("Visual analytics require 'Region' and 'Total_Amount' columns.")

                with col_down:
                    st.write("") 
                    st.write("") 
                    with open(file_path, "rb") as f:
                        st.download_button(
                            label="⬇️ Download File",
                            data=f,
                            file_name=selected_file,
                            mime="text/csv",
                            use_container_width=True
                        )
            except Exception as e:
                st.error(f"Error reading artifact: {e}")
    else:
        st.info("No processed artifacts available. Execute the pipeline to generate data.")

if __name__ == "__main__":
    render_dashboard()