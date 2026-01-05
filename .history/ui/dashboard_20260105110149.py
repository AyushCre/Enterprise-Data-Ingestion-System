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

def save_uploaded_files(uploaded_files):
    """
    Persists uploaded files to the local raw data directory after SECURITY VALIDATION.
    Returns the count of successfully saved files.
    """
    # Clear existing raw data to avoid mixing batches
    if os.path.exists(RAW_DATA_DIR):
        shutil.rmtree(RAW_DATA_DIR)
    os.makedirs(RAW_DATA_DIR)
    
    saved_count = 0
    rejected_count = 0
    progress_bar = st.progress(0, text="Initializing Security Protocols...")
    
    total_files = len(uploaded_files)
    
    for i, uploaded_file in enumerate(uploaded_files):
        # --- SECURITY CHECKPOINT ---
        # Inspect file for malicious patterns
        is_safe, reason = SecurityInspector.inspect_file(uploaded_file)
        
        if is_safe:
            # Only save if the file is clean
            file_path = os.path.join(RAW_DATA_DIR, uploaded_file.name)
            with open(file_path, "wb") as f:
                f.write(uploaded_file.getbuffer())
            saved_count += 1
            status_text = f"Scanning & Staging: {uploaded_file.name} (Clean)"
        else:
            # Reject the file and log the reason
            rejected_count += 1
            status_text = f"⚠️ BLOCKED: {uploaded_file.name} - {reason}"
            print(f"SECURITY ALERT: {status_text}") 
            
        progress_bar.progress((i + 1) / total_files, text=status_text)
        
    time.sleep(0.5)
    progress_bar.empty()
    
    # Notify user if some files were blocked
    if rejected_count > 0:
        st.error(f"Security Alert: {rejected_count} files were blocked due to malicious patterns.")
        
    return saved_count

def natural_sort_key(text):
    """
    Helper function to enable natural sorting of filenames containing numbers.
    Example: Ensures 'file_2.csv' comes before 'file_10.csv'.
    """
    return [int(c) if c.isdigit() else c for c in re.split(r'(\d+)', text)]

def create_merged_zip():
    """
    Reads all processed CSV files, merges them into a single DataFrame,
    saves it as one master CSV, and returns the path to a ZIP containing that single file.
    """
    # 1. Identify all processed CSV files
    all_files = [os.path.join(PROCESSED_DATA_DIR, f) for f in os.listdir(PROCESSED_DATA_DIR) if f.endswith('.csv')]
    
    if not all_files:
        return None

    # 2. Read and Merge (Concatenate) Data
    # Using list comprehension for performance
    df_list = [pd.read_csv(f) for f in all_files]
    
    if not df_list:
        return None
        
    merged_df = pd.concat(df_list, ignore_index=True)
    
    # 3. Save as a single Master CSV
    master_csv_name = "Enterprise_Consolidated_Data.csv"
    merged_df.to_csv(master_csv_name, index=False)
    
    # 4. Create a ZIP file containing ONLY the master CSV
    zip_filename = "Enterprise_Full_Report.zip"
    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(master_csv_name)
    
    # Clean up the temporary CSV file to keep workspace clean
    os.remove(master_csv_name)
    
    return zip_filename

def render_dashboard():
    """
    Main entry point for the Streamlit Dashboard UI.
    Orchestrates Data Ingestion, Processing, Analytics, and Export.
    """
    st.set_page_config(page_title="Enterprise Data Pipeline", page_icon="⚡", layout="wide")
    
    # Custom CSS for metrics
    st.markdown("""
        <style>
        .metric-card { border-left: 5px solid #00CC96; background-color: #f0f2f6; }
        </style>
    """, unsafe_allow_html=True)

    st.title("⚡ Enterprise Batch Data Pipeline")
    st.markdown("**High-Performance Computing & Data Ingestion System**")
    st.markdown("---")

    # --- Sidebar: System Controls ---
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

    # --- MODULE 1: DATA INGESTION ---
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

    # --- MODULE 2: PROCESSING ENGINE ---
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
                
                # Compare Logic: Determine if faster or slower
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
                    st.warning(f"⚠️ **Performance Insight:** Parallel processing was slower ({speedup_factor:.2f}x). Overhead exceeds benefits for small datasets.")

    st.markdown("---")
    
    # --- MODULE 3: ANALYTICS & EXPORT (UPDATED) ---
    st.subheader("3. Analytics & Export Module")
    st.markdown("Real-time visualization and artifact retrieval.")
    
    processed_files = []
    if os.path.exists(PROCESSED_DATA_DIR):
        processed_files = [f for f in os.listdir(PROCESSED_DATA_DIR) if f.endswith('.csv')]
    
    processed_files.sort(key=natural_sort_key)

    if processed_files:
        # --- NEW MERGED DOWNLOAD SECTION ---
        with st.container():
            col_bulk_info, col_bulk_btn = st.columns([3, 1])
            with col_bulk_info:
                st.info(f"✅ **Batch Ready:** {len(processed_files)} processed files available for consolidation.")
                st.caption("This will merge all individual files into a single master dataset.")
            
            with col_bulk_btn:
                # On button click, we generate the merged zip
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
        # -----------------------------------

        col_sel, col_down = st.columns([3, 1])
        
        with col_sel:
            selected_file = st.selectbox(
                "Inspect Individual Artifact:", 
                processed_files
            )

        if selected_file:
            file_path = os.path.join(PROCESSED_DATA_DIR, selected_file)
            try:
                df_preview = pd.read_csv(file_path)
                
                tab1, tab2 = st.tabs(["📄 Data View", "📊 Visual Analytics"])
                
                with tab1:
                    st.dataframe(df_preview.head(100), use_container_width=True)
                    st.caption(f"Displaying top 100 records from {selected_file}. Total Records: {len(df_preview)}")
                
                with tab2:
                    if 'Region' in df_preview.columns and 'Total_Amount' in df_preview.columns:
                        st.markdown("#### Regional Revenue Distribution")
                        chart_data = df_preview.groupby('Region')['Total_Amount'].sum()
                        st.bar_chart(chart_data, color="#00CC96")
                        
                        st.markdown("#### Key Performance Indicators")
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