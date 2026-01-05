import streamlit as st
import pandas as pd
import os
import shutil
import time
import re
import io
import zipfile
from config import RAWDATADIR, PROCESSEDDATADIR
from modules.processor import processsequential, processparallel, create_merged_table
from modules.security import SecurityInspector

def saveuploadedfiles(uploadedfiles):
    """
    Persists uploaded files to the local raw data directory after SECURITY VALIDATION.
    """
    if os.path.exists(RAWDATADIR):
        shutil.rmtree(RAWDATADIR)
    os.makedirs(RAWDATADIR)
    
    savedcount = 0
    rejectedcount = 0
    progressbar = st.progress(0, text="Initializing Security Protocols...")
    totalfiles = len(uploadedfiles)
    
    for i, uploadedfile in enumerate(uploadedfiles):
        # --- SECURITY CHECKPOINT ---
        issafe, reason = SecurityInspector.inspectfile(uploadedfile)
        
        if issafe:
            # Only save if the file is clean
            filepath = os.path.join(RAWDATADIR, uploadedfile.name)
            with open(filepath, "wb") as f:
                f.write(uploadedfile.getbuffer())
            savedcount += 1
            statustext = f"Scanning & Staging: {uploadedfile.name} (Clean)"
        else:
            # Reject the file
            rejectedcount += 1
            statustext = f"⚠️ BLOCKED: {uploadedfile.name} - {reason}"
            print(f"SECURITY ALERT: {statustext}")
        
        progressbar.progress((i + 1) / totalfiles, text=statustext)
        time.sleep(0.5)
    
    progressbar.empty()
    
    # Notify user if some files were blocked
    if rejectedcount > 0:
        st.error(f"Security Alert: {rejectedcount} files were blocked due to malicious patterns.")
    
    return savedcount

def naturalsortkey(text):
    """
    Helper function to enable natural sorting of filenames containing numbers.
    """
    return [int(c) if c.isdigit() else c for c in re.split(r'(\d+)', text)]

def renderdashboard():
    """
    Main entry point for the Streamlit Dashboard UI.
    Orchestrates Data Ingestion, Processing, Analytics, and Export.
    """
    st.setpageconfig(pagetitle="Enterprise Data Pipeline", pageicon="⚡", layout="wide")
    
    st.markdown("""
        <style>
        .metric-card {
            border-left: 5px solid #00CC96;
            background-color: #f0f2f6;
        }
        </style>
    """, unsafeallowhtml=True)
    
    st.title("⚡ Enterprise Batch Data Pipeline")
    st.markdown("**High-Performance Computing & Data Ingestion System**")
    st.markdown("---")
    
    # --- Sidebar: System Controls ---
    with st.sidebar:
        st.header("⚙️ System Configuration")
        if st.button("🧹 Reset Workspace"):
            if os.path.exists(RAWDATADIR):
                shutil.rmtree(RAWDATADIR)
                os.makedirs(RAWDATADIR)
            if os.path.exists(PROCESSEDDATADIR):
                shutil.rmtree(PROCESSEDDATADIR)
                os.makedirs(PROCESSEDDATADIR)
            st.toast("Workspace reset successfully.", icon="🗑️")
    
    # --- MODULE 1: DATA INGESTION ---
    st.subheader("1. Data Ingestion Module")
    
    # Supports CSV and JSON both
    uploadedfiles = st.fileuploader(
        "Upload Source Files (CSV/JSON)",
        type=['csv', 'json'],
        acceptmultiplefiles=True
    )
    
    if uploadedfiles:
        if st.button(f"📥 Stage {len(uploadedfiles)} Files"):
            # The count returned here is ONLY the safe files
            count = saveuploadedfiles(uploadedfiles)
            if count > 0:
                st.success(f"Successfully staged {count} secure files for processing.")
            else:
                st.warning("No files were staged. All files failed security validation.")
    
    st.markdown("---")
    
    # --- MODULE 2: PROCESSING ENGINE ---
    st.subheader("2. Processing Orchestration Engine")
    
    files = []
    if os.path.exists(RAWDATADIR):
        files = [os.path.join(RAWDATADIR, f) for f in os.listdir(RAWDATADIR)
                 if f.endswith(('.csv', '.json'))]
    
    if not files:
        st.warning("⚠️ No data found in staging area. Please upload files to proceed.")
    else:
        st.write(f"Queue Status: **{len(files)} items pending**")
        colprod, colbenchmark = st.columns(2)
        
        with colprod:
            prodrun = st.button("🚀 Execute Production Pipeline (Parallel)", type="primary", usecontainerwidth=True)
        
        with colbenchmark:
            comprun = st.button("⚔️ Run Performance Benchmark", usecontainerwidth=True)
        
        if prodrun:
            st.markdown("### ⚡ Execution Status")
            progressbar = st.progress(0)
            executiontime = processparallel(files, lambda p, t: progressbar.progress(p, text=t))
            st.balloons()
            st.success(f"✅ Pipeline completed in **{executiontime:.2f} seconds** using Parallel Architecture.")
            
            # Auto-create merged table after processing
            st.info("🔄 Creating consolidated master table...")
            create_merged_table()
            st.success("✅ Merged table created successfully!")
        
        if comprun:
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("### ⚡ Parallel (Optimized)")
                bar1 = st.progress(0)
                tpar = processparallel(files, lambda p, t: bar1.progress(p, text=t))
                st.metric("Latency", f"{tpar:.4f} s")
            
            with col2:
                st.markdown("### 🐢 Sequential (Legacy)")
                bar2 = st.progress(0)
                tseq = processsequential(files, lambda p, t: bar2.progress(p, text=t))
            
            # --- SMART COMPARISON LOGIC ---
            diff = tseq - tpar
            if diff > 0:
                # Sequential took MORE time (Slower)
                deltamsg = f"+{diff:.4f} s (Slower)"
                deltacolormode = "inverse"
            else:
                # Sequential took LESS time (Faster)
                deltamsg = f"{diff:.4f} s (Faster)"
                deltacolormode = "inverse"
            
            st.metric("Latency", f"{tseq:.4f} s", delta=deltamsg, deltacolor=deltacolormode)
            st.divider()
            
            # --- INTELLIGENT INSIGHTS MESSAGE ---
            if tpar > 0:
                speedupfactor = tseq / tpar
                if speedupfactor >= 1.0:
                    # Parallel Won
                    st.success(f"🚀 **Performance Win:** Parallel architecture achieved a **{speedupfactor:.1f}x speedup** over legacy sequential processing.")
                else:
                    # Parallel Lost (Overhead)
                    st.warning(
                        f"⚠️ **Performance Insight:** Parallel processing was slower ({speedupfactor:.2f}x). "
                        f"**Reason:** For small datasets, the overhead of creating CPU processes exceeds the processing time. "
                        f"Parallelism shines with 100+ files."
                    )
    
    st.markdown("---")
    
    # --- MODULE 3: ANALYTICS & EXPORT ---
    st.subheader("3. Analytics & Export Module")
    st.markdown("Real-time visualization and artifact retrieval.")
    
    processedfiles = []
    if os.path.exists(PROCESSEDDATADIR):
        processedfiles = [f for f in os.listdir(PROCESSEDDATADIR) if f.endswith('.csv') and f != "MERGED_ALL.csv"]
        processedfiles.sort(key=naturalsortkey)
    
    mergedfileexists = os.path.exists(os.path.join(PROCESSEDDATADIR, "MERGED_ALL.csv"))
    
    # --- DEFAULT VIEW: MERGED TABLE (CONSOLIDATED) ---
    if mergedfileexists:
        st.subheader("📊 Consolidated Master Table (All Files Merged)")
        
        mergedpath = os.path.join(PROCESSEDDATADIR, "MERGED_ALL.csv")
        try:
            dfmerged = pd.read_csv(mergedpath)
            
            # Display merged table stats
            st.markdown(f"**Total Records: {len(dfmerged):,}** | **Total Source Files: {len(processedfiles)}**")
            
            coltable, coldown = st.columns([4, 1])
            
            with coltable:
                st.dataframe(dfmerged, usecontainerwidth=True)
            
            with coldown:
                st.write("")  # Spacer
                st.write("")  # Spacer
                with open(mergedpath, "rb") as f:
                    st.downloadbutton(
                        label="⬇️ Download Merged CSV",
                        data=f.read(),
                        filename=f"MERGED_ALL_{len(dfmerged)}_records.csv",
                        mime="text/csv",
                        type="primary",
                        usecontainerwidth=True
                    )
            
            # Analytics on merged data
            st.markdown("---")
            st.subheader("📈 Master Analytics (Consolidated)")
            
            if 'Region' in dfmerged.columns and 'TotalAmount' in dfmerged.columns:
                col1, col2, col3 = st.columns(3)
                col1.metric("Total Revenue", f"${dfmerged['TotalAmount'].sum():,.2f}")
                col2.metric("Avg Transaction", f"${dfmerged['TotalAmount'].mean():,.2f}")
                col3.metric("Total Transactions", f"{len(dfmerged):,}")
                
                st.markdown("#### Regional Revenue Distribution (All Files Combined)")
                chartdata = dfmerged.groupby('Region')['TotalAmount'].sum()
                st.barchart(chartdata, color="#00CC96")
            else:
                st.info("Analytics require 'Region' and 'Total_Amount' columns in your data.")
        
        except Exception as e:
            st.error(f"Error reading merged table: {e}")
    
    else:
        st.info("📊 Consolidated table not available. Execute the pipeline to generate merged data.")
    
    # --- OPTIONAL: Individual File Explorer ---
    st.markdown("---")
    st.subheader("🔍 Individual File Inspector (Optional)")
    st.markdown("Browse individual processed files if needed.")
    
    if processedfiles:
        colsel, coldown = st.columns([3, 1])
        
        with colsel:
            selectedfile = st.selectbox(
                "Select Individual File to Inspect:",
                processedfiles,
                key="individual_select"
            )
        
        if selectedfile:
            filepath = os.path.join(PROCESSEDDATADIR, selectedfile)
            try:
                dfpreview = pd.read_csv(filepath)
                
                tab1, tab2 = st.tabs(["📄 Data View", "📊 Analytics"])
                
                with tab1:
                    st.dataframe(dfpreview.head(50), usecontainerwidth=True)
                    st.caption(f"Displaying top 50 records from {selectedfile}. Total Records: {len(dfpreview)}")
                
                with tab2:
                    if 'Region' in dfpreview.columns and 'TotalAmount' in dfpreview.columns:
                        m1, m2, m3 = st.columns(3)
                        m1.metric("File Revenue", f"${dfpreview['TotalAmount'].sum():,.2f}")
                        m2.metric("Avg Trans", f"${dfpreview['TotalAmount'].mean():,.2f}")
                        m3.metric("Records", f"{len(dfpreview):,}")
                        
                        st.markdown("#### File Revenue Distribution")
                        chartdata = dfpreview.groupby('Region')['TotalAmount'].sum()
                        st.barchart(chartdata, color="#00CC96")
                
                # Download individual file
                with coldown:
                    st.write("")
                    st.write("")
                    with open(filepath, "rb") as f:
                        st.downloadbutton(
                            label="⬇️ Download File",
                            data=f.read(),
                            filename=selectedfile,
                            mime="text/csv",
                            type="secondary",
                            usecontainerwidth=True
                        )
            
            except Exception as e:
                st.error(f"Error reading file: {e}")
    else:
        st.info("No individual files available. Execute pipeline to generate data.")
    
    # --- BULK EXPORT: Download All Individual Files as ZIP (Optional) ---
    if processedfiles:
        st.markdown("---")
        st.subheader("📦 Bulk Export (Optional)")
        st.markdown(f"**Alternative:** Download all {len(processedfiles)} individual processed files in compressed ZIP format")
        
        colzipinfo, colzipbtn = st.columns([2, 1])
        
        with colzipinfo:
            st.info(f"✅ Ready to export {len(processedfiles)} individual files | No duplicates | Original filenames preserved")
        
        with colzipbtn:
            if st.button(f"📦 Download All as ZIP ({len(processedfiles)} files)", 
                        type="secondary", usecontainerwidth=True,
                        help="Safe bulk ZIP export - No duplicates, all original filenames preserved"):
                
                # Create ZIP buffer
                zipbuffer = io.BytesIO()
                with zipfile.ZipFile(zipbuffer, "w", zipfile.ZIP_DEFLATED) as zipfileobj:
                    for csvfile in processedfiles:
                        filepath = os.path.join(PROCESSEDDATADIR, csvfile)
                        zipfileobj.write(filepath, arcname=csvfile)
                
                zipbuffer.seek(0)
                
                # Download button for ZIP
                st.downloadbutton(
                    label=f"✅ Download ZIP ({len(processedfiles)} files)",
                    data=zipbuffer.getvalue(),
                    filename=f"processed_batch_{len(processedfiles)}_files.zip",
                    mime="application/zip",
                    type="success",
                    usecontainerwidth=True
                )
                st.success(f"✅ ZIP ready with {len(processedfiles)} individual files (no duplicates)")

if __name__ == "__main__":
    renderdashboard()
