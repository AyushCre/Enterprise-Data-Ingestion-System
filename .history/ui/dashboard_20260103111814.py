import re  # <--- Essential Import for Regex

def sanitize_filename(filename):
    """
    Security Utility: Sanitizes the filename by removing dangerous characters.
    It strictly allows only A-Z, 0-9, _, ., and - to prevent shell injection risks.
    """
    # 1. Whitelist approach: Retain only safe characters
    clean_name = re.sub(r'[^a-zA-Z0-9_.-]', '_', filename)
    
    # 2. Prevent Path Traversal: Remove double dots (..) to stop directory escapes
    clean_name = clean_name.replace('..', '')
    
    # 3. Fallback: Assign a default name if the result is empty
    if not clean_name:
        clean_name = "uploaded_file.csv"
    return clean_name

def save_uploaded_files(uploaded_files):
    """
    Securely saves uploaded user files to the staging directory.
    """
    if os.path.exists(RAW_DATA_DIR):
        shutil.rmtree(RAW_DATA_DIR)
    os.makedirs(RAW_DATA_DIR)
    
    saved_count = 0
    my_bar = st.progress(0, text="Processing files...")
    
    total_files = len(uploaded_files)
    for i, uploaded_file in enumerate(uploaded_files):
        
        # --- SECURITY CHECK 1: FILE SIZE ENFORCEMENT ---
        # Reject files larger than 200MB to mitigate DoS or Zip Bomb attacks
        if uploaded_file.size > 200 * 1024 * 1024:
            st.error(f"⚠️ Security Alert: {uploaded_file.name} is too large (>200MB). Skipped.")
            continue
            
        # --- SECURITY CHECK 2: FILENAME SANITIZATION ---
        safe_name = sanitize_filename(uploaded_file.name)
        
        # --- SECURITY CHECK 3: CONTENT TYPE (MIME) VALIDATION ---
        # Ensure the file is genuinely a CSV and not just a renamed file
        if uploaded_file.type != "text/csv" and not uploaded_file.name.endswith('.csv'):
             st.warning(f"⚠️ Security Alert: {uploaded_file.name} does not appear to be a valid CSV. Skipped.")
             continue

        file_path = os.path.join(RAW_DATA_DIR, safe_name)
        with open(file_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        saved_count += 1
        my_bar.progress((i + 1) / total_files)
        
    time.sleep(0.5)
    my_bar.empty()
    return saved_count