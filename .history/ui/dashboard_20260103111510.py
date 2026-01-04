import re  # <--- Ye import zaroori hai

def sanitize_filename(filename):
    """
    Security Function: Filename me se dangerous characters hatata hai.
    Sirf A-Z, 0-9, _, ., - ko allow karega.
    """
    # 1. Sirf safe characters rakho
    clean_name = re.sub(r'[^a-zA-Z0-9_.-]', '_', filename)
    # 2. Double dots (..) hatao taaki koi folder se bahar na ja sake (Path Traversal)
    clean_name = clean_name.replace('..', '')
    # 3. Agar naam khali ho gaya, to default naam do
    if not clean_name:
        clean_name = "uploaded_file.csv"
    return clean_name

def save_uploaded_files(uploaded_files):
    """
    Securely save uploaded user files.
    """
    if os.path.exists(RAW_DATA_DIR):
        shutil.rmtree(RAW_DATA_DIR)
    os.makedirs(RAW_DATA_DIR)
    
    saved_count = 0
    my_bar = st.progress(0, text="Staging files...")
    
    total_files = len(uploaded_files)
    for i, uploaded_file in enumerate(uploaded_files):
        
        # --- SECURITY CHECK 1: FILE SIZE ---
        # 200MB se badi file reject karo (Zip Bomb attack rokne ke liye)
        if uploaded_file.size > 200 * 1024 * 1024:
            st.error(f"⚠️ Security Alert: {uploaded_file.name} is too large (>200MB). Skipped.")
            continue
            
        # --- SECURITY CHECK 2: FILENAME SANITIZATION ---
        safe_name = sanitize_filename(uploaded_file.name)
        
        # --- SECURITY CHECK 3: CONTENT TYPE (MIME) ---
        # Sirf naam nahi, file ka type bhi CSV hona chahiye
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