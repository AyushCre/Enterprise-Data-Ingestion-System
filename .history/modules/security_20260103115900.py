# FILE: modules/security.py
import re
import logging

class SecurityInspector:
    """
    Implements security protocols to sanitize incoming data streams.
    Detects potential malware payloads, SQL injections, and file extension spoofing.
    """
    
    # Dangerous patterns often found in malicious files masquerading as CSV/JSON
    BLACKLIST_PATTERNS = [
        rb"<script>",           # XSS Attack Vector
        rb"javascript:",        # JS Injection
        rb"os\.system",         # Python Command Injection
        rb"subprocess\.call",   # Shell Injection
        rb"DROP TABLE",         # SQL Injection
        rb"cmd\.exe",           # Windows Command Execution
        rb"/bin/sh"             # Linux Shell Execution
    ]

    @staticmethod
    def inspect_file(uploaded_file) -> tuple[bool, str]:
        """
        Scans a file object for security threats before processing.
        
        Args:
            uploaded_file: The stream object from Streamlit uploader.
            
        Returns:
            tuple: (is_safe: bool, reason: str)
        """
        filename = uploaded_file.name.lower()
        
        # 1. Double Extension Check (e.g., data.csv.exe)
        if re.search(r'\.[a-z]{3,4}\.(exe|bat|sh|bin|cmd)$', filename):
            return False, "Double extension spoofing detected."

        # 2. File Size Limit (Prevent DoS Attacks - e.g., limit to 200MB)
        # Note: uploaded_file.size is in bytes
        if uploaded_file.size > 200 * 1024 * 1024:
            return False, "File exceeds maximum upload size policy (200MB)."

        # 3. Content Inspection (Heuristic Scanning)
        # Read first 1KB to check for binary headers or malicious scripts
        # We don't read full file here to save memory (Optimization)
        try:
            start_content = uploaded_file.getvalue()[:1024]
            
            # Check for executable magic numbers (MZ for .exe)
            if start_content.startswith(b'MZ'): 
                return False, "Malicious binary header detected (Executable masquerading as text)."
                
            # Scan for blacklist patterns
            for pattern in SecurityInspector.BLACKLIST_PATTERNS:
                if pattern in start_content:
                    return False, f"Malicious payload signature detected: {pattern.decode('utf-8', errors='ignore')}"
                    
        except Exception as e:
            logging.error(f"Security scan error: {e}")
            return False, "File integrity check failed."

        return True, "File is clean."