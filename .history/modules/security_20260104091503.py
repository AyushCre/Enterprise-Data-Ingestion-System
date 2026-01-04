# FILE: modules/security.py

import re
import logging
import hashlib
import json
import os
from datetime import datetime

# --- Enterprise Configuration ---
# Audit Log File: Ye file saare security events record karegi (JSON format me)
AUDIT_LOG_FILE = "security_audit.jsonl" 

class SecurityAuditor:
    """
    Handles secure logging of events for compliance and post-mortem analysis.
    Generates JSON-structured logs suitable for SIEM tools (Splunk, ELK).
    """
    @staticmethod
    def log_event(filename, status, reason, file_hash="N/A"):
        event = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "filename": filename,
            "status": status,  # "BLOCKED" or "CLEAN"
            "reason": reason,
            "file_hash_sha256": file_hash,
            "scan_engine": "v2.1-Enterprise"
        }
        
        # Log to Console (for Devs)
        if status == "BLOCKED":
            logging.warning(f"SECURITY ALERT: {json.dumps(event)}")
        else:
            logging.info(f"Scan Passed: {json.dumps(event)}")
            
        # Log to Audit File (For Compliance/Security Team)
        try:
            with open(AUDIT_LOG_FILE, "a") as f:
                f.write(json.dumps(event) + "\n")
        except Exception as e:
            logging.error(f"Failed to write audit log: {e}")

class SecurityInspector:
    """
    Enterprise-Grade Security Inspector.
    Implements Deep Content Scanning, Hashing, and Pattern Matching.
    """

    # Extended Threat Signatures (SQLi, XSS, RCE, Shell)
    THREAT_SIGNATURES = [
        rb"<script", b"javascript:", b"vbscript:",  # XSS
        rb"onload=", rb"onerror=",                   # Event Handlers
        rb"DROP TABLE", rb"UNION SELECT",            # SQL Injection
        rb"os\.system", rb"subprocess\.Popen",       # RCE (Python)
        rb"cmd\.exe", rb"/bin/sh", rb"/bin/bash",    # Shell Execution
        rb"powershell", rb"bitsadmin",               # Windows Admin Tools
    ]

    @staticmethod
    def calculate_hash(file_stream):
        """Generates SHA-256 Hash of the file for tracking."""
        sha256_hash = hashlib.sha256()
        file_stream.seek(0)
        # Read in chunks to avoid memory overflow on large files
        for byte_block in iter(lambda: file_stream.read(4096), b""):
            sha256_hash.update(byte_block)
        file_stream.seek(0) # Reset pointer
        return sha256_hash.hexdigest()

    @staticmethod
    def inspect_file(uploaded_file) -> tuple[bool, str]:
        """
        Main Security Entry Point.
        1. Metadata Check
        2. Hash Calculation
        3. Magic Byte Verification
        4. Deep Content Scanning (Chunk-based)
        """
        filename = uploaded_file.name.lower()
        
        # --- LAYER 1: Policy Checks ---
        # Double Extension Spoofing (e.g., report.csv.exe)
        if re.search(r"\.[a-z0-9]{2,4}\.(exe|bat|sh|bin|dll|ps1)$", filename):
            SecurityAuditor.log_event(filename, "BLOCKED", "Double Extension Spoofing")
            return False, "Security Policy Violation: Double Extension Detected."

        # Size Limit (200MB)
        if uploaded_file.size > 200 * 1024 * 1024:
            SecurityAuditor.log_event(filename, "BLOCKED", "File Size Limit Exceeded")
            return False, "File exceeds 200MB limit."

        # --- LAYER 2: Integrity & Fingerprinting ---
        try:
            file_hash = SecurityInspector.calculate_hash(uploaded_file)
        except Exception as e:
            return False, f"Hashing Failed: {str(e)}"

        # --- LAYER 3: Magic Byte (Header) Verification ---
        # Pehle 1024 bytes padho header check ke liye
        header = uploaded_file.read(1024)
        uploaded_file.seek(0) # Important: Pointer reset karo

        # MZ Header Check (Windows Executables)
        if header.startswith(b"MZ"):
            SecurityAuditor.log_event(filename, "BLOCKED", "Malicious Header (MZ)", file_hash)
            return False, "Critical: Executable header detected in non-executable file."
        
        # ELF Header Check (Linux Executables)
        if header.startswith(b"\x7fELF"):
            SecurityAuditor.log_event(filename, "BLOCKED", "Malicious Header (ELF)", file_hash)
            return False, "Critical: Linux binary header detected."

        # --- LAYER 4: Deep Content Scanning (Stream Process) ---
        # Puri file ko memory me load karne ki bajaye, chunks me scan karenge
        # Taki 'Zip Bomb' ya Memory Overflow na ho.
        
        chunk_size = 1024 * 1024 * 2  # 2MB chunks
        file_size = uploaded_file.size
        bytes_read = 0
        
        found_threat = False
        threat_detail = ""

        uploaded_file.seek(0)
        while bytes_read < file_size:
            chunk = uploaded_file.read(chunk_size)
            if not chunk:
                break
            
            # Check signatures in this chunk
            for pattern in SecurityInspector.THREAT_SIGNATURES:
                if pattern in chunk:
                    found_threat = True
                    try:
                        threat_detail = pattern.decode('utf-8')
                    except:
                        threat_detail = "Binary Signature"
                    break
            
            if found_threat:
                break
            
            bytes_read += len(chunk)

        uploaded_file.seek(0) # Reset pointer for the next process (Processing Engine)

        if found_threat:
            SecurityAuditor.log_event(filename, "BLOCKED", f"Threat Signature Detected: {threat_detail}", file_hash)
            return False, f"Malicious Payload Detected: {threat_detail}"

        # --- PASSED ALL CHECKS ---
        SecurityAuditor.log_event(filename, "CLEAN", "Passed all inspections", file_hash)
        return True, "File is clean."