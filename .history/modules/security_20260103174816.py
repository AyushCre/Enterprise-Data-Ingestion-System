# FILE: modules/security.py

import re
import logging
import math


class SecurityInspector:
    """
    Implements security protocols to sanitize incoming data streams.
    Detects potential malware payloads, SQL injections, and file extension spoofing.
    """

    # Dangerous patterns often found in malicious files masquerading as CSV/JSON
    BLACKLIST_PATTERNS = [
        rb"<script",          # XSS Attack Vector
        rb"javascript:",      # JS Injection
        rb"os\.system",       # Python Command Injection
        rb"subprocess\.call", # Shell Injection
        rb"DROP TABLE",       # SQL Injection
        rb"cmd\.exe",         # Windows Command Execution
        rb"/bin/sh",          # Linux Shell Execution
    ]

    @staticmethod
    def _heuristic_suspicious_score(start_content: bytes) -> float:
        """
        Lightweight heuristic analyzer to flag files that LOOK normal
        but contain suspicious patterns at high density.

        Returns score between 0.0 (benign) and 1.0 (highly suspicious).
        """
        try:
            text = start_content.decode("utf-8", errors="ignore")
        except Exception:
            # Binary / undecodable data is more suspicious in a CSV/JSON context
            return 0.9

        if not text.strip():
            return 0.0

        total_len = len(text)

        # 1) High ratio of non-alphanumeric chars (often scripts / obfuscation)
        special_chars = sum(
            1
            for c in text
            if not c.isalnum() and c not in " ,._-:/\\\n\r\t"
        )
        special_ratio = special_chars / total_len

        # 2) Base64-like long chunks (often used to hide payloads)
        base64_like = re.findall(r"[A-Za-z0-9+/]{40,}={0,2}", text)
        base64_score = min(len("".join(base64_like)) / (total_len + 1), 1.0)

        # 3) SQL keyword density
        sql_keywords = [
            "SELECT", "INSERT", "UPDATE", "DELETE",
            "DROP", "CREATE", "ALTER", "UNION"
        ]
        upper_text = text.upper()
        sql_hits = sum(upper_text.count(k) for k in sql_keywords)
        sql_score = min(sql_hits / 10.0, 1.0)  # normalize

        # 4) Script keyword density (JS / HTML)
        script_keywords = [
            "<SCRIPT", "ONERROR", "ONLOAD",
            "EVAL(", "FUNCTION(", "XMLHTTPREQUEST"
        ]
        script_hits = sum(upper_text.count(k) for k in script_keywords)
        script_score = min(script_hits / 5.0, 1.0)

        # Weighted combination
        score = (
            0.35 * special_ratio +
            0.25 * base64_score +
            0.20 * sql_score +
            0.20 * script_score
        )

        # Cap between 0 and 1
        return max(0.0, min(score, 1.0))

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
        if re.search(r"\.[a-z0-9]{2,4}\.(exe|bat|sh|bin|cmd)$", filename):
            return False, "Double extension spoofing detected."

        # 2. Size Policy Check (example: 200 MB limit)
        # Note: uploaded_file.size is in bytes
        max_size_bytes = 200 * 1024 * 1024  # 200 MB
        if getattr(uploaded_file, "size", 0) > max_size_bytes:
            return False, "File exceeds maximum upload size policy (200MB)."

        try:
            # Read only the first 1024 bytes for quick inspection (optimization)
            startcontent = uploaded_file.getvalue()[:1024]

            # 3. Magic Byte Check for Executables (MZ header)
            if startcontent.startswith(b"MZ"):
                return False, "Malicious binary header detected (Executable masquerading as text)."

            # 4. Signature-based blacklist pattern scan
            for pattern in SecurityInspector.BLACKLIST_PATTERNS:
                if pattern in startcontent:
                    return False, f"Malicious payload signature detected: {pattern.decode('utf-8', errors='ignore')}"

            # 5. Heuristic anomaly detection (advanced layer)
            heuristic_score = SecurityInspector._heuristic_suspicious_score(startcontent)
            if heuristic_score >= 0.75:
                return False, f"Heuristic anomaly detected (score={heuristic_score:.2f})"

        except Exception as e:
            logging.error(f"Security scan error: {e}")
            return False, "File integrity check failed."

        # If all checks passed
        return True, "File is clean."
