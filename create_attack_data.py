import os

# Configuration: Output Directory for Security Test Artifacts
FOLDER = "security_test_suite"
os.makedirs(FOLDER, exist_ok=True)

print(f"🚀 Initializing Security Compliance Test Suite generation in '{FOLDER}'...")

# ---------------------------------------------------------
# TEST CASE 1: POSITIVE CONTROL (Valid Data)
# Description: Standard CSV file with legitimate data structure.
# Expected Result: PASS (Staged successfully)
# ---------------------------------------------------------
file_path_1 = os.path.join(FOLDER, "valid_clean_data.csv")
with open(file_path_1, "w") as f:
    f.write("Transaction_ID,Amount,Product\n")
    f.write("TXN-100,500,Laptop\n")
    f.write("TXN-101,200,Mouse\n")


# ---------------------------------------------------------
# TEST CASE 2: NEGATIVE TEST (Extension Violation)
# Description: A simulated executable file (.exe) attempting to bypass upload filters.
# Expected Result: BLOCKED (Policy Violation)
# ---------------------------------------------------------
file_path_2 = os.path.join(FOLDER, "malicious_executable.exe")
with open(file_path_2, "w") as f:
    f.write("This is a simulated binary payload for security testing.")


# ---------------------------------------------------------
# TEST CASE 3: NEGATIVE TEST (Payload Injection / XSS)
# Description: Valid extension (.csv) containing malicious script tags (Heuristic Check).
# Expected Result: BLOCKED (Threat Signature Detected)
# ---------------------------------------------------------
file_path_3 = os.path.join(FOLDER, "payload_injection_attack.csv")
with open(file_path_3, "w") as f:
    f.write("Transaction_ID,Amount,Product\n")
    # Injecting a known threat signature (<script>) to trigger the security firewall
    f.write("TXN-666,0,<script>alert('System Compromised')</script>\n") 
    f.write("TXN-667,100,Keyboard\n")

print("-" * 50)
print(f"✅ Security Test Suite generated successfully.")
print(f"📂 Location: ./{FOLDER}/")
print(f"👉 Action Required: Upload these 3 artifacts to the Dashboard to validate the 'Threat Detection Module'.")
print("-" * 50)