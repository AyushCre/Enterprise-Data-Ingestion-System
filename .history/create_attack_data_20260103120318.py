import os

# Folder create karo
FOLDER = "security_test_files"
os.makedirs(FOLDER, exist_ok=True)

print(f"😈 Creating Attack Simulation Files in '{FOLDER}'...")

# 1. ✅ SAFE FILE (Ye pass honi chahiye)
with open(os.path.join(FOLDER, "clean_data.csv"), "w") as f:
    f.write("Transaction_ID,Amount,Product\n")
    f.write("TXN-100,500,Laptop\n")
    f.write("TXN-101,200,Mouse\n")

# 2. ❌ BAD EXTENSION (File name .exe hai - Ye Block honi chahiye)
# Hum bas ek text file bana rahe hain par naam .exe rakh rahe hain test ke liye
with open(os.path.join(FOLDER, "malware_script.exe"), "w") as f:
    f.write("This is a fake virus for testing.")

# 3. ❌ MALICIOUS CONTENT (Extension sahi hai, par andar danger hai)
# Isme hum '<script>' tag daalenge jo hamare blacklist me hai
with open(os.path.join(FOLDER, "sql_injection_attack.csv"), "w") as f:
    f.write("Transaction_ID,Amount,Product\n")
    f.write("TXN-666,0,<script>alert('Hacked')</script>\n") # <--- Ye pakda jayega
    f.write("TXN-667,100,Keyboard\n")

print("✅ Files Ready! Ab Dashboard me jakar in 3 files ko upload karo.")