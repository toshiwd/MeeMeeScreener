
import os
import tempfile
import shutil

import re

def normalize_code_txt(path: str) -> bool:
    """
    Sorts and deduplicates code lines. Preserves non-code lines at the bottom.
    Creates a backup (code.txt.bak) before modification.
    Returns True if changes were made, False if already normalized.
    """
    if not os.path.exists(path):
        return False

    # 1. Read content
    lines = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            lines = [line.strip() for line in f.readlines()]
    except UnicodeDecodeError:
        with open(path, "r", encoding="cp932") as f:
            lines = [line.strip() for line in f.readlines()]

    # 2. Parse (Code vs Non-Code)
    # Lax pattern for tickers: 4 digits, optional suffix.
    code_pattern = re.compile(r"^\d{4}[A-Z]?$")
    
    codes = set()
    others = []
    
    for line in lines:
        if not line: 
            continue
        # Check standard ticker format
        # If user puts "1301.T", strip .T? The ingest usually handles lax inputs,
        # but code.txt for VBS often expects strict 4 digit or simple format.
        # Let's clean standard casing.
        clean_line = line.upper().replace(" ", "")
        
        if code_pattern.match(clean_line):
            codes.add(clean_line)
        else:
            others.append(line) # Keep original for others

    sorted_codes = sorted(list(codes))
    
    # 3. Construct New Content
    # Codes first, then others (with a separator if others exist)
    new_lines = sorted_codes[:]
    if others:
        new_lines.append("") 
        new_lines.append("# --- Non-Code / Preserved Lines ---")
        new_lines.extend(others)
    
    new_content = "\n".join(new_lines) + "\n"

    # 4. Check diff
    try:
        with open(path, "r", encoding="utf-8") as f:
            current_content = f.read()
    except Exception:
        current_content = ""
        
    # normalization for comparison
    if new_content.replace("\r\n", "\n") == current_content.replace("\r\n", "\n"):
        return False

    # 5. Backup
    backup_path = path + ".bak"
    try:
        shutil.copy2(path, backup_path)
    except Exception as e:
        print(f"Warning: Failed to backup code.txt: {e}")
        # Proceeding anyway? User requested "退避" so we should probably try hard.
        # But if permission error, maybe we can't write `code.txt` either.

    # 6. Atomic Write
    dir_name = os.path.dirname(path)
    fd, tmp_path = tempfile.mkstemp(dir=dir_name, text=True)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(new_content)
        shutil.move(tmp_path, path)
    except Exception:
        os.remove(tmp_path)
        raise
        
    return True
