
def try_convert(path):
    encodings = ["utf-8", "cp932", "utf-8-sig", "euc-jp"]
    content = None
    detected = None
    
    with open(path, "rb") as f:
        raw = f.read()
    
    for enc in encodings:
        try:
            content = raw.decode(enc)
            # If successful, check if it looks reasonable? 
            # (utf-8 might decode cp932 as garbage but validly)
            # But usually cp932 bytes are invalid utf-8.
            # If it decodes, we assume it's that.
            detected = enc
            break
        except Exception:
            continue
            
    if detected:
        print(f"Decoded as {detected}")
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        print("Restored as clean UTF-8")
    else:
        print("Could not detect encoding")

if __name__ == "__main__":
    try_convert(r"c:\work\meemee-screener\app\frontend\src\routes\GridView.tsx")

