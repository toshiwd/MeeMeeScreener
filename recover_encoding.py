
def recover(path):
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    
    # Attempt to recover
    try:
        # Encode back to bytes using cp932 (reversing the incorrect decode)
        raw_bytes = content.encode("cp932")
        # Now decode as utf-8, ignoring/replacing errors
        restored = raw_bytes.decode("utf-8", errors="replace")
        
        print("Successfully recovered (with replacements)")
        with open(path, "w", encoding="utf-8") as f:
            f.write(restored)
    except Exception as e:
        print(f"Failed to recover: {e}")

if __name__ == "__main__":
    recover(r"c:\work\meemee-screener\app\frontend\src\routes\GridView.tsx")
