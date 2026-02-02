
path = r"c:\work\meemee-screener\app\frontend\src\routes\GridView.tsx"

with open(path, "r", encoding="utf-8") as f:
    lines = f.readlines()

new_lines = []
for line in lines:
    stripped = line.strip()
    
    # 1. Undo "元に戻す"
    if "允戻ぁ" in stripped:
        new_lines.append(line.replace("允戻ぁ", "元に戻す"))
    elif "戻ぁ" in stripped:
        new_lines.append(line.replace("戻ぁ", "戻す"))
        
    # 2. Open "開く"
    elif "開ぁ" in stripped:
        # Check if missing quote
        if 'ariaLabel="' in stripped and stripped.endswith('ぁ'):
             new_lines.append(line.replace('ぁ', 'く"'))
        else:
             new_lines.append(line.replace("開ぁ", "開く").replace("開ぁ", "開く"))
    
    # 3. Missing "無い"
    elif "無ぁ" in stripped:
        new_lines.append(line.replace("無ぁ", "無い"))
        
    # 4. Stock Code "銘柄"
    elif "銘柁" in stripped:
        new_lines.append(line.replace("銘柁", "銘柄"))
        
    # 5. "3x3に戻す"
    elif "3x3に戻" in stripped:
        new_lines.append(line.replace("3x3に戻ぁ", "3x3に戻す"))

    # 6. Check for "を推" (推測/抽出)
    elif "を推" in stripped and "銘柄" in stripped:
        # "ファイル名から銘柄コードを推..." -> 推測? 抽出?
        if "推" in stripped: # Context check
             pass # "推" is correct, but check if trailing garbage
             if "測" not in stripped and "定" not in stripped:
                 # Likely broken
                 new_lines.append(line.replace("を推", "を推測"))
        else:
            new_lines.append(line)
            
    else:
        new_lines.append(line)

with open(path, "w", encoding="utf-8") as f:
    f.writelines(new_lines)
