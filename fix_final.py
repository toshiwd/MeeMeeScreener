
path = r"c:\work\meemee-screener\app\frontend\src\routes\GridView.tsx"

with open(path, "r", encoding="utf-8") as f:
    lines = f.readlines()

new_lines = []
for line in lines:
    stripped = line.strip()
    
    # 1. Rights data (Line 175 approx)
    if "return formatted ?" in stripped and ("権利" in stripped or "篁" in stripped):
        new_lines.append(line.replace(stripped, 'return formatted ? `権利データ範囲: ～${formatted}` : null;'))
        
    # 2. Watchlist ternary (Line 1152 approx)
    elif "const message =" in stripped and "error.message" in stripped and "ウォチ" in stripped:
        # Check which one
        if "削除" in stripped:
            new_lines.append(line.split("const")[0] + 'const message = error instanceof Error ? error.message : "ウォッチリスト削除に失敗しました。";\n')
        elif "追加" in stripped:
             new_lines.append(line.split("const")[0] + 'const message = error instanceof Error ? error.message : "ウォッチリスト追加に失敗しました。";\n')
        else:
             new_lines.append(line) # Should not happen based on findings
             
    # 3. Consult placeholder (Line 2235 approx)
    elif "consult-placeholder" in stripped and ("建玉" in stripped or "相諁" in stripped):
        # <div className="consult-placeholder">建玉相諁 ...</div>
        # Replace content inside div
        indent = line.split("<div")[0]
        new_lines.append(indent + '<div className="consult-placeholder">建玉情報がありません</div>\n')
        
    # 4. Close button / Copy
    elif "コピ" in stripped and ("" in stripped or "E" in stripped or "E" in stripped):
        # <button ...>コピE</button>
        if ">コピ" in line:
            new_lines.append(line.replace(">コピE", ">コピー").replace(">コピE", ">コピー").replace("コピE", "コピー").replace("コピE", "コピー"))
        else:
            new_lines.append(line.replace("コピE", "コピー").replace("コピE", "コピー"))
            
    elif "閉じめ" in stripped or ("閉じ" in stripped and "E" in stripped):
        new_lines.append(line.replace("閉じめE", "閉じる").replace("閉じE", "閉じる"))

    elif "相諁" in stripped:
         # Generic fix for this mojibake
         new_lines.append(line.replace("相諁", "相談").replace("相諁E", "相談").replace("相諁E", "相談"))
         
    # 5. Fullwidth again just in case (the previous fix worked but let's be safe)
    # 6. Titles again
    elif "title:" in stripped and ("E" in stripped or "\ufffd" in stripped):
         # Same logic as before
        indent = line.split("title")[0] if "title" in line else ""
        if "買" in stripped:
            new_lines.append(indent + 'title: "買い候補",\n')
        elif "売り" in stripped: 
             new_lines.append(indent + 'title: "売り候補",\n')
        elif "チ" in stripped or "ニカル" in stripped:
             new_lines.append(indent + 'title: "テクニカル",\n')
        else:
            new_lines.append(line)

    elif "E" in stripped or "、E" in stripped:
         # Catch-all for remaining garbage
         # Replace 、E with 。
         clean = line.replace("、E", "。").replace("E", "")
         if clean != line:
             new_lines.append(clean)
         else:
             new_lines.append(line)
    else:
        new_lines.append(line)

with open(path, "w", encoding="utf-8") as f:
    f.writelines(new_lines)
