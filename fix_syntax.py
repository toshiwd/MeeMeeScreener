
path = r"c:\work\meemee-screener\app\frontend\src\routes\GridView.tsx"

with open(path, "r", encoding="utf-8") as f:
    lines = f.readlines()

new_lines = []
for line in lines:
    stripped = line.strip()
    
    # Fix the syntax error
    if "options: [options:" in stripped:
        indent = line.split("options")[0]
        new_lines.append(indent + 'options: [{ key: "boxState", label: "ボックス状態" }]\n')
        continue
        
    # Fix titles
    if "title:" in stripped and ("E" in stripped or "\ufffd" in stripped):
        indent = line.split("title")[0] if "title" in line else ""
        if "買" in stripped:
            new_lines.append(indent + 'title: "買い候補",\n')
        elif "売り" in stripped: # The garbage might effectively obscure "売り" but "売り" is often preserved?
             # In view_file: title: "売り候裁E, 
             # So "売り" is there.
             new_lines.append(indent + 'title: "売り候補",\n')
        elif "チ" in stripped or "ニカル" in stripped:
             # title: "チEニカル",
             new_lines.append(indent + 'title: "テクニカル",\n')
        elif "ボックス" in stripped:
             new_lines.append(indent + 'title: "ボックス",\n')
        elif "スコア" in stripped:
             new_lines.append(indent + 'title: "スコア",\n')
        elif "パフォーマンス" in stripped:
             new_lines.append(indent + 'title: "パフォーマンス",\n')
        elif "基本" in stripped:
             new_lines.append(indent + 'title: "基本",\n')
        else:
            # Fallback
            new_lines.append(line)
    else:
        new_lines.append(line)

with open(path, "w", encoding="utf-8") as f:
    f.writelines(new_lines)
