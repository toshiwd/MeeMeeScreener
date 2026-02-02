
path = r"c:\work\meemee-screener\app\frontend\src\routes\GridView.tsx"

with open(path, "r", encoding="utf-8") as f:
    lines = f.readlines()

new_lines = []
for line in lines:
    stripped = line.strip()
    
    # 1. Save button
    if '保孁}' in stripped:
        new_lines.append(line.replace('保孁}', '保存"}'))
        
    # 2. Save Path
    elif "保存:" in stripped:
        new_lines.append(line.replace("保存:", "保存先:"))
        
    # 3. Not set (inside hint)
    elif "未設宁" in stripped:
        # dataDir || (dataDirLoading ? "..." : "未設宁)
        # Missing quote at end
        new_lines.append(line.replace("未設宁", '未設定"'))

    else:
        new_lines.append(line)

with open(path, "w", encoding="utf-8") as f:
    f.writelines(new_lines)
