
path = r"c:\work\meemee-screener\app\frontend\src\routes\GridView.tsx"

with open(path, "r", encoding="utf-8") as f:
    lines = f.readlines()

new_lines = []
for line in lines:
    stripped = line.strip()
    
    # 1. tooltips
    if 'tooltip="設宁' in stripped:
        indent = line.split("tooltip")[0]
        new_lines.append(indent + 'tooltip="設定"\n')
        
    elif 'ariaLabel="設宁' in stripped:
        indent = line.split("ariaLabel")[0]
        new_lines.append(indent + 'ariaLabel="設定メニューを開く"\n')
        
    # 2. Appearance Title
    elif "外観設" in stripped:
        if "/div" in stripped:
            indent = line.split("<div")[0]
            new_lines.append(indent + '<div className="popover-title">外観設定</div>\n')
        else:
            new_lines.append(line.replace("設宁", "設定"))
            
    # 3. Light mode
    elif "ライチ" in stripped:
        # <span>ライチ/span> -> <span>ライト</span>
        if "/span" in stripped:
             new_lines.append(line.replace("ライチ/span>", "ライト</span>").replace("ライチ", "ライト"))
        else:
             new_lines.append(line.replace("ライチ", "ライト"))

    else:
        new_lines.append(line)

with open(path, "w", encoding="utf-8") as f:
    f.writelines(new_lines)
