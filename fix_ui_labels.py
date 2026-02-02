
path = r"c:\work\meemee-screener\app\frontend\src\routes\GridView.tsx"

with open(path, "r", encoding="utf-8") as f:
    lines = f.readlines()

new_lines = []
for line in lines:
    stripped = line.strip()
    
    # 1. Consult span
    if "<span>相談" in stripped and "/span>" in stripped:
        new_lines.append(line.replace("<span>相談E/span>", "<span>相談</span>").replace("<span>相談/span>", "<span>相談</span>"))
        
    # 2. MA Toggle
    elif "MA一括表示" in stripped:
        indent = line.split("<span")[0]
        new_lines.append(indent + '<span className="popover-item-label">MA一括表示切替</span>\n')
        
    # 3. Sort tooltip
    elif 'tooltip="並び替' in stripped:
        indent = line.split("tooltip")[0]
        new_lines.append(indent + 'tooltip="並び替え"\n')
        
    # 4. Sort aria
    elif 'ariaLabel="並び替えメニューを開' in stripped:
        indent = line.split("ariaLabel")[0]
        new_lines.append(indent + 'ariaLabel="並び替えメニューを開く"\n')
        
    # 5. Indicator settings
    elif "インジケーター設" in stripped:
        # Check context. Usually <span ...>...</span>
        if "/span" in stripped:
            indent = line.split("<span")[0]
            new_lines.append(indent + '<span className="popover-item-header-label">インジケーター設定</span>\n')
        else:
             new_lines.append(line.replace("設宁", "設定").replace("設E", "設定"))

    # 6. Any other ぁ at end of quote?
    elif stripped.endswith('ぁ') or stripped.endswith('ぁ"'):
         # Suspicious
         if "並び順を" in stripped:
             new_lines.append(line.replace('ぁ', 'える"').replace('ぁ"', 'える"'))
         else:
             new_lines.append(line)

    else:
        new_lines.append(line)

with open(path, "w", encoding="utf-8") as f:
    f.writelines(new_lines)
