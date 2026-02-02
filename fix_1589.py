
path = r"c:\work\meemee-screener\app\frontend\src\routes\GridView.tsx"

with open(path, "r", encoding="utf-8") as f:
    lines = f.readlines()

new_lines = []
for line in lines:
    stripped = line.strip()
    if 'tooltip="表示設' in stripped:
        new_lines.append(line.split('tooltip=')[0] + 'tooltip="表示設定"\n')
    else:
        new_lines.append(line)

with open(path, "w", encoding="utf-8") as f:
    f.writelines(new_lines)
