
path = r"c:\work\meemee-screener\app\frontend\src\routes\GridView.tsx"

with open(path, "r", encoding="utf-8") as f:
    lines = f.readlines()

new_lines = []
for line in lines:
    stripped = line.strip()
    if '3x3に戻す/span>' in stripped or '3x3に戻/span>' in stripped:
        new_lines.append(line.replace('/span>', '</span>'))
    else:
        new_lines.append(line)

with open(path, "w", encoding="utf-8") as f:
    f.writelines(new_lines)
