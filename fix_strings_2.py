
path = r"c:\work\meemee-screener\app\frontend\src\routes\GridView.tsx"

with open(path, "r", encoding="utf-8") as f:
    content = f.read()

lines = content.split("\n")
new_lines = []
for line in lines:
    if "未設" in line and "E" in line:
        new_lines.append(line.replace('return "未設宁E;', 'return "未設定";'))
    elif 'return "褁E' in line:
        new_lines.append(line.replace('return "褁E";', 'return "複数";'))
    else:
        new_lines.append(line)

content = "\n".join(new_lines)
with open(path, "w", encoding="utf-8") as f:
    f.write(content)
