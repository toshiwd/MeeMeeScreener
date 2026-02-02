
path = r"c:\work\meemee-screener\app\frontend\src\routes\GridView.tsx"

with open(path, "r", encoding="utf-8") as f:
    content = f.read()

lines = content.split("\n")
new_lines = []
for line in lines:
    stripped = line.strip()
    if "ウォチ" in stripped and ("E" in stripped or "\ufffd" in stripped):
        # Watchlist messages
        if "削除に失敗" in stripped:
             new_lines.append(line.split("showToast")[0] + 'showToast("ウォッチリスト削除に失敗しました。");')
        elif "削除しました" in stripped:
             new_lines.append(line.split("showToast")[0] + 'showToast("ウォッチリストから削除しました。");')
        elif "追加に失敗" in stripped:
             new_lines.append(line.split("showToast")[0] + 'showToast("ウォッチリスト追加に失敗しました。");')
        elif "追加しました" in stripped:
             new_lines.append(line.split("showToast")[0] + 'showToast("ウォッチリストに追加しました。");')
        else:
             # Fallback
             print(f"Unknown watchlist message: {stripped}")
             new_lines.append(line.split("showToast")[0] + 'showToast("ウォッチリスト操作完了。");')
    elif "元に戻しました" in stripped and ("E" in stripped or "\ufffd" in stripped):
          new_lines.append(line.split("showToast")[0] + 'showToast("元に戻しました。");')
    elif "削除しました" in stripped and "showToast" in stripped and ("E" in stripped or "\ufffd" in stripped):
          # Generic delete?
          new_lines.append(line.split("showToast")[0] + 'showToast("削除しました。");')
          
    else:
        new_lines.append(line)

content = "\n".join(new_lines)
with open(path, "w", encoding="utf-8") as f:
    f.write(content)
