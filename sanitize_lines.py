
import re

path = r"c:\work\meemee-screener\app\frontend\src\routes\GridView.tsx"

with open(path, "r", encoding="utf-8") as f:
    lines = f.readlines()

new_lines = []
for line in lines:
    stripped = line.strip()
    if "showToast" in stripped and ("E" in stripped or "アチ" in stripped or "、" in stripped or "\ufffd" in stripped):
        # Identify which message it is by context
        replacement = line # default
        
        if "トレードCSV" in stripped and "失敗" in stripped:
            replacement = line.split("showToast")[0] + 'showToast(`トレードCSVのアップロードに失敗しました。(${detail})`);\n'
        elif "トレードCSV" in stripped:
             replacement = line.split("showToast")[0] + 'showToast("トレードCSVをアップロードしました。");\n'
             
        elif "強制同期" in stripped and "エラー" in stripped:
             replacement = line.split("showToast")[0] + 'showToast(`強制同期でエラーが発生しました。(${errors[0]})`);\n'
        elif "強制同期" in stripped and "失敗" in stripped:
             replacement = line.split("showToast")[0] + 'showToast(`強制同期に失敗しました。(${detail})`);\n'
        elif "強制同期" in stripped:
             replacement = line.split("showToast")[0] + 'showToast("強制同期を実行しました。");\n'
             
        elif "TXT更新" in stripped and "失敗" in stripped:
             replacement = line.split("showToast")[0] + 'showToast("TXT更新の起動に失敗しました。");\n'
        elif "TXT更新" in stripped:
             replacement = line.split("showToast")[0] + 'showToast("TXT更新を開始しました。");\n'
        
        elif "相場" in stripped and "コピ" in stripped:
             replacement = line.split("showToast")[0] + 'showToast("相場メモをコピーしました。");\n'
        elif "まだありません" in stripped:
             replacement = line.split("showToast")[0] + 'showToast("相場メモがまだありません。");\n'
             
        elif "code.txt" in stripped and "開けません" in stripped:
             replacement = line.split("showToast")[0] + 'showToast("code.txt を開けませんでした。");\n'
        elif "code.txt" in stripped and "開きました" in stripped:
             replacement = line.split("showToast")[0] + 'showToast("code.txt を開きました。");\n'
             
        elif "エクスポート" in stripped and "キャンセル" in stripped:
             replacement = line.split("showToast")[0] + 'showToast(ok ? "銘柄一覧をエクスポートしました。" : "エクスポートをキャンセルしました。");\n'
        elif "エクスポート" in stripped and "失敗" in stripped:
             replacement = line.split("showToast")[0] + 'showToast("銘柄一覧のエクスポートに失敗しました。");\n'
        elif "エクスポート" in stripped:
             replacement = line.split("showToast")[0] + 'showToast("エクスポート対象の銘柄がありません。");\n'
             
        elif "MEEMEE_DATA_DIR" in stripped and "失敗" in stripped:
             replacement = line.split("showToast")[0] + 'showToast("MEEMEE_DATA_DIR の保存に失敗しました。");\n'
        elif "MEEMEE_DATA_DIR" in stripped:
             replacement = line.split("showToast")[0] + 'showToast("MEEMEE_DATA_DIR を更新しました。再起動してください。");\n'

        if replacement != line:
            print(f"Replaced: {stripped} -> {replacement.strip()}")
            new_lines.append(replacement)
        else:
            # Maybe just replace with safe "TODO" if we can't key it?
            # But the logic above covers most errors seen.
            # If line still has corrupted chars, it breaks build.
            # Let's clean it aggressively if we can't identify.
            if "showToast" in replacement:
                # Last resort cleanup
                print(f"Aggressive clean on: {stripped}")
                # Keep indentation
                indent = line.split("showToast")[0]
                new_lines.append(indent + 'showToast("Message restored");\n')
            else:
                new_lines.append(line)
                
    elif "setDataDirMessage" in stripped and ("E" in stripped or "\ufffd" in stripped):
         # Handle setDataDirMessage
         replacement = line
         if "パス" in stripped:
             replacement = line.split("setDataDirMessage")[0] + 'setDataDirMessage("パスを入力してください。");\n'
         elif "再起動" in stripped:
             replacement = line.split("setDataDirMessage")[0] + 'setDataDirMessage(res.data?.message ?? "保存しました。アプリを再起動してください。");\n'
         elif "失敗" in stripped:
             replacement = line.split("setDataDirMessage")[0] + 'setDataDirMessage(`保存に失敗しました: ${detail}`);\n'
             
         if replacement != line:
             print(f"Replaced: {stripped} -> {replacement.strip()}")
             new_lines.append(replacement)
         else:
             # Aggressive
             print(f"Aggressive clean on: {stripped}")
             indent = line.split("setDataDirMessage")[0]
             new_lines.append(indent + 'setDataDirMessage("Message restored");\n')




    elif "label:" in stripped and (not all(ord(c) < 128 for c in stripped)):
         # Handle labels forcefully
         indent = line.split("{")[0] if "{" in line else ""
         
         if "performance" in stripped:
             replacement = indent + '{ key: "performance", label: "騰落率" }  // Period selected via dropdown\n'
         elif "upScore" in stripped:
             replacement = indent + '{ key: "upScore", label: "上昇スコア" },\n'
         elif "downScore" in stripped:
             replacement = indent + '{ key: "downScore", label: "下落スコア" },\n'
         elif "overheatUp" in stripped:
             replacement = indent + '{ key: "overheatUp", label: "過熱(上)" },\n'
         elif "overheatDown" in stripped:
             replacement = indent + '{ key: "overheatDown", label: "過熱(下)" },\n'
         elif "boxState" in stripped:
             replacement = indent + 'options: [{ key: "boxState", label: "ボックス状態" }]\n'
         elif "ma20Dev" in stripped:
             replacement = indent + '{ key: "ma20Dev", label: "乖離率(MA20)" },\n'
         elif "ma60Dev" in stripped:
             replacement = indent + '{ key: "ma60Dev", label: "乖離率(MA60)" },\n'
         elif "ma20Slope" in stripped:
             replacement = indent + '{ key: "ma20Slope", label: "MA20傾き" },\n'
         elif "ma60Slope" in stripped:
             replacement = indent + '{ key: "ma60Slope", label: "MA60傾き" },\n'
         elif "buyCandidate" in stripped:
             replacement = indent + '{ key: "buyCandidate", label: "買い候補(総合)" },\n'
         elif "buyInitial" in stripped:
             replacement = indent + '{ key: "buyInitial", label: "買い候補(初動)" },\n'
         elif "buyBase" in stripped:
             replacement = indent + '{ key: "buyBase", label: "買い候補(底がため)" },\n'
         elif "shortScore" in stripped:
             replacement = indent + '{ key: "shortScore", label: "売り候補(総合)" },\n'
         elif "aScore" in stripped:
             replacement = indent + '{ key: "aScore", label: "売り候補(反転確実)" },\n'
         elif "bScore" in stripped:
             replacement = indent + '{ key: "bScore", label: "売り候補(戻り売り)" },\n'
         elif "code" in stripped and "label" in stripped: # key: "code"
             replacement = indent + '{ key: "code", label: "コード" },\n'
         elif "name" in stripped and "label" in stripped:
             replacement = indent + '{ key: "name", label: "銘柄名" },\n'
         elif "sector" in stripped and "label" in stripped:
             replacement = indent + '{ key: "sector", label: "業種" }\n'
         else:
             replacement = line 

         if replacement != line:
             print(f"Fixed label: {stripped.strip()}")
             new_lines.append(replacement)
         else:
             new_lines.append(line)

    elif "sortDirLabel" in stripped and ("E" in stripped or "\ufffd" in stripped):
         new_lines.append('  const sortDirLabel = sortDir === "desc" ? "降順" : "昇順";\n')

    else:
        new_lines.append(line)

with open(path, "w", encoding="utf-8") as f:
    f.writelines(new_lines)
