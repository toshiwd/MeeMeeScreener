
path = r"c:\work\meemee-screener\app\frontend\src\routes\GridView.tsx"

with open(path, "r", encoding="utf-8") as f:
    content = f.read()

# Fix double spacing (naive but likely sufficient if consistent)
# Check if widespread double spacing exists
if "\n\n" in content:
    # It might be \r\n\r\n or \n\n. Python 'r' mode handles newlines.
    # If we read in text mode, newlines are \n. 
    # If the file has empty lines between code lines, it means \n\n.
    # We want to remove *extra* newlines.
    # But wait, code often has legitimate empty lines.
    # The double spacing I saw was *every* line.
    # E.g. line 1 \n \n line 2 \n \n.
    # If I verify that *most* lines are followed by a blank line, I can collapse them.
    # A simple heuristic: replace \n\n with \n. But that kills paragraphs.
    # Better: check if it looks like uniform double spacing.
    lines = content.split("\n")
    # If more than 50% of lines are empty, and they alternate...
    empty_count = sum(1 for l in lines if not l.strip())
    if empty_count > len(lines) * 0.4: 
        # Naive compression: remove every second empty line?
        # Or just replace \n\n with \n globally?
        # Let's try replace \n\n with \n. It might merge intentional paragraphs, 
        # but code formatting (prettier) can fix that later if needed.
        # But wait, if I have:
        # code;
        # 
        # code;
        #
        # It becomes:
        # code;
        # code;
        # This is fine for JS.
        print("Removing extra newlines...")
        content = content.replace("\n\n", "\n")

# Use manual replacements for the garbled strings
replacements = {
    'showToast("トレードCSVをアチEEロードしました、E);': 'showToast("トレードCSVをアップロードしました。");',
    'showToast(`トレードCSVのアチEEロードに失敗しました、E(${detail})`);': 'showToast(`トレードCSVのアップロードに失敗しました。(${detail})`);',
    'showToast(`強制同期でエラーが発生しました、E(${errors[0]})`);': 'showToast(`強制同期でエラーが発生しました。(${errors[0]})`);',
    'showToast("強制同期を実行しました、E);': 'showToast("強制同期を実行しました。");',
    'showToast(`強制同期に失敗しました、E(${detail})`);': 'showToast(`強制同期に失敗しました。(${detail})`);',
    # Add others if I recall them from context or view_file output
    # I saw "code.txt 繧帝幕縺阪∪縺励◆縲・" -> "code.txt を開きました。"
    # "code.txt 繧帝幕縺代∪縺帙ｓ縺ｧ縺励◆縲・" -> "code.txt を開けませんでした。"
    'showToast("code.txt 繧帝幕縺阪∪縺励◆縲・);': 'showToast("code.txt を開きました。");',
    'showToast("code.txt 繧帝幕縺代∪縺帙ｓ縺ｧ縺励◆縲・);': 'showToast("code.txt を開けませんでした。");',
    'showToast("繧ｨ繧ｯ繧ｹ繝昴・繝亥ｯｾ雎｡縺ｮ驫俶氛縺後≠繧翫∪縺帙ｓ縲・);': 'showToast("エクスポート対象の銘柄がありません。");',
    'showToast(ok ? "驫俶氛荳€隕ｧ繧偵お繧ｯ繧ｹ繝昴・繝医＠縺ｾ縺励◆縲・ : "繧ｨ繧ｯ繧ｹ繝昴・繝医ｒ繧ｭ繝｣繝ｳ繧ｻ繝ｫ縺励∪縺励◆縲・);': 'showToast(ok ? "銘柄一覧をエクスポートしました。" : "エクスポートをキャンセルしました。");',
    'showToast("驫俶氛荳€隕ｧ縺ｮ繧ｨ繧ｯ繧ｹ繝昴・繝医↓螟ｱ謨励＠縺ｾ縺励◆縲・);': 'showToast("銘柄一覧のエクスポートに失敗しました。");',
    'setDataDirMessage("繝代せ繧貞・蜉帙＠縺ｦ縺上□縺輔＞縲・);': 'setDataDirMessage("パスを入力してください。");',
    'setDataDirMessage(res.data?.message ?? "菫晏ｭ倥＠縺ｾ縺励◆縲ゅい繝励Μ繧貞・襍ｷ蜍輔＠縺ｦ縺上□縺輔＞縲・);': 'setDataDirMessage(res.data?.message ?? "保存しました。アプリを再起動してください。");',
    'showToast("MEEMEE_DATA_DIR 繧呈峩譁ｰ縺励∪縺励◆縲ょ・襍ｷ蜍輔＠縺ｦ縺上□縺輔＞縲・);': 'showToast("MEEMEE_DATA_DIR を更新しました。再起動してください。");',
    'setDataDirMessage(`菫晏ｭ倥↓螟ｱ謨励＠縺ｾ縺励◆: ${detail}`);': 'setDataDirMessage(`保存に失敗しました: ${detail}`);',
    'showToast("MEEMEE_DATA_DIR 縺ｮ菫晏ｭ倥↓螟ｱ謨励＠縺ｾ縺励◆縲・);': 'showToast("MEEMEE_DATA_DIR の保存に失敗しました。");',
    
    # Also I saw "讓ｩ蛻ｩ繝・・繧ｿ遽・峇: 縲・{formatted}" in useMemo
    'return formatted ? `讓ｩ蛻ｩ繝・・繧ｿ遽・峇: 縲・{formatted}` : null;': 'return formatted ? `権利データ範囲: ～${formatted}` : null;',
    
    # "繝懊ャ繧ｯ繧ｹ" -> "ボックス"
    'title: "繝懊ャ繧ｯ繧ｹ",': 'title: "ボックス",',
    'options: [{ key: "boxState", label: "繝懊ャ繧ｯ繧ｹ迥ｶ諷・ }]': 'options: [{ key: "boxState", label: "ボックス状態" }]',
    
    # "繝・け繝九き繝ｫ" -> "テクニカル"
    'title: "繝・け繝九き繝ｫ",': 'title: "テクニカル",',
    
    # "繝代ヵ繧ｩ繝ｼ繝槭Φ繧ｹ" -> "パフォーマンス"
    'title: "繝代ヵ繧ｩ繝ｼ繝槭Φ繧ｹ",': 'title: "パフォーマンス",',
    
    # "繧ｹ繧ｳ繧｢" -> "スコア"
    'title: "繧ｹ繧ｳ繧｢",': 'title: "スコア",',
    
    # "雋ｷ縺・€呵｣懶ｼ育ｷ丞粋・・" -> "買い候補（総合）"
    '{ key: "buyCandidate", label: "雋ｷ縺・€呵｣懶ｼ育ｷ丞粋・・ },': '{ key: "buyCandidate", label: "買い候補（総合）" },',
    '{ key: "buyInitial", label: "雋ｷ縺・€呵｣懶ｼ亥・蜍包ｼ・ },': '{ key: "buyInitial", label: "買い候補（初動）" },',
    '{ key: "buyBase", label: "雋ｷ縺・€呵｣懶ｼ亥ｺ輔′縺溘ａ・・ },': '{ key: "buyBase", label: "買い候補（底がため）" },',
    
    '{ key: "shortScore", label: "螢ｲ繧雁€呵｣懶ｼ育ｷ丞粋・・ },': '{ key: "shortScore", label: "売り候補（総合）" },',
    '{ key: "aScore", label: "螢ｲ繧雁€呵｣懶ｼ亥渚霆｢遒ｺ螳夲ｼ・ },': '{ key: "aScore", label: "売り候補（反転確実）" },',
    '{ key: "bScore", label: "螢ｲ繧雁€呵｣懶ｼ域綾繧雁｣ｲ繧奇ｼ・ },': '{ key: "bScore", label: "売り候補（戻り売り）" },',
    
    # "蝓ｺ譛ｬ" -> "基本"
    'title: "蝓ｺ譛ｬ",': 'title: "基本",',
    
    '{ key: "code", label: "繧ｳ繝ｼ繝・ },': '{ key: "code", label: "コード" },',
    '{ key: "name", label: "驫俶氛蜷・ },': '{ key: "name", label: "銘柄名" },',
    '{ key: "sector", label: "讌ｭ遞ｮ" }': '{ key: "sector", label: "業種" }',
    
    # "荵夜屬邇・ｼ・A20・・" -> "乖離率(MA20)"
    '{ key: "ma20Dev", label: "荵夜屬邇・ｼ・A20・・ },': '{ key: "ma20Dev", label: "乖離率(MA20)" },',
    '{ key: "ma60Dev", label: "荵夜屬邇・ｼ・A60・・ },': '{ key: "ma60Dev", label: "乖離率(MA60)" },',
    '{ key: "ma20Slope", label: "MA20蛯ｾ縺・ },': '{ key: "ma20Slope", label: "MA20傾き" },',
    '{ key: "ma60Slope", label: "MA60蛯ｾ縺・ }': '{ key: "ma60Slope", label: "MA60傾き" }',
    
    '{ key: "performance", label: "鬨ｰ關ｽ邇・ }': '{ key: "performance", label: "騰落率" }',
    
    '{ key: "upScore", label: "荳頑・繧ｹ繧ｳ繧｢" },': '{ key: "upScore", label: "上昇スコア" },',
    '{ key: "downScore", label: "荳玖誠繧ｹ繧ｳ繧｢" },': '{ key: "downScore", label: "下落スコア" },',
    '{ key: "overheatUp", label: "驕守・・井ｸ奇ｼ・ },': '{ key: "overheatUp", label: "過熱（上）" },',
    '{ key: "overheatDown", label: "驕守・・井ｸ具ｼ・ }': '{ key: "overheatDown", label: "過熱（下）" }',
    
    'const sortDirLabel = sortDir === "desc" ? "髯埼・ : "譏・・;': 'const sortDirLabel = sortDir === "desc" ? "降順" : "昇順";',
    'gridTimeframe === "daily" ? "譌･雜ｳ" : gridTimeframe === "weekly" ? "騾ｱ雜ｳ" : "譛郁ｶｳ";': 'gridTimeframe === "daily" ? "日足" : gridTimeframe === "weekly" ? "週足" : "月足";',
    
    'if (activeConditionTimeframes.size === 0) return "譛ｪ險ｭ螳・;': 'if (activeConditionTimeframes.size === 0) return "未設定";',
    'return value === "daily" ? "譌･雜ｳ" : value === "weekly" ? "騾ｱ雜ｳ" : "譛郁ｶｳ";': 'return value === "daily" ? "日足" : value === "weekly" ? "週足" : "月足";',
    
    'return "隍・焚";': 'return "複数";',
    
    'const target = activeKey === "buyInitial" ? "蛻晏虚" : "蠎輔′縺溘ａ";': 'const target = activeKey === "buyInitial" ? "初動" : "底がため";',
}

for old, new_ in replacements.items():
    if old in content:
        content = content.replace(old, new_)
        print(f"Fixed: {new_}")

with open(path, "w", encoding="utf-8") as f:
    f.write(content)
