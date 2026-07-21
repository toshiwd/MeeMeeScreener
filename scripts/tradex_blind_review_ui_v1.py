"""Generate a machine-label-free local HTML reviewer for the frozen chart board."""
import argparse
import hashlib
import json
import shutil
from pathlib import Path


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    image_dir = args.output / "images"
    image_dir.mkdir()

    source_rows = [json.loads(line) for line in args.manifest.read_text(encoding="utf-8").splitlines() if line.strip()]
    rows = []
    for row in source_rows:
        source = args.manifest.parent / row["image_relpath"]
        destination = image_dir / source.name
        shutil.copy2(source, destination)
        rows.append({
            "case_id": row["case_id"],
            "code": row["code"],
            "ymd": row["ymd"],
            "image": f"images/{destination.name}",
        })
    if len(rows) != 40:
        raise RuntimeError(f"expected 40 cases, got {len(rows)}")

    contract = {
        "schema_version": "tradex_blind_human_annotation_v1",
        "machine_labels_visible": False,
        "outcomes_visible": False,
        "weekly_visible": False,
        "new_entry_decision": ["SELL", "AVOID", "WAIT"],
        "existing_short_management": ["NA", "HOLD", "TAKE_PROFIT", "FULL_HEDGE", "REENTRY"],
        "entry_stage": ["NA", "PROBE", "CORE", "ADD"],
        "confidence": ["LOW", "MEDIUM", "HIGH"],
        "required_fields": ["new_entry_decision", "existing_short_management", "entry_stage", "confidence"],
    }
    contract_path = args.output / "annotation_contract.json"
    contract_path.write_text(json.dumps(contract, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    cases_json = json.dumps(rows, ensure_ascii=False)
    storage_key = f"tradex-blind-review-{sha(args.manifest)[:16]}"
    html = f"""<!doctype html>
<html lang="ja"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>TRADEX ブラインド行動レビュー</title>
<style>
body{{margin:0;background:#f3f0e9;color:#172536;font-family:system-ui,'Yu Gothic UI',sans-serif}}header{{position:sticky;top:0;z-index:3;background:#fffdf8;border-bottom:1px solid #d7d1c5;padding:10px 18px;display:flex;gap:16px;align-items:center;flex-wrap:wrap}}header strong{{font-size:18px}}button,select,input,textarea{{font:inherit}}button{{padding:7px 12px;border:1px solid #9aa7b5;border-radius:6px;background:white;cursor:pointer}}button.primary{{background:#174f7a;color:white;border-color:#174f7a}}#progress{{font-weight:700}}main{{max-width:1500px;margin:16px auto;padding:0 16px 32px}}.card{{background:white;border:1px solid #d5dce3;border-radius:9px;overflow:hidden;box-shadow:0 2px 10px #0001}}.meta{{padding:12px 16px;display:flex;gap:20px;align-items:center;border-bottom:1px solid #e1e5e9}}.meta b{{font-size:22px}}img{{display:block;width:100%;height:auto;background:#fff}}.form{{display:grid;grid-template-columns:repeat(4,minmax(150px,1fr));gap:12px;padding:16px;border-top:1px solid #e1e5e9}}label{{display:grid;gap:5px;font-size:13px;font-weight:700}}textarea{{grid-column:1/-1;min-height:72px;padding:8px}}select,input{{padding:7px;border:1px solid #aeb8c2;border-radius:5px}}.nav{{display:flex;justify-content:space-between;padding:14px 16px;border-top:1px solid #e1e5e9}}.done{{color:#08723d}}.warning{{color:#9a4b00;font-size:13px}}@media(max-width:800px){{.form{{grid-template-columns:1fr 1fr}}}}
</style></head><body>
<header><strong>TRADEX ブラインド行動レビュー</strong><span id="progress"></span><span class="warning">月足選定→日足判断。週足・機械判定・将来結果は非表示。</span><button onclick="exportJson()">JSON出力</button><button onclick="exportCsv()">CSV出力</button><button onclick="importJson()">JSON読込</button><input id="importFile" type="file" accept="application/json" hidden></header>
<main><div class="card"><div class="meta"><b id="caseId"></b><span id="ticker"></span><span id="date"></span><span id="status"></span></div><img id="chart" alt="review chart"><div class="form">
<label>新規売り<select id="new_entry_decision"><option value="">未選択</option><option>SELL</option><option>AVOID</option><option>WAIT</option></select></label>
<label>既存売り管理<select id="existing_short_management"><option value="">未選択</option><option>NA</option><option>HOLD</option><option>TAKE_PROFIT</option><option>FULL_HEDGE</option><option>REENTRY</option></select></label>
<label>投入段階<select id="entry_stage"><option value="">未選択</option><option>NA</option><option>PROBE</option><option>CORE</option><option>ADD</option></select></label>
<label>確信度<select id="confidence"><option value="">未選択</option><option>LOW</option><option>MEDIUM</option><option>HIGH</option></select></label>
<label style="grid-column:1/-1">理由コード（カンマ区切り）<input id="reason_codes" placeholder="例: MONTHLY_TOP, TRY_FAIL, MA20_BREAK, SUPPORT_NEAR"></label>
<label style="grid-column:1/-1">判断メモ<textarea id="reviewer_note" placeholder="環境、形状、MA位置、支持抵抗、打診/本玉/追加の根拠"></textarea></label>
</div><div class="nav"><button onclick="move(-1)">← 前へ</button><button class="primary" onclick="saveAndNext()">保存して次へ →</button></div></div></main>
<script>
const cases={cases_json}; const storageKey={json.dumps(storage_key)}; let index=0; let answers=JSON.parse(localStorage.getItem(storageKey)||'{{}}');
const fields=['new_entry_decision','existing_short_management','entry_stage','confidence','reason_codes','reviewer_note'];
function current(){{return cases[index]}} function complete(a){{return ['new_entry_decision','existing_short_management','entry_stage','confidence'].every(k=>a&&a[k])}}
function save(){{const c=current();const a=answers[c.case_id]||{{}};fields.forEach(k=>a[k]=document.getElementById(k).value);a.case_id=c.case_id;a.code=c.code;a.ymd=c.ymd;a.reviewed_at=new Date().toISOString();answers[c.case_id]=a;localStorage.setItem(storageKey,JSON.stringify(answers));renderProgress()}}
function render(){{const c=current(),a=answers[c.case_id]||{{}};caseId.textContent=c.case_id;ticker.textContent='銘柄 '+c.code;date.textContent='基準日 '+c.ymd;chart.src=c.image;fields.forEach(k=>document.getElementById(k).value=a[k]||'');status.textContent=complete(a)?'入力済み':'';status.className=complete(a)?'done':'';renderProgress();scrollTo(0,0)}}
function renderProgress(){{const n=cases.filter(c=>complete(answers[c.case_id])).length;progress.textContent=`${{index+1}} / ${{cases.length}}（完了 ${{n}}）`}}
function move(d){{save();index=Math.max(0,Math.min(cases.length-1,index+d));render()}} function saveAndNext(){{save();if(index<cases.length-1)index++;render()}}
function download(name,text,type){{const a=document.createElement('a');a.href=URL.createObjectURL(new Blob([text],{{type}}));a.download=name;a.click();URL.revokeObjectURL(a.href)}}
function exportJson(){{save();download('tradex_blind_human_annotations.json',JSON.stringify({{schema_version:'tradex_blind_human_annotation_v1',annotations:cases.map(c=>answers[c.case_id]||{{case_id:c.case_id,code:c.code,ymd:c.ymd}})}},null,2),'application/json')}}
function csvCell(v){{return JSON.stringify(String(v??''))}} function exportCsv(){{save();const keys=['case_id','code','ymd',...fields,'reviewed_at'];const lines=[keys.join(','),...cases.map(c=>keys.map(k=>csvCell((answers[c.case_id]||c)[k])).join(','))];download('tradex_blind_human_annotations.csv','\ufeff'+lines.join('\\n'),'text/csv')}}
function importJson(){{importFile.click()}} importFile.onchange=async e=>{{const p=JSON.parse(await e.target.files[0].text());for(const a of p.annotations||[])answers[a.case_id]=a;localStorage.setItem(storageKey,JSON.stringify(answers));render()}};render();
</script></body></html>"""
    html_path = args.output / "review_board.html"
    html_path.write_text(html, encoding="utf-8")
    audit = {
        "schema_version": "tradex_blind_review_ui_v1.audit",
        "case_count": len(rows),
        "machine_labels_visible": False,
        "outcomes_visible": False,
        "weekly_visible": False,
        "manifest_sha256": sha(args.manifest),
        "contract_sha256": sha(contract_path),
        "html_sha256": sha(html_path),
    }
    audit_path = args.output / "audit.json"
    audit_path.write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "audit.json", "sha256": sha(audit_path)}, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"output": str(args.output), **audit}, indent=2))


if __name__ == "__main__":
    main()
