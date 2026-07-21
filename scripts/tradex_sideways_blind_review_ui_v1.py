from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from pathlib import Path


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build_ui(manifest: Path, output: Path) -> Path:
    output.mkdir(parents=True, exist_ok=False)
    (output / "images").mkdir()
    source = [json.loads(line) for line in manifest.read_text(encoding="utf-8").splitlines() if line.strip()]
    rows = []
    for row in source:
        image = manifest.parent / row["image_relpath"]
        destination = output / "images" / image.name
        shutil.copy2(image, destination)
        rows.append({"case_id": row["case_id"], "code": row["code"], "ymd": row["ymd"], "image": f"images/{image.name}"})
    cases = json.dumps(rows, ensure_ascii=True)
    storage_key = f"tradex-sideways-{sha(manifest)[:16]}"
    html = """<!doctype html><html lang="ja"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>TRADEX 横ばいブラインドレビュー</title><style>
body{margin:0;background:#eee9df;font-family:system-ui,'Yu Gothic UI',sans-serif;color:#162536}header{position:sticky;top:0;background:#fff;padding:10px 18px;display:flex;gap:14px;align-items:center;z-index:2;border-bottom:1px solid #ccc;flex-wrap:wrap}main{max-width:1700px;margin:16px auto;padding:0 16px}.card{background:#fff;border:1px solid #ccd3da;border-radius:8px;overflow:hidden}.meta,.controls{padding:12px 16px;display:flex;gap:18px;align-items:center;flex-wrap:wrap}img{width:100%;display:block}button,select,textarea{font:inherit;padding:8px}textarea{flex:1;min-width:320px;min-height:44px}.primary{background:#174f7a;color:#fff}.note{font-size:13px;color:#7a4a00}.hint{font-size:12px;color:#526579}</style></head><body>
<header><b>TRADEX 横ばいブラインドレビュー</b><span id="progress"></span><span class="note">基準日までの月足・日足だけで判定。検出器の答え・サンプル群・未来結果は非表示。</span><button id="exportButton">JSON出力</button></header>
<main><div class="card"><div class="meta"><b id="caseId"></b><span id="ticker"></span><span id="date"></span><span class="hint">1=横ばい / 2=横ばいではない / 3=境界・迷う</span></div><img id="chart" alt="monthly and daily chart"><div class="controls">
<label>判定 <select id="decision"><option value="">未選択</option><option value="SIDEWAYS">横ばい</option><option value="NOT_SIDEWAYS">横ばいではない</option><option value="BORDERLINE">境界・迷う</option></select></label>
<label>確信度 <select id="confidence"><option value="">未選択</option><option value="LOW">低</option><option value="MEDIUM">中</option><option value="HIGH">高</option></select></label>
<textarea id="note" placeholder="任意メモ"></textarea></div><div class="controls"><button id="previous">前へ</button><button class="primary" id="next">保存して次へ</button></div></div></main>
<script>const cases=__CASES__,key=__KEY__;let index=0;const answers=JSON.parse(localStorage.getItem(key)||'{}');
const byId=id=>document.getElementById(id);const decision=byId('decision'),confidence=byId('confidence'),note=byId('note');
function save(){const c=cases[index];answers[c.case_id]={case_id:c.case_id,code:c.code,ymd:c.ymd,sideways_decision:decision.value,confidence:confidence.value,reviewer_note:note.value,reviewed_at:new Date().toISOString()};localStorage.setItem(key,JSON.stringify(answers));renderProgress()}
function render(){const c=cases[index],a=answers[c.case_id]||{};byId('caseId').textContent=c.case_id;byId('ticker').textContent='銘柄 '+c.code;byId('date').textContent='基準日 '+c.ymd;byId('chart').src=c.image;decision.value=a.sideways_decision||'';confidence.value=a.confidence||'';note.value=a.reviewer_note||'';renderProgress();scrollTo(0,0)}
function renderProgress(){byId('progress').textContent=`${index+1} / ${cases.length} - 回答済み ${cases.filter(c=>answers[c.case_id]?.sideways_decision&&answers[c.case_id]?.confidence).length}`}
function move(delta){save();index=Math.max(0,Math.min(cases.length-1,index+delta));render()}
async function exportJson(){save();const missing=cases.filter(c=>!answers[c.case_id]?.sideways_decision||!answers[c.case_id]?.confidence);if(missing.length){alert(`未回答があります: ${missing.map(c=>c.case_id).join(', ')}`);return}const payload={schema_version:'tradex_sideways_blind_annotation_v1',annotations:cases.map(c=>answers[c.case_id])};try{const response=await fetch('/save-annotations',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});const result=await response.json();if(!response.ok)throw new Error(result.error||`HTTP ${response.status}`);alert(`JSONを保存しました: ${result.saved_path}`);return}catch(error){console.warn('direct save unavailable; falling back to browser download',error)}const link=document.createElement('a');const objectUrl=URL.createObjectURL(new Blob([JSON.stringify(payload,null,2)],{type:'application/json'}));link.href=objectUrl;link.download='tradex_sideways_blind_annotations.json';document.body.appendChild(link);link.click();link.remove();setTimeout(()=>URL.revokeObjectURL(objectUrl),1000)}
byId('previous').onclick=()=>move(-1);byId('next').onclick=()=>move(1);byId('exportButton').onclick=exportJson;document.addEventListener('keydown',e=>{if(e.target.tagName==='TEXTAREA'||e.target.tagName==='SELECT')return;if(e.key==='1')decision.value='SIDEWAYS';if(e.key==='2')decision.value='NOT_SIDEWAYS';if(e.key==='3')decision.value='BORDERLINE'});render();</script></body></html>"""
    html = html.replace("__CASES__", cases).replace("__KEY__", json.dumps(storage_key))
    html_path = output / "review_board.html"
    html_path.write_text(html, encoding="utf-8")
    contract = {
        "schema_version": "tradex_sideways_blind_annotation_v1", "case_count": len(rows),
        "valid_decisions": ["SIDEWAYS", "NOT_SIDEWAYS", "BORDERLINE"], "valid_confidence": ["LOW", "MEDIUM", "HIGH"],
        "required_fields": ["sideways_decision", "confidence"], "machine_labels_visible": False,
        "sample_groups_visible": False, "outcomes_visible": False, "weekly_visible": False,
    }
    contract_path = output / "annotation_contract.json"
    contract_path.write_text(json.dumps(contract, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {**contract, "manifest_sha256": sha(manifest), "html_sha256": sha(html_path), "utf8_japanese_labels": True}
    audit_path = output / "audit.json"
    audit_path.write_text(json.dumps(audit, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "audit.json", "sha256": sha(audit_path)}, indent=2) + "\n", encoding="utf-8")
    return html_path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(build_ui(args.manifest, args.output))


if __name__ == "__main__":
    main()
