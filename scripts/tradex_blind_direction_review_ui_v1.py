"""Create a lightweight, outcome-blind direction review UI."""
import argparse, hashlib, json, shutil
from pathlib import Path

def sha(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def main():
    ap=argparse.ArgumentParser();ap.add_argument("--manifest",type=Path,required=True);ap.add_argument("--output",type=Path,required=True);a=ap.parse_args()
    a.output.mkdir(parents=True,exist_ok=False);(a.output/"images").mkdir()
    source=[json.loads(line) for line in a.manifest.read_text(encoding="utf-8").splitlines() if line.strip()];rows=[]
    for row in source:
        image=a.manifest.parent/row["image_relpath"];destination=a.output/"images"/image.name;shutil.copy2(image,destination)
        rows.append({"case_id":row["case_id"],"code":row["code"],"ymd":row["ymd"],"image":f"images/{image.name}"})
    cases=json.dumps(rows,ensure_ascii=True);storage_key=f"tradex-direction-{sha(a.manifest)[:16]}"
    html="""<!doctype html><html lang="ja"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>TRADEX &#x58F2;&#x308A;&#x65B9;&#x5411;&#x30D6;&#x30E9;&#x30A4;&#x30F3;&#x30C9;&#x30EC;&#x30D3;&#x30E5;&#x30FC;</title><style>
body{margin:0;background:#eee9df;font-family:system-ui,'Yu Gothic UI',sans-serif;color:#162536}header{position:sticky;top:0;background:#fff;padding:10px 18px;display:flex;gap:14px;align-items:center;z-index:2;border-bottom:1px solid #ccc}main{max-width:1700px;margin:16px auto;padding:0 16px}.card{background:#fff;border:1px solid #ccd3da;border-radius:8px;overflow:hidden}.meta,.controls{padding:12px 16px;display:flex;gap:18px;align-items:center}img{width:100%;display:block}button,select,textarea{font:inherit;padding:8px}textarea{flex:1;min-height:44px}.primary{background:#174f7a;color:#fff}.note{font-size:13px;color:#7a4a00}</style></head><body>
<header><b>&#x58F2;&#x308A;&#x65B9;&#x5411;&#x30D6;&#x30E9;&#x30A4;&#x30F3;&#x30C9;&#x30EC;&#x30D3;&#x30E5;&#x30FC;</b><span id="progress"></span><span class="note">&#x6708;&#x8DB3;&#x3067;&#x74B0;&#x5883;&#x3092;&#x8A8D;&#x8B58;&#x3057;&#x3001;&#x65E5;&#x8DB3;&#x3067;&#x65B0;&#x898F;&#x58F2;&#x308A;&#x3092;&#x5224;&#x65AD;&#x3002;&#x9031;&#x8DB3;&#x30FB;&#x30E2;&#x30C7;&#x30EB;&#x5224;&#x5B9A;&#x30FB;&#x7D50;&#x679C;&#x306F;&#x975E;&#x8868;&#x793A;&#x3002;</span><button id="exportButton">JSON&#x51FA;&#x529B;</button></header>
<main><div class="card"><div class="meta"><b id="caseId"></b><span id="ticker"></span><span id="date"></span></div><img id="chart" alt="monthly and daily chart"><div class="controls">
<label>&#x65B0;&#x898F;&#x58F2;&#x308A; <select id="decision"><option value="">&#x672A;&#x9078;&#x629E;</option><option value="SELL">SELL</option><option value="WAIT">WAIT</option><option value="AVOID">AVOID</option></select></label>
<label>&#x78BA;&#x4FE1;&#x5EA6; <select id="confidence"><option value="">&#x672A;&#x9078;&#x629E;</option><option value="LOW">LOW</option><option value="MEDIUM">MEDIUM</option><option value="HIGH">HIGH</option></select></label>
<textarea id="note" placeholder="optional note"></textarea></div><div class="controls"><button id="previous">&#x524D;&#x3078;</button><button class="primary" id="next">&#x4FDD;&#x5B58;&#x3057;&#x3066;&#x6B21;&#x3078;</button></div></div></main>
<script>const cases=__CASES__,key=__KEY__;let index=0;const answers=JSON.parse(localStorage.getItem(key)||'{}');
const byId=id=>document.getElementById(id);const decision=byId('decision'),confidence=byId('confidence'),note=byId('note');
function save(){const c=cases[index];answers[c.case_id]={case_id:c.case_id,code:c.code,ymd:c.ymd,new_entry_decision:decision.value,confidence:confidence.value,reviewer_note:note.value,reviewed_at:new Date().toISOString()};localStorage.setItem(key,JSON.stringify(answers));renderProgress()}
function render(){const c=cases[index],a=answers[c.case_id]||{};byId('caseId').textContent='E'+c.case_id.slice(1);byId('ticker').textContent=String.fromCodePoint(37528,26564)+' '+c.code;byId('date').textContent=String.fromCodePoint(22522,28310,26085)+' '+c.ymd;byId('chart').src=c.image;decision.value=a.new_entry_decision||'';confidence.value=a.confidence||'';note.value=a.reviewer_note||'';renderProgress();scrollTo(0,0)}
function renderProgress(){byId('progress').textContent=`${index+1} / ${cases.length} - `+String.fromCodePoint(22238,31572,28168,12415)+' '+cases.filter(c=>answers[c.case_id]?.new_entry_decision).length}
function move(delta){save();index=Math.max(0,Math.min(cases.length-1,index+delta));render()}
function exportJson(){save();const missing=cases.filter(c=>!answers[c.case_id]?.new_entry_decision);if(missing.length){alert(`Please answer all 32 cases. Missing: ${missing.map(c=>c.case_id).join(', ')}`);return}const payload={schema_version:'tradex_blind_direction_annotation_v1',annotations:cases.map(c=>answers[c.case_id])};const link=document.createElement('a');link.href=URL.createObjectURL(new Blob([JSON.stringify(payload,null,2)],{type:'application/json'}));link.download='tradex_blind_direction_annotations.json';link.click();URL.revokeObjectURL(link.href)}
byId('previous').onclick=()=>move(-1);byId('next').onclick=()=>move(1);byId('exportButton').onclick=exportJson;render();</script></body></html>"""
    html=html.replace("__CASES__",cases).replace("__KEY__",json.dumps(storage_key))
    html_path=a.output/"review_board.html";html_path.write_text(html,encoding="utf-8")
    contract={"schema_version":"tradex_blind_direction_annotation_v1","case_count":len(rows),"required_fields":["new_entry_decision"],"machine_labels_visible":False,"outcomes_visible":False,"weekly_visible":False}
    contract_path=a.output/"annotation_contract.json";contract_path.write_text(json.dumps(contract,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    audit={**contract,"manifest_sha256":sha(a.manifest),"html_sha256":sha(html_path),"utf8_japanese_labels":True};audit_path=a.output/"audit.json";audit_path.write_text(json.dumps(audit,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    (a.output/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"audit.json","sha256":sha(audit_path)},indent=2)+"\n",encoding="utf-8")
    print(json.dumps({"output":str(a.output),**audit},ensure_ascii=False,indent=2))
if __name__=="__main__":main()
