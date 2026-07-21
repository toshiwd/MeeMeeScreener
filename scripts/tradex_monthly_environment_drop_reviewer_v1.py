"""Build a post-reveal review page for completed downside outcomes."""
import argparse, html, json, shutil
from pathlib import Path

import pandas as pd


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ledger", type=Path, required=True)
    ap.add_argument("--image-dir", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    args = ap.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    images = args.output / "images"
    images.mkdir()

    ledger = pd.read_parquet(args.ledger)
    drops = ledger[ledger.outcome_fixed3.eq("D")].copy()
    drops["missed"] = ~drops.human_sell
    drops = drops.sort_values(["missed", "case_id"], ascending=[False, True])

    cards = []
    rows = []
    for row in drops.itertuples():
        matches = list(args.image_dir.glob(f"{row.case_id}_{row.code}_{int(row.ymd)}.png"))
        if len(matches) != 1:
            raise RuntimeError(f"image parity failed for {row.case_id}: {matches}")
        src = matches[0]
        dst = images / src.name
        shutil.copy2(src, dst)
        original = "SELL" if bool(row.human_sell) else "WAIT / AVOID"
        group = "MISSED_DOWNSIDE" if not bool(row.human_sell) else "CAUGHT_DOWNSIDE"
        cards.append(
            f'<section class="card"><div class="meta"><b>{html.escape(row.case_id)} / {html.escape(str(row.code))}</b>'
            f'<span>{int(row.ymd)}</span><span class="{group}">{group}</span>'
            f'<span>original: {original}</span><span>return: {float(row.return_fixed3_pct):+.3f}%</span>'
            f'<span>exit: {html.escape(str(row.exit_reason_fixed3))}</span></div>'
            f'<img src="images/{html.escape(src.name)}" alt="{html.escape(row.case_id)} chart"></section>'
        )
        rows.append({
            "case_id": row.case_id,
            "code": str(row.code),
            "ymd": int(row.ymd),
            "original_human_direction": str(row.human_direction),
            "review_group": group,
            "outcome_fixed3": "D",
            "return_fixed3_pct": float(row.return_fixed3_pct),
            "exit_reason_fixed3": str(row.exit_reason_fixed3),
            "image": f"images/{src.name}",
        })

    document = """<!doctype html><html lang="ja"><head><meta charset="utf-8">
<title>TRADEX downside outcome review</title><style>
body{margin:0;background:#eee9df;color:#172536;font-family:system-ui,'Yu Gothic UI',sans-serif}
header{position:sticky;top:0;z-index:2;background:#fff;border-bottom:1px solid #ccd3da;padding:12px 18px}
main{max-width:1800px;margin:16px auto;padding:0 16px}.card{background:#fff;border:1px solid #ccd3da;border-radius:8px;margin-bottom:20px;overflow:hidden}
.meta{display:flex;gap:16px;align-items:center;padding:10px 14px;flex-wrap:wrap}.MISSED_DOWNSIDE{color:#a33;font-weight:700}.CAUGHT_DOWNSIDE{color:#176b3a;font-weight:700}
img{display:block;width:100%}.note{color:#6b4b00;font-size:13px}</style></head><body>
<header><b>下落成功チャート再レビュー</b> <span class="note">最初の8件は見送ったD、後半9件はSELLで捉えたD。結果開示後の診断用。</span></header>
<main>__CARDS__</main></body></html>"""
    (args.output / "review.html").write_text(document.replace("__CARDS__", "\n".join(cards)), encoding="utf-8")
    audit = {
        "schema_version": "tradex_monthly_environment_drop_reviewer_v1.audit.v1",
        "review_only": True,
        "post_reveal_diagnostic": True,
        "rows": len(rows),
        "missed_downside": sum(r["review_group"] == "MISSED_DOWNSIDE" for r in rows),
        "caught_downside": sum(r["review_group"] == "CAUGHT_DOWNSIDE" for r in rows),
        "all_outcomes_D": all(r["outcome_fixed3"] == "D" for r in rows),
        "source_ledger": str(args.ledger.resolve()),
        "not_changed": ["blind annotations", "authoritative drop decision", "MeeMee", "ranking", "runtime DB", "production trading logic"],
        "rows_detail": rows,
    }
    (args.output / "audit.json").write_text(json.dumps(audit, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: audit[k] for k in ["rows", "missed_downside", "caught_downside", "all_outcomes_D"]}, indent=2))


if __name__ == "__main__":
    main()
