# Tradex Similarity Feedback Integration v1

## Purpose

Mimi の類似チャート分析を、Tradex の state evaluation と promotion review に正式接続する。これにより、今の候補銘柄に似た過去ケースのその後の推移を、判定スコアと理由表示に使えるようにする。

この変更後は、candidate baseline 実行時に候補コードだけ similarity case library を再利用し、`類似ケースの平均パス`, `big up/down 傾向`, `近傍件数` を state eval の score と reason text に反映する。promotion review の summary でも similarity support が見える。

## Progress

- [x] 類似チャート helper の再利用経路を確認した
- [x] state eval に similarity support を組み込んだ
- [x] candidate/nightly/replay へ similarity_db_path を通した
- [x] 関連テストを通す
- [x] 結果を振り返る

## Design

既存の `external_analysis/similarity/baseline.py` には、候補コードごとの query vector と case library を引く helper がある。これを `external_analysis/models/state_eval_baseline.py` から再利用し、候補コードだけを対象に top-k の類似ケースを集計する。

集計値は `avg_path_20`, `success_rate`, `big_drop_rate`, `big_up_rate`, `avg_similarity_score`, `neighbor_count` とする。long は「似たケースがその後上がったか」「大きく崩れたか」、short は「似たケースがその後下がったか」「大きく踏み上げたか」で signal を作る。

この similarity signal は teacher signal と並ぶ補助根拠として score に入れる。reason text では `Similar charts rose after setup` や `Similar charts often squeezed up` のような短文へ変換する。promotion review では champion/challenger の平均 similarity support を summary に保存し、challenger が similarity support を悪化させた時は reason code に出す。

## Implementation Notes

主な変更ファイルは `external_analysis/models/state_eval_baseline.py`、`external_analysis/models/candidate_baseline.py`、`external_analysis/runtime/nightly_pipeline.py`、`external_analysis/runtime/historical_replay.py`、`external_analysis/__main__.py` だ。public schema を増やさず、`reason_text_top3` と readiness `summary_json` を強化する。

テストは candidate baseline と nightly、similarity baseline の3系統で確認する。

    cd C:\work\meemee-screener
    python -m pytest tests\test_external_analysis_candidate_baseline.py tests\test_phase2_slice_f_nightly_pipeline.py tests\test_phase3_similarity_baseline.py tests\test_phase3_similarity_nightly_pipeline.py

必要に応じて API 回帰も行う。

    cd C:\work\meemee-screener
    python -m pytest tests\test_analysis_bridge_api.py

## Surprises & Discoveries

- similarity helper は private 関数だが、候補コード限定で再利用するには十分だった。
- state eval に similarity を入れても public table schema を増やす必要はなく、`reason_text_top3` と readiness `summary_json` の拡張だけで観測可能性を確保できる。
- 5本まとめの pytest はタイムアウトしやすかった。candidate/nightly と similarity/API に束を分けると安定して通った。

## Decision Log

- 類似ケースは `anchor_date < as_of_date` と `neighbor_code != query_code` を必須にして、自己参照と未来混入を抑える。
- similarity support は `teacher profile` そのものには保存せず、state eval 実行時の補助 signal として扱う。
- similarity evidence が無い時は gate を落とさず中立 (`0.5`) に戻す。

## Outcomes & Retrospective

state eval は、候補ごとに `top-k 類似ケース` の平均パスと big up/down 傾向を読むようになった。これにより、判定 score・理由テキスト・promotion review summary のすべてに Mimi の類似チャート分析が入った。

検証は `tests/test_external_analysis_candidate_baseline.py`、`tests/test_phase2_slice_f_nightly_pipeline.py`、`tests/test_phase3_similarity_baseline.py`、`tests/test_phase3_similarity_nightly_pipeline.py`、`tests/test_analysis_bridge_api.py` で通した。次の大きな仕事は、ローソク足組み合わせ研究をこの similarity/tag/teacher 軸へ統合することだ。
