# Tradex Teacher Profile Weighting v1

## Purpose

Tradex の state evaluation は、実売買履歴を教師にする設計だが、現状は `code + side` の偏りが中心で、`holding_band` と `strategy_tag` の好みが十分にスコアへ効いていない。今回の目的は、実売買履歴から「どの side / 保有帯 / 手法タグをどれだけ使っているか」を復元し、state eval があなたの実運用にもっと寄るようにすることだ。

この変更後は、`external_trade_teacher_profiles` の中身にタグ別の集計が入り、`state_eval_daily` の confidence と shadow readiness がその教師情報を反映する。確認方法は、candidate baseline のテストと AI Research の tag rollup / promotion review を見ることだ。

## Progress

- [x] 現状の teacher profile と scoring の接続点を確認した
- [x] 実装方針を `tag/band 集計 + 信頼度重み` に決めた
- [x] teacher profile 生成を強化する
- [x] state eval scoring へ新しい教師重みを入れる
- [x] テストを更新して通す

## Design

`external_analysis/models/state_eval_baseline.py` にある `_load_trade_teacher_profile` は、いま `trade_event_export` と `position_snapshot_export` から銘柄ごとの long/short 偏りしか作っていない。ここを拡張して、過去の entry event が発生した営業日の feature を引き直し、その日の `holding_band` と `strategy_tag` を同じ関数群で再計算して集計する。

集計は 3 層に分ける。第一に `code + side` の銘柄嗜好、第二に `side + holding_band` の保有帯嗜好、第三に `side + holding_band + strategy_tag` の手法嗜好だ。各候補の teacher score は、この 3 層の一致度を `trade_count` に応じた信頼度で混ぜて作る。件数が少ない時は 0.5 へ寄せ、件数が増えるほど嗜好が強く出るようにする。

state eval 側では既存の `alignment_score` と `position_bias` だけでなく、`band_alignment`、`tag_alignment`、`confidence_weight` を teacher summary に含める。scoring は既存の trend/momentum/risk を保ちつつ、teacher 部分を「強い嗜好があり、かつ件数も十分な時にだけ効く」形へ変える。

## Implementation Notes

編集対象は主に `external_analysis/models/state_eval_baseline.py` とテストの `tests/test_external_analysis_candidate_baseline.py` だ。必要なら `summary_json` の中に新しい教師指標を追加するが、public schema は変えない。`external_trade_teacher_profiles` の列追加は避け、再利用しやすいように `summary_json` を拡張する。

実装後は次の確認を行う。

    cd C:\work\meemee-screener
    python -m pytest tests\test_external_analysis_candidate_baseline.py

必要に応じて関連回帰も行う。

    cd C:\work\meemee-screener
    python -m pytest tests\test_analysis_bridge_api.py tests\test_phase2_slice_f_nightly_pipeline.py

## Surprises & Discoveries

- `docs/MEEMEE_PRINCIPLES.md` は文字化けして読みにくいが、研究系は Tradex 側に閉じ、MeeMee は publish 済み read-only を表示する原則は既存実装と整合している。
- 現状の teacher profile は `holding_band` と `strategy_tag` のキーを持っているが、値は銘柄ごとの side 偏りを複製しているだけで、タグ学習にはなっていない。
- `trade_event_export` の entry event と当日の export feature を join するだけで、手法タグの傾向を十分再現できた。public schema を増やさず `summary_json` 拡張で収まった。
- nightly と API の既存テストは、teacher signal を強めても壊れなかった。state eval の public 契約を触らなければ波及は小さい。

## Decision Log

- `teacher profile` の public schema は増やさない。internal summary を濃くして scoring に効かせる。
- 履歴集計は `entry event` ベースで再構築する。exit や全イベント混在にすると意味がぶれるため、まずは entry のみ使う。
- タグ一致の強さは `0.5 へ収束する信頼度重み` を使う。件数ゼロ付近で過剰反応しないことを優先する。

## Outcomes & Retrospective

teacher profile は `code + side` の偏りだけでなく、`side + holding_band + strategy_tag` の実売買傾向を internal summary に持つようになった。state eval scoring はその profile を `confidence_weight` 付きで使うようになり、件数が十分な手法だけが強く効く設計になった。

検証は `tests/test_external_analysis_candidate_baseline.py`、`tests/test_analysis_bridge_api.py`、`tests/test_phase2_slice_f_nightly_pipeline.py` で通した。残る大きな仕事は、ローソク足組み合わせ研究をこの teacher/tag 軸へ統合することだ。
