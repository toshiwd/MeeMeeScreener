# TRADEX champion / challenger 実データ評価 runner

## Purpose

この作業の目的は、TRADEX で champion（現行ランキング）と challenger（新選定ロジック 1 案）を confirmed データで比較し、昇格候補にしてよいかを数値で判断できるようにすることです。

完了すると、同じ universe・同じ期間・同じ約定条件・同じ top-K での比較結果を、artifact と markdown の両方で確認できます。MeeMee 側の順位反映はまだ行わず、TRADEX の診断と評価だけを強化します。

## Progress

- [ ] 実データ evaluation runner を追加する
- [ ] 昇格条件を定数として固定する
- [ ] compare artifact に evaluation summary を追加する
- [ ] markdown レポートを出力する
- [ ] MeeMee 反映禁止をコードとテストで固定する
- [ ] 対象 pytest と py_compile を通す

## Plan

まず `app/backend/services/tradex_experiment_service.py` に、confirmed データから評価 window を選ぶ helper と、window ごとの champion / challenger 比較をまとめる runner を追加します。window は market regime の既存定義を使って、上昇・下落・横ばいの 3 区分を 60 営業日以上の連続区間から 1 つずつ選びます。

次に、昇格条件を module constant として固定し、`promote_ready` と `promote_reasons` が artifact に残るようにします。昇格判定は平均値だけでなく、中央値、月次勝率、worst regime、DD、turnover、liquidity fail をまとめて見るようにします。

最後に、`docs/reports/tradex_champion_challenger_eval.md` に human-readable な短いレポートを書き出します。レポートには評価 window、champion / challenger の定義、top5 / top10、月次、regime 別比較、昇格可否、MeeMee へまだ反映しないことを含めます。

## Implementation Notes

比較の正本は candidate compare record 側に置きます。family compare の上位には必要最小限の集約だけ置いてよく、判定の source of truth は candidate compare の evaluation summary にします。

window 抽出は手で日付を選ばず、DB の regime テーブルから連続区間を決めます。抽出できる window が不足する場合は、黙って別期間に置き換えず、`promote_ready=false` と理由を artifact に残します。

shadow 由来のフィールドは ranking_input_hash から除外したまま維持します。MeeMee の現行順位出力に challenger が影響する経路は追加しません。

## Validation

以下を実行して確認します。

    python -m py_compile app/backend/services/tradex_experiment_service.py tests/test_tradex_experiment_family_api.py
    python -m pytest tests/test_tradex_experiment_family_api.py -q

必要なら、evaluation runner を 1 回だけ dry-run して markdown と artifact の両方が出ることを確認します。

## Outcomes

成功条件は、実データで champion / challenger の比較が同一条件で出せること、昇格条件が定数で固定されること、`promote_ready` が理由付きで artifact に残ること、そして MeeMee 側の順位反映が変わらないことです。
