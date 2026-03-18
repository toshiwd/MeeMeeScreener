# Tradex Candle Combo Tags v1

## Purpose

ローソク足の単独パターンと簡易組み合わせを、Tradex の strategy tag として扱えるようにする。これにより、既存の AI Research 画面で `ろうそく足タグごとの件数・期待値・失敗例` をそのまま研究できる。

この変更後は、`indicator_daily_export.candle_flags` と前日フラグから `bullish_engulfing` や `shooting_star_reversal` のようなタグが state eval に入り、teacher profile・tag rollup・promotion review の文脈に乗る。

## Progress

- [x] 既存の candle_flags 取り込み経路を確認した
- [x] candidate input に candle context を追加する
- [x] state eval の strategy tag に candle combo を統合する
- [x] 既存テストを更新して通す
- [x] 結果を振り返る

## Design

`external_analysis/models/candidate_baseline.py` の input frame へ `candle_flags`, `prev_candle_flags`, `prev2_candle_flags` を追加する。`external_analysis/models/state_eval_baseline.py` では、既存の `box_breakout` や `ma20_reclaim` に加えて、candle flags から broad に拾うタグを追加する。

long は `bullish_engulfing`, `hammer_reversal`, `inside_break_bull`, `bullish_follow_through` を候補にする。short は `bearish_engulfing`, `shooting_star_reversal`, `inside_break_bear`, `bearish_follow_through` を候補にする。タグは広めに拾い、詳細な精緻化は tag rollup の結果を見てから行う。

## Implementation Notes

編集対象は `external_analysis/models/candidate_baseline.py` と `external_analysis/models/state_eval_baseline.py`、確認用の `tests/test_external_analysis_candidate_baseline.py` だ。public schema は増やさず、既存の `strategy_tags` と `reason_text_top3` を拡張するだけに留める。

検証は次で行う。

    cd C:\work\meemee-screener
    python -m pytest tests\test_external_analysis_candidate_baseline.py tests\test_phase2_slice_f_nightly_pipeline.py

## Surprises & Discoveries

- 新テーブルを作らなくても、`strategy_tags -> tag rollup -> AI Research` の既存導線だけで足形研究を始められた。
- `candle_flags` 単独よりも、`prev_candle_flags` を見た broad な 2本組み合わせの方が strategy tag として扱いやすかった。

## Decision Log

- candle 研究は新テーブルを作らず、既存の tag rollup を再利用する。
- タグ判定は strict にせず broad に拾う。最初は研究母数を確保する方を優先する。

## Outcomes & Retrospective

candidate input は `candle_flags`, `prev_candle_flags`, `prev2_candle_flags` を持つようになり、state eval は `bullish_engulfing`, `hammer_reversal`, `inside_break_bull`, `bearish_engulfing`, `shooting_star_reversal` などの足形タグを strategy tag として出すようになった。

検証は `tests/test_external_analysis_candidate_baseline.py` と `tests/test_phase2_slice_f_nightly_pipeline.py` で通した。これで AI Research 画面に足形タグがそのまま流れ、件数・期待値・失敗例の研究を始められる。
