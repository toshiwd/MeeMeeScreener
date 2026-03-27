from __future__ import annotations

from typing import Any

import numpy as np


def rank_rows(rows: list[dict[str, Any]], *, score_key: str, descending: bool = True) -> list[dict[str, Any]]:
    return sorted(
        rows,
        key=lambda row: (
            -float(row.get(score_key) or 0.0) if descending else float(row.get(score_key) or 0.0),
            str(row.get("code") or ""),
        ),
    )


def compute_top_k_metrics(*, base_rows: list[dict[str, Any]], fused_rows: list[dict[str, Any]], k: int) -> dict[str, Any]:
    base_top = rank_rows(base_rows, score_key="base_score")[:k]
    fused_top = rank_rows(fused_rows, score_key="fused_score")[:k]
    base_returns = [float(row.get("future_return") or 0.0) for row in base_top]
    fused_returns = [float(row.get("future_return") or 0.0) for row in fused_top]
    base_negative = sum(1 for row in base_top if str(row.get("label_bucket") or "") == "negative")
    fused_negative = sum(1 for row in fused_top if str(row.get("label_bucket") or "") == "negative")
    base_codes = [str(row.get("code") or "") for row in base_top]
    fused_codes = [str(row.get("code") or "") for row in fused_top]
    return {
        "top_k": int(k),
        "base_mean_future_return": float(np.mean(base_returns)) if base_returns else 0.0,
        "fused_mean_future_return": float(np.mean(fused_returns)) if fused_returns else 0.0,
        "top_k_uplift": (float(np.mean(fused_returns)) - float(np.mean(base_returns))) if base_returns and fused_returns else 0.0,
        "base_negative_count": int(base_negative),
        "fused_negative_count": int(fused_negative),
        "bad_pick_removal": int(base_negative - fused_negative),
        "changed_top10_count": int(len(set(base_codes) ^ set(fused_codes))),
        "base_top_codes": base_codes,
        "fused_top_codes": fused_codes,
    }


def build_compare_readout(*, base_rows: list[dict[str, Any]], fused_rows: list[dict[str, Any]], k: int) -> dict[str, Any]:
    base_top = rank_rows(base_rows, score_key="base_score")[:k]
    fused_top = rank_rows(fused_rows, score_key="fused_score")[:k]
    base_by_code = {str(row.get("code") or ""): row for row in base_top}
    fused_by_code = {str(row.get("code") or ""): row for row in fused_top}
    base_codes = [str(row.get("code") or "") for row in base_top]
    fused_codes = [str(row.get("code") or "") for row in fused_top]
    shared_codes = [code for code in base_codes if code in fused_by_code]
    dropped_codes = [code for code in base_codes if code not in fused_by_code]
    added_codes = [code for code in fused_codes if code not in base_by_code]

    uplift_contributors: list[dict[str, Any]] = []
    for code in shared_codes:
        base_rank = int(base_by_code[code]["base_rank"] or 0)
        fused_rank = int(fused_by_code[code]["fused_rank"] or 0)
        if base_rank > fused_rank:
            uplift_contributors.append(
                {
                    "code": code,
                    "base_rank": base_rank,
                    "fused_rank": fused_rank,
                    "rank_delta": base_rank - fused_rank,
                    "future_return": float(fused_by_code[code].get("future_return") or 0.0),
                    "label_bucket": str(fused_by_code[code].get("label_bucket") or "neutral"),
                }
            )

    bad_pick_removal_contributors = [
        {
            "code": code,
            "base_rank": int(base_by_code[code]["base_rank"] or 0),
            "future_return": float(base_by_code[code].get("future_return") or 0.0),
            "label_bucket": str(base_by_code[code].get("label_bucket") or "neutral"),
        }
        for code in dropped_codes
        if str(base_by_code[code].get("label_bucket") or "") == "negative"
    ]
    false_veto_codes = [
        code
        for code in dropped_codes
        if str(base_by_code[code].get("label_bucket") or "") in {"positive", "neutral"}
    ]
    winner_drop_codes: list[str] = []
    if base_top:
        winner = max(
            base_top,
            key=lambda row: (
                float(row.get("future_return") or 0.0),
                -int(row.get("base_rank") or 0),
                str(row.get("code") or ""),
            ),
        )
        winner_code = str(winner.get("code") or "")
        if winner_code not in fused_by_code:
            winner_drop_codes.append(winner_code)

    return {
        "shared_codes": shared_codes,
        "dropped_codes": dropped_codes,
        "added_codes": added_codes,
        "uplift_contributors": uplift_contributors,
        "bad_pick_removal_contributors": bad_pick_removal_contributors,
        "false_veto_codes": false_veto_codes,
        "false_veto_count": len(false_veto_codes),
        "winner_drop_codes": winner_drop_codes,
        "winner_drop_count": len(winner_drop_codes),
    }


def compute_binary_metrics(*, labels: list[int], scores: list[float], threshold: float = 0.5) -> dict[str, Any]:
    if not labels:
        return {"sample_count": 0, "auc": None, "mcc": None, "accuracy": None}
    y_true = np.asarray(labels, dtype=np.int32)
    y_score = np.asarray(scores, dtype=np.float32)
    payload: dict[str, Any] = {"sample_count": int(len(labels))}
    try:
        order = np.argsort(y_score)
        ranks = np.empty_like(order, dtype=np.float64)
        ranks[order] = np.arange(1, len(y_score) + 1, dtype=np.float64)
        pos = y_true == 1
        neg = y_true == 0
        pos_count = int(pos.sum())
        neg_count = int(neg.sum())
        if pos_count == 0 or neg_count == 0:
            raise ValueError("auc requires both classes")
        auc = (ranks[pos].sum() - (pos_count * (pos_count + 1) / 2.0)) / (pos_count * neg_count)
        payload["auc"] = float(auc)
    except Exception:
        payload["auc"] = None
    try:
        y_pred = (y_score >= threshold).astype(np.int32)
        tp = int(np.sum((y_true == 1) & (y_pred == 1)))
        tn = int(np.sum((y_true == 0) & (y_pred == 0)))
        fp = int(np.sum((y_true == 0) & (y_pred == 1)))
        fn = int(np.sum((y_true == 1) & (y_pred == 0)))
        denom = float((tp + fp) * (tp + fn) * (tn + fp) * (tn + fn))
        payload["mcc"] = float(((tp * tn) - (fp * fn)) / np.sqrt(denom)) if denom > 0.0 else None
    except Exception:
        payload["mcc"] = None
    try:
        payload["accuracy"] = float(np.mean((y_score >= threshold).astype(np.int32) == y_true))
    except Exception:
        payload["accuracy"] = None
    return payload
