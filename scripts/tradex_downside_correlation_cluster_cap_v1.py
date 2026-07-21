"""Audit PIT 60-session correlation cluster caps for staged short exposure."""
import argparse, hashlib, json
from pathlib import Path
import duckdb
import numpy as np
import pandas as pd


def sha(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def health(frame):
    history, flags = [], []
    for _, group in frame.groupby("ymd", sort=True):
        prior = np.asarray(history[-5:], dtype=float)
        flags.extend([bool(len(prior) >= 5 and prior.mean() > 0)] * len(group))
        history.extend(group.return_fixed3_pct.tolist())
    return pd.Series(flags, index=frame.index)


def components(nodes, edges):
    parent = {node: node for node in nodes}
    def find(node):
        while parent[node] != node:
            parent[node] = parent[parent[node]]; node = parent[node]
        return node
    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb: parent[rb] = ra
    for a, b in edges: union(a, b)
    groups = {}
    for node in nodes: groups.setdefault(find(node), []).append(node)
    return list(groups.values())


def allocate(frame, returns, threshold, cluster_cap=.50):
    allocation = pd.Series(0.0, index=frame.index); active = []; cache = {}
    def corr(a, b, day):
        if frame.loc[a, "code"] == frame.loc[b, "code"]: return 1.0
        key = tuple(sorted((frame.loc[a, "code"], frame.loc[b, "code"]))) + (int(day),)
        if key not in cache:
            q = returns.loc[returns.index < day, list(key[:2])].dropna().tail(60)
            cache[key] = float(q.iloc[:, 0].corr(q.iloc[:, 1])) if len(q) >= 40 else None
        return cache[key]
    for day, new_group in frame.loc[frame.desired_weight > 0].groupby("entry_ymd", sort=True):
        active = [idx for idx in active if frame.loc[idx, "exit_ymd"] >= day and allocation.loc[idx] > 0]
        new_nodes = list(new_group.index); nodes = active + new_nodes; edges = []
        for pos, a in enumerate(nodes):
            for b in nodes[pos + 1:]:
                value = corr(a, b, int(day))
                if value is not None and value >= threshold: edges.append((a, b))
        for group in components(nodes, edges):
            new = [idx for idx in group if idx in new_nodes]
            if not new: continue
            active_used = float(allocation.loc[[idx for idx in group if idx in active]].sum())
            wanted = float(frame.loc[new, "desired_weight"].sum())
            scale = min(1.0, max(0.0, cluster_cap - active_used) / wanted) if wanted else 0.0
            allocation.loc[new] = frame.loc[new, "desired_weight"] * scale
        active += new_nodes
    return allocation


def metrics(frame, allocation):
    x = frame.loc[allocation > 0].copy(); x["allocation"] = allocation.loc[allocation > 0]
    x["weighted_return"] = x.return_fixed3_pct * x.allocation
    r = x.weighted_return; loss = -r[r < 0].sum(); daily = x.groupby("exit_ymd").weighted_return.sum()
    years = {str(key): {"n": int(len(group)), "weighted_total": float(group.weighted_return.sum())} for key, group in x.groupby("year")}
    halves = {str(key): {"n": int(len(group)), "weighted_total": float(group.weighted_return.sum())} for key, group in x.groupby("half")}
    return {"n": int(len(x)), "weighted_total": float(r.sum()), "weighted_mean": float(r.mean()),
            "profit_factor": float(r[r > 0].sum() / loss), "max_trade_loss": float(r.min()),
            "worst_exit_day_loss": float(daily.min()), "worst_exit_day": int(daily.idxmin()),
            "positive_years": sum(row["weighted_total"] > 0 for row in years.values()),
            "positive_halves": sum(row["weighted_total"] > 0 for row in halves.values()),
            "years": years, "halves": halves,
            "fully_allocated": int(((allocation == frame.desired_weight) & (frame.desired_weight > 0)).sum()),
            "partially_allocated": int(((allocation > 0) & (allocation < frame.desired_weight)).sum()),
            "zeroed_by_cap": int(((allocation == 0) & (frame.desired_weight > 0)).sum())}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--natural", type=Path, required=True); ap.add_argument("--daily", type=Path, required=True)
    ap.add_argument("--db", type=Path, required=True); ap.add_argument("--parent-compare", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True); a = ap.parse_args(); a.output.mkdir(parents=True, exist_ok=False)
    x = pd.read_parquet(a.natural)
    f = pd.read_parquet(a.daily, columns=["code", "ymd", "market_breadth_ma60"]); f.code = f.code.astype(str).str.zfill(4)
    x = x.merge(f, on=["code", "ymd"], validate="one_to_one")
    x = x.loc[x.base_regime.eq("BOX") & (x.close_pos <= .20)].sort_values(["ymd", "code"]).reset_index(drop=True)
    x["health"] = health(x); x["desired_weight"] = np.where(x.market_breadth_ma60 <= .40, 0, np.where(x.health, .50, .25))
    con = duckdb.connect(str(a.db), read_only=True)
    prices = con.execute("select code,strftime(to_timestamp(date),'%Y%m%d')::integer ymd,c from daily_bars where code in (select unnest(?)) order by code,date",
                         [x.code.unique().tolist()]).fetchdf(); con.close()
    prices.code = prices.code.astype(str).str.zfill(4)
    returns = prices.pivot(index="ymd", columns="code", values="c").pct_change(fill_method=None)
    base = metrics(x, x.desired_weight)
    variants = {"uncapped": base}; checks = {}
    for threshold in (.60, .70, .80):
        name = f"corr_ge_{threshold:.2f}_cluster_cap_0.50"
        row = metrics(x, allocate(x, returns, threshold)); variants[name] = row
        checks[name] = {"worst_day_improved": row["worst_exit_day_loss"] > base["worst_exit_day_loss"],
                        "weighted_total_retained_ge_90pct": row["weighted_total"] >= .90 * base["weighted_total"],
                        "pf_not_lower": row["profit_factor"] >= base["profit_factor"]}
    kept = [name for name, row in checks.items() if all(row.values())]
    result = {"schema_version": "tradex_downside_correlation_cluster_cap_v1.compare.v1",
              "artifact_role": "authoritative_downside_correlation_cluster_cap", "review_only": True, "research_phase": "effectiveness_judgment",
              "fixed_conditions": {"selector_and_sizing": "fixed staged downside selector", "axis_changed": "PIT 60-session close-return correlation threshold only",
                                   "thresholds": [.60, .70, .80], "cluster_cap": .50, "minimum_overlap": 40,
                                   "same_day_contract": "connected components; new exposure allocated pro-rata", "costs": "ignored", "weekly_inputs": []},
              "authoritative_result": {"variants": variants, "gate_checks": checks, "kept_variants": kept},
              "observed_branching": {"changed_top5_members_count": None, "changed_top10_members_count": None, "changed_rank_count": None,
                                     "selection_divergence_reason": "high-correlation active and new positions share a 0.50 cluster cap"},
              "judgment": {"candidate_local_decision": "keep" if kept else "drop", "session_aggregate_decision": "keep_correlation_cap" if kept else "drop_correlation_cap",
                           "authoritative_rollup_decision": kept[0] if kept else "drop_correlation_cluster_axis",
                           "reason_type": "worst_day_improved_with_return_and_pf_preserved" if kept else "fixed_risk_return_gate_failed"},
              "not_changed": ["selector", "individual desired weights", "MeeMee", "runtime DB", "production logic"]}
    cp = a.output / "compare.json"; cp.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {"sources": {"natural": {"path": str(a.natural.resolve()), "sha256": sha(a.natural)}, "daily": {"path": str(a.daily.resolve()), "sha256": sha(a.daily)},
                         "db": {"path": str(a.db.resolve()), "read_only": True}, "parent_compare": {"path": str(a.parent_compare.resolve()), "sha256": sha(a.parent_compare)}},
             "candidate_events": int(len(x)), "future_columns_used": [], "weekly_columns_used": [], "same_day_pro_rata": True, "compare_sha256": sha(cp)}
    (a.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (a.output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(cp)}, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(a.output), "result": result["authoritative_result"], "judgment": result["judgment"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__": main()
