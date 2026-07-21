import json

from app.backend.services.tradex_research_tags import build_short_research_tags_by_code


def test_build_short_research_tags_by_code_uses_current_triggered_and_waiting_rows(tmp_path):
    report = {
        "meemee_display_contract": {
            "meemee_reflectable": True,
            "display_status": "research_match_not_trade_signal",
            "display_label_ja": "上昇成熟・上ヒゲ三陰線ショート研究一致",
            "latest_current_scan": {
                "triggered_previous_signal_rows": [{"code": "9147"}],
                "waiting_latest_signal_rows": [{"code": "4272"}],
                "recent_60_calendar_day_rows": [{"code": "7327"}],
            },
        }
    }
    path = tmp_path / "report.json"
    path.write_text(json.dumps(report, ensure_ascii=False), encoding="utf-8")

    tags = build_short_research_tags_by_code(report_path=path)

    assert tags["9147"] == ["上昇成熟・上ヒゲ三陰線ショート研究一致:発動"]
    assert tags["4272"] == ["上昇成熟・上ヒゲ三陰線ショート研究一致:待ち"]
    assert "7327" not in tags


def test_build_short_research_tags_by_code_ignores_non_reflectable_report(tmp_path):
    path = tmp_path / "report.json"
    path.write_text(
        json.dumps(
            {
                "meemee_display_contract": {
                    "meemee_reflectable": False,
                    "display_status": "research_match_not_trade_signal",
                    "display_label_ja": "tag",
                    "latest_current_scan": {"triggered_previous_signal_rows": [{"code": "9147"}]},
                }
            }
        ),
        encoding="utf-8",
    )

    assert build_short_research_tags_by_code(report_path=path) == {}
