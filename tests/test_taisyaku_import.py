from __future__ import annotations

from pathlib import Path

from app.backend.services.data import taisyaku_import


def test_parse_balance_rows_computes_loan_ratio() -> None:
    rows = [
        [
            "申込日",
            "決済日",
            "銘柄コード",
            "銘柄名",
            "取引所区分名",
            "上場区分",
            "速報／確報",
            "融資新規株数",
            "融資返済株数",
            "融資残高株数",
            "貸株新規株数",
            "貸株返済株数",
            "貸株残高株数",
            "差引残高株数",
        ],
        ["2026/03/11", "2026/03/13", "1306", "NF TOPIX", "東証", "", "確報", "0", "29960", "14410", "31750", "390", "48030", "-33620"],
    ]

    parsed = taisyaku_import.parse_balance_rows(rows)

    assert len(parsed) == 1
    assert parsed[0][0] == 20260311
    assert parsed[0][2] == "1306"
    assert parsed[0][8] == 14410
    assert parsed[0][11] == 48030
    assert round(parsed[0][13], 4) == round(14410 / 48030, 4)


def test_import_taisyaku_csvs_and_load_snapshot(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "taisyaku-test.duckdb"
    csv_map = {
        taisyaku_import.TAISYAKU_MASTER_URL: [
            ["貸借取引対象銘柄一覧", "20260311"],
            ["貸借申込日", "コード", "銘柄名", "貸借銘柄区分（東証）", "－", "貸借銘柄区分（ＪＮＸ）", "貸借銘柄区分（ＯＤＸ）", "貸借銘柄区分（ＪＡＸ）", "貸借銘柄区分（名証）", "貸借銘柄区分（福証）", "貸借銘柄区分（札証）"],
            ["20260312", "1306", "NEXT TOPIX ETF", "1", "0", "1", "1", "1", "0", "0", "0"],
        ],
        taisyaku_import.TAISYAKU_BALANCE_URL: [
            ["申込日", "決済日", "銘柄コード", "銘柄名", "取引所区分名", "上場区分", "速報／確報", "融資新規株数", "融資返済株数", "融資残高株数", "貸株新規株数", "貸株返済株数", "貸株残高株数", "差引残高株数"],
            ["2026/03/11", "2026/03/13", "1306", "NEXT TOPIX ETF", "東証およびＰＴＳ", "", "確報", "0", "29960", "14410", "31750", "390", "48030", "-33620"],
            ["2026/03/10", "2026/03/12", "1306", "NEXT TOPIX ETF", "東証およびＰＴＳ", "", "確報", "0", "25000", "15000", "30000", "100", "40000", "-25000"],
        ],
        taisyaku_import.TAISYAKU_FEE_URL: [
            ["品貸料率一覧"],
            ["注記"],
            ["抽出条件", "全体"],
            ["貸借申込日", "決済日", "コード", "銘柄名", "取引所区分", "決算事由", "決算等", "貸借値段（円）", "貸株超過株数", "最高料率（円）", "当日品貸料率（円）", "当日品貸日数", "前日品貸料率（円）"],
            ["20260311", "20260313", "1306", "NEXT TOPIX ETF", "東証", "臨時", "20260331", "3883.00", "48030", "9.00", "0.00", "3", "0.00"],
        ],
        taisyaku_import.TAISYAKU_RESTRICTION_URL: [
            ["貸借取引銘柄別制限措置等一覧"],
            ["注記1"],
            ["注記2"],
            ["注記3"],
            ["直近発表", "銘柄コード", "銘柄名", "実施措置", "実施内容", "通知日・実施日", "後場停止"],
            ["", "1306", "NEXT TOPIX ETF", "注意喚起", "新規売り", "2026/03/12", ""],
        ],
    }

    def fake_download(url: str) -> list[list[str]]:
        return csv_map[url]

    monkeypatch.setattr(taisyaku_import, "_download_csv_rows", fake_download)

    result = taisyaku_import.import_taisyaku_csvs(db_path=db_path)
    snapshot = taisyaku_import.load_taisyaku_snapshot("1306", db_path=db_path, history_limit=5)

    assert result["balanceSaved"] == 2
    assert result["feeSaved"] == 1
    assert result["restrictionSaved"] == 1
    assert snapshot is not None
    assert snapshot["latestBalance"]["applicationDate"] == 20260311
    assert round(snapshot["latestBalance"]["loanRatio"], 4) == round(14410 / 48030, 4)
    assert len(snapshot["balanceHistory"]) == 2
    assert snapshot["latestFee"]["currentFeeYen"] == 0.0
    assert snapshot["restrictions"][0]["measureType"] == "注意喚起"

