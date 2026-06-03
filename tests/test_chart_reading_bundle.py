from __future__ import annotations

import duckdb
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.backend.api.routers import chart_reading
from app.backend.infra.duckdb.memo_repo import MemoRepository
from app.backend.services import chart_reading_bundle
from app.backend.services.chart_reading_bundle import get_chart_reading_bundle
from app.db.schema import ensure_schema


def test_seeded_daily_memos_migrate_to_chart_note_v1() -> None:
    conn = duckdb.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE daily_memos (
            symbol TEXT,
            date TEXT,
            timeframe TEXT,
            memo TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (symbol, date, timeframe)
        )
        """
    )
    conn.execute(
        """
        INSERT INTO daily_memos (symbol, date, timeframe, memo, updated_at)
        VALUES ('1001', '2026-03-31', 'daily', '20下20で前のボックスの下限で下げ止まった', TIMESTAMP '2026-05-26 15:41:49')
        """
    )

    ensure_schema(conn)

    row = conn.execute(
        """
        SELECT code, as_of_date, timeframe, note_text
        FROM chart_notes
        WHERE code = '1001' AND as_of_date = '2026-03-31' AND timeframe = 'daily'
        """
    ).fetchone()
    assert row == ("1001", "2026-03-31", "daily", "20下20で前のボックスの下限で下げ止まった")

    memo = MemoRepository.get_memo(conn, "1001", "2026-03-31", "daily")
    assert memo is not None
    assert memo["memo"] == "20下20で前のボックスの下限で下げ止まった"
    assert memo["note_text"] == "20下20で前のボックスの下限で下げ止まった"


def test_chart_note_v1_keeps_legacy_memo_compatibility() -> None:
    conn = duckdb.connect(":memory:")
    ensure_schema(conn)

    result = MemoRepository.upsert_memo(
        conn,
        "1001",
        "2026-03-31",
        "D",
        "20下20で前のボックスの下限で下げ止まった",
        tags=["support", "box"],
        linked_objects=[{"type": "bar", "date": "2026-03-31"}],
    )
    assert result["schema_version"] == "chart_note_v1"

    memo = MemoRepository.get_memo(conn, "1001", "2026-03-31", "D")
    assert memo is not None
    assert memo["memo"] == "20下20で前のボックスの下限で下げ止まった"
    assert memo["note_text"] == memo["memo"]
    assert memo["tags"] == ["support", "box"]
    assert memo["linked_objects"] == [{"type": "bar", "date": "2026-03-31"}]
    assert memo["no_lookahead"] is True


def test_chart_reading_bundle_returns_notes_annotations_and_position_state() -> None:
    conn = duckdb.connect(":memory:")
    ensure_schema(conn)
    conn.execute(
        """
        INSERT INTO daily_bars (code, date, o, h, l, c, v)
        VALUES
            ('1001', 20260327, 90, 105, 88, 100, 1000),
            ('1001', 20260330, 100, 110, 99, 108, 1200),
            ('1001', 20260331, 108, 112, 101, 106, 1400)
        """
    )
    conn.execute(
        """
        INSERT INTO monthly_bars (code, month, o, h, l, c, v)
        VALUES ('1001', 202603, 80, 112, 75, 106, 9000)
        """
    )
    conn.execute(
        """
        INSERT INTO positions_live (
            symbol, spot_qty, margin_long_qty, margin_short_qty, buy_qty, sell_qty, has_issue, issue_note
        )
        VALUES ('1001', 100, 0, 0, 1, 0, FALSE, NULL)
        """
    )
    MemoRepository.upsert_memo(
        conn,
        "1001",
        "2026-03-31",
        "D",
        "support note",
        tags=["support"],
        linked_objects=[{"type": "region", "id": "box-1"}],
    )
    conn.execute(
        """
        INSERT INTO chart_annotations (
            id, code, as_of_date, timeframe, object_type, payload_json, tags_json
        )
        VALUES (
            'ann-1',
            '1001',
            '2026-03-31',
            'D',
            'region',
            '{"low":101,"high":112}',
            '["box"]'
        )
        """
    )

    bundle = get_chart_reading_bundle(conn, code="1001", as_of_date="2026-03-31")

    assert bundle["schema_version"] == "chart_reading_bundle_v1"
    assert bundle["research_logic_included"] is False
    assert bundle["selected_bar"]["date"] == "2026-03-31"
    assert bundle["chart_context"]["weekly"]["source"] == "aggregated_from_daily_bars"
    assert isinstance(bundle["notes"], list)
    assert isinstance(bundle["annotations"], list)
    assert isinstance(bundle["date_notes"], list)
    assert isinstance(bundle["visible_annotations"], list)
    assert set(bundle["timeframe_notes"]) == {"daily", "weekly", "monthly", "environment"}
    assert isinstance(bundle["environment_notes"], list)
    assert isinstance(bundle["drawing_objects"], list)
    assert isinstance(bundle["linked_notes"], list)
    assert bundle["notes"] == bundle["date_notes"]
    assert bundle["annotations"] == bundle["visible_annotations"]
    assert bundle["notes"][0]["note_text"] == "support note"
    assert bundle["annotations"][0]["object_type"] == "region"
    assert bundle["linked_tags"] == ["box", "support"]
    assert bundle["position_state"]["long_lots"] == 1
    assert bundle["ma_role_review"]["schema_version"] == "ma_role_readonly_review_v1"
    assert bundle["ma_role_review"]["read_only"] is True
    assert bundle["ma_role_review"]["ranking_effect"] is False
    assert bundle["ma_role_review"]["automatic_trade_action"] is False


def test_chart_reading_bundle_api_exposes_ma_role_review_read_only_payload(monkeypatch) -> None:
    conn = duckdb.connect(":memory:")
    ensure_schema(conn)
    conn.execute(
        """
        INSERT INTO daily_bars (code, date, o, h, l, c, v)
        VALUES ('1001', 20260331, 108, 112, 101, 106, 1400)
        """
    )

    class ConnContext:
        def __enter__(self):
            return conn

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(chart_reading, "try_get_conn", lambda timeout_sec=0.4: ConnContext())
    app = FastAPI()
    app.include_router(chart_reading.router)
    client = TestClient(app)

    response = client.get("/api/chart-reading/bundle", params={"code": "1001", "as_of_date": "2026-03-31"})

    assert response.status_code == 200
    payload = response.json()["ma_role_review"]
    assert payload["schema_version"] == "ma_role_readonly_review_v1"
    assert payload["read_only"] is True
    assert payload["ranking_effect"] is False
    assert payload["automatic_trade_action"] is False


def test_chart_reading_bundle_labels_yahoo_overlay_as_provisional_visual_evaluation() -> None:
    conn = duckdb.connect(":memory:")
    ensure_schema(conn)
    conn.execute(
        """
        INSERT INTO daily_bars (code, date, o, h, l, c, v, source)
        VALUES
            ('1001', 20260529, 100, 110, 99, 108, 1200, 'pan'),
            ('1001', 20260601, 108, 112, 101, 106, 1400, 'yahoo')
        """
    )

    bundle = get_chart_reading_bundle(conn, code="1001", as_of_date="2026-06-01")

    assert bundle["selected_bar"]["date"] == "2026-06-01"
    assert bundle["selected_bar"]["source"] == "yahoo"
    assert bundle["chart_context"]["daily"]["recent_bars"][-1]["source"] == "yahoo"
    assert bundle["visual_evaluation"] == {
        "classification": "mixed",
        "display_basis": "confirmed_plus_yahoo_provisional",
        "display_evaluation_available": True,
        "confirmed_judgment_available": True,
        "provisional_visual_evaluation_available": True,
        "requested_date": "2026-06-01",
        "display_last_date": "2026-06-01",
        "confirmed_last_date": "2026-05-29",
        "yahoo_provisional_last_date": "2026-06-01",
        "confirmed_judgment_basis": "non_yahoo_daily_bars_only",
        "provisional_visual_evaluation_basis": "daily_bars_including_yahoo_overlay",
        "warnings": ["Yahoo overlay is provisional display data and must not be presented as confirmed judgment."],
    }


def test_chart_reading_bundle_can_fetch_read_only_yahoo_visual_overlay(monkeypatch) -> None:
    conn = duckdb.connect(":memory:")
    ensure_schema(conn)
    conn.execute(
        """
        INSERT INTO daily_bars (code, date, o, h, l, c, v, source)
        VALUES ('1001', 20260529, 100, 110, 99, 108, 1200, 'pan')
        """
    )
    monkeypatch.setattr(
        chart_reading_bundle,
        "get_provisional_daily_row_from_chart",
        lambda code: (20260601, 108.0, 112.0, 101.0, 106.0, 1400.0),
    )

    bundle = get_chart_reading_bundle(
        conn,
        code="1001",
        as_of_date="2026-06-01",
        include_provisional_visual=True,
    )

    assert bundle["selected_bar"]["date"] == "2026-06-01"
    assert bundle["selected_bar"]["source"] == "yahoo"
    assert bundle["visual_evaluation"]["classification"] == "mixed"
    assert conn.execute("SELECT COUNT(*) FROM daily_bars").fetchone()[0] == 1


def test_chart_reading_bundle_v1_1_additive_fields_for_notes_and_callouts() -> None:
    conn = duckdb.connect(":memory:")
    ensure_schema(conn)
    conn.execute(
        """
        INSERT INTO daily_bars (code, date, o, h, l, c, v)
        VALUES ('1001', 20260331, 108, 112, 101, 106, 1400)
        """
    )
    conn.execute(
        """
        INSERT INTO monthly_bars (code, month, o, h, l, c, v)
        VALUES ('1001', 202603, 80, 112, 75, 106, 9000)
        """
    )
    conn.execute(
        """
        INSERT INTO chart_notes (code, as_of_date, timeframe, note_text, tags_json)
        VALUES
            ('1001', '2026-03-31', 'weekly', 'weekly context', '["weekly"]'),
            ('1001', '2026-03-31', 'monthly', 'monthly context', '["monthly"]'),
            ('1001', '2026-03-31', 'environment', 'environment context', '["environment"]')
        """
    )
    conn.execute(
        """
        INSERT INTO chart_annotations (
            id, code, as_of_date, timeframe, object_type, payload_json, tags_json
        )
        VALUES
            (
                'callout-1',
                '1001',
                '2026-03-31',
                'daily',
                'callout',
                '{"timeframe":"daily","anchor_type":"indicator","anchor_target":"ma20","anchor_date":"2026-03-31","anchor_price":51060,"label_position":{"date":"2026-04-08","price":53500},"leader_line":true,"free_text":"callout note","tags":["ma20"],"comment_type":"chart_context","no_lookahead":true}',
                '["ma20"]'
            ),
            (
                'region-link-1',
                '1001',
                '2026-03-31',
                'daily',
                'region',
                '{"free_text":"box annotation","linked_object":{"object_type":"region","timeframe":"daily","resolution":"payload_fallback","payload":{"startTime":1,"endTime":2,"topPrice":120,"bottomPrice":100}}}',
                '["box"]'
            ),
            (
                'env-ann-1',
                '1001',
                '2026-03-31',
                'monthly',
                'chart_context',
                '{"timeframe":"monthly","context_type":"environment","market_phase":"range_breakout_watch","free_text":"environment annotation","tags":["watch"],"no_lookahead":true}',
                '["watch"]'
            )
        """
    )

    bundle = get_chart_reading_bundle(conn, code="1001", as_of_date="2026-03-31")

    assert bundle["schema_version"] == "chart_reading_bundle_v1"
    assert bundle["timeframe_notes"]["weekly"][0]["note_text"] == "weekly context"
    assert bundle["timeframe_notes"]["monthly"][0]["note_text"] == "monthly context"
    assert bundle["timeframe_notes"]["environment"][0]["note_text"] == "environment context"
    assert any(item.get("object_type") == "chart_context" for item in bundle["environment_notes"])
    assert any(item.get("object_type") == "callout" for item in bundle["annotations"])
    assert bundle["drawing_objects"][0]["resolution"] == "payload_fallback"
    assert bundle["notes"] == bundle["date_notes"]
    assert bundle["annotations"] == bundle["visible_annotations"]


def test_chart_annotation_crud_api_and_bundle(monkeypatch) -> None:
    conn = duckdb.connect(":memory:")
    ensure_schema(conn)
    conn.execute(
        """
        INSERT INTO daily_bars (code, date, o, h, l, c, v)
        VALUES ('1001', 20260331, 108, 112, 101, 106, 1400)
        """
    )
    conn.execute(
        """
        INSERT INTO monthly_bars (code, month, o, h, l, c, v)
        VALUES ('1001', 202603, 80, 112, 75, 106, 9000)
        """
    )

    class ConnContext:
        def __enter__(self):
            return conn

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(chart_reading, "try_get_conn", lambda timeout_sec=0.4: ConnContext())
    app = FastAPI()
    app.include_router(chart_reading.router)
    client = TestClient(app)

    create_response = client.post(
        "/api/chart-annotations",
        json={
            "code": "1001",
            "as_of_date": "2026-03-31",
            "timeframe": "D",
            "object_type": "bar",
            "payload": {
                "bar_date": "2026-03-31",
                "bar_role": "support_test",
                "action_label": "watch",
                "reason_tags": ["support"],
                "free_text": "bar note",
                "importance": 3,
            },
            "tags": ["support"],
        },
    )
    assert create_response.status_code == 200
    annotation_id = create_response.json()["id"]

    list_response = client.get("/api/chart-annotations", params={"code": "1001", "as_of_date": "2026-03-31"})
    assert list_response.status_code == 200
    assert list_response.json()["items"][0]["id"] == annotation_id

    update_response = client.put(
        f"/api/chart-annotations/{annotation_id}",
        json={
            "code": "1001",
            "as_of_date": "2026-03-31",
            "timeframe": "D",
            "object_type": "bar",
            "payload": {
                "bar_date": "2026-03-31",
                "bar_role": "support_test",
                "action_label": "watch",
                "reason_tags": ["support", "updated"],
                "free_text": "updated note",
                "importance": 4,
            },
            "tags": ["support", "updated"],
        },
    )
    assert update_response.status_code == 200
    bundle = get_chart_reading_bundle(conn, code="1001", as_of_date="2026-03-31")
    assert bundle["annotations"][0]["id"] == annotation_id
    assert bundle["annotations"][0]["payload"]["free_text"] == "updated note"

    delete_response = client.delete(f"/api/chart-annotations/{annotation_id}")
    assert delete_response.status_code == 200
    assert delete_response.json()["deleted"] is True
    assert get_chart_reading_bundle(conn, code="1001", as_of_date="2026-03-31")["annotations"] == []


def test_chart_annotation_api_validates_callout_payload(monkeypatch) -> None:
    conn = duckdb.connect(":memory:")
    ensure_schema(conn)

    class ConnContext:
        def __enter__(self):
            return conn

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(chart_reading, "try_get_conn", lambda timeout_sec=0.4: ConnContext())
    app = FastAPI()
    app.include_router(chart_reading.router)
    client = TestClient(app)

    invalid_response = client.post(
        "/api/chart-annotations",
        json={
            "code": "1001",
            "as_of_date": "2026-03-31",
            "timeframe": "daily",
            "object_type": "callout",
            "payload": {"anchor_type": "indicator", "anchor_price": 51060},
        },
    )
    assert invalid_response.status_code == 400
    assert invalid_response.json()["detail"] == "invalid_callout_anchor_target"

    valid_response = client.post(
        "/api/chart-annotations",
        json={
            "code": "1001",
            "as_of_date": "2026-03-31",
            "timeframe": "daily",
            "object_type": "callout",
            "payload": {
                "anchor_type": "indicator",
                "anchor_target": "ma20",
                "anchor_date": "2026-03-31",
                "anchor_price": 51060,
                "label_position": {"date": "2026-04-08", "price": 53500},
                "leader_line": True,
                "free_text": "validated callout",
            },
            "tags": ["callout"],
        },
    )
    assert valid_response.status_code == 200
    assert valid_response.json()["annotation"]["object_type"] == "callout"


def test_chart_note_api_saves_monthly_weekly_environment_notes(monkeypatch) -> None:
    conn = duckdb.connect(":memory:")
    ensure_schema(conn)
    conn.execute(
        """
        INSERT INTO daily_bars (code, date, o, h, l, c, v)
        VALUES ('1001', 20260331, 108, 112, 101, 106, 1400)
        """
    )
    conn.execute(
        """
        INSERT INTO monthly_bars (code, month, o, h, l, c, v)
        VALUES ('1001', 202603, 80, 112, 75, 106, 9000)
        """
    )

    class ConnContext:
        def __enter__(self):
            return conn

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(chart_reading, "try_get_conn", lambda timeout_sec=0.4: ConnContext())
    app = FastAPI()
    app.include_router(chart_reading.router)
    client = TestClient(app)

    for timeframe in ("weekly", "monthly", "environment"):
        response = client.put(
            "/api/chart-notes",
            json={
                "code": "1001",
                "as_of_date": "2026-03-31",
                "timeframe": timeframe,
                "note_text": f"{timeframe} note",
                "tags": [timeframe],
                "no_lookahead": True,
            },
        )
        assert response.status_code == 200
        assert response.json()["note"]["timeframe"] == timeframe

    bundle = get_chart_reading_bundle(conn, code="1001", as_of_date="2026-03-31")
    assert bundle["timeframe_notes"]["weekly"][0]["note_text"] == "weekly note"
    assert bundle["timeframe_notes"]["monthly"][0]["note_text"] == "monthly note"
    assert bundle["timeframe_notes"]["environment"][0]["note_text"] == "environment note"
    assert bundle["environment_notes"][0]["note_text"] == "environment note"


def test_chart_note_api_saves_paragraph_linked_objects(monkeypatch) -> None:
    conn = duckdb.connect(":memory:")
    ensure_schema(conn)
    conn.execute(
        """
        INSERT INTO daily_bars (code, date, o, h, l, c, v)
        VALUES ('1001', 20260331, 108, 112, 101, 106, 1400)
        """
    )
    conn.execute(
        """
        INSERT INTO monthly_bars (code, month, o, h, l, c, v)
        VALUES ('1001', 202603, 80, 112, 75, 106, 9000)
        """
    )
    for annotation_id, object_type in [
        ("bar-1", "bar"),
        ("region-1", "region"),
        ("line-1", "line"),
        ("callout-1", "callout"),
    ]:
        conn.execute(
            """
            INSERT INTO chart_annotations (id, code, as_of_date, timeframe, object_type, payload_json, tags_json)
            VALUES (?, '1001', '2026-03-31', 'daily', ?, '{}', '[]')
            """,
            [annotation_id, object_type],
        )

    class ConnContext:
        def __enter__(self):
            return conn

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(chart_reading, "try_get_conn", lambda timeout_sec=0.4: ConnContext())
    app = FastAPI()
    app.include_router(chart_reading.router)
    client = TestClient(app)

    response = client.put(
        "/api/chart-notes",
        json={
            "code": "1001",
            "as_of_date": "2026-03-31",
            "timeframe": "mixed",
            "title": "2026/03/31 review",
            "paragraphs": [
                {
                    "paragraph_id": "p1",
                    "order": 1,
                    "text": "20下20で前のボックス下限で下げ止まった",
                    "comment_type": "daily_bar_reading",
                    "linked_objects": [
                        {"object_type": "bar", "annotation_id": "bar-1"},
                        {"object_type": "region", "annotation_id": "region-1"},
                        {"object_type": "line", "annotation_id": "line-1"},
                        {"object_type": "callout", "annotation_id": "callout-1"},
                        {"object_type": "indicator", "anchor_type": "indicator", "anchor_target": "ma20"},
                    ],
                    "reason_tags": ["20下20", "box_lower"],
                    "no_lookahead": True,
                }
            ],
            "tags": ["chart_note"],
            "no_lookahead": True,
        },
    )
    assert response.status_code == 200
    assert response.json()["note"]["paragraphs"][0]["linked_objects"][4]["anchor_target"] == "ma20"

    bundle = get_chart_reading_bundle(conn, code="1001", as_of_date="2026-03-31")
    note = bundle["notes"][0]
    paragraph = note["paragraphs"][0]
    assert note["title"] == "2026/03/31 review"
    assert note["note_text"] == "20下20で前のボックス下限で下げ止まった"
    assert paragraph["comment_type"] == "daily_bar_reading"
    assert paragraph["reason_tags"] == ["20下20", "box_lower"]
    linked = paragraph["linked_objects"]
    assert {item.get("object_type") for item in linked} == {"bar", "region", "line", "callout", "indicator"}
    assert all(item.get("resolution") for item in linked)
    assert any(item.get("anchor_target") == "ma20" for item in linked)
    assert any(item.get("paragraph_id") == "p1" and item.get("object_type") == "indicator" for item in bundle["linked_notes"])
