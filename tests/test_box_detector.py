from app.services.box_detector import detect_boxes


def test_detect_boxes_body_basis_handles_wicks():
    rows = [
        (202401, 100, 130, 70, 102),
        (202402, 101, 128, 72, 99),
        (202403, 99, 125, 75, 103),
        (202404, 102, 140, 68, 101),
        (202405, 100, 135, 70, 104),
    ]

    boxes_high_low = detect_boxes(rows, range_basis="high_low", max_range_pct=0.2)
    assert boxes_high_low == []

    boxes_body = detect_boxes(
        rows,
        range_basis="body",
        max_range_pct=0.2,
        min_range_pct=0.0,
        min_edge_touches_per_side=1,
    )
    assert len(boxes_body) == 1

    box = boxes_body[0]
    assert box["startTime"] == 202401
    assert box["endTime"] == 202405
    assert box["lower"] == 99
    assert box["upper"] == 104


def test_detect_boxes_keeps_retested_monthly_range():
    rows = [
        (1, 100, 126, 92, 101),
        (2, 102, 130, 95, 116),
        (3, 117, 128, 105, 118),
        (4, 110, 122, 99, 112),
        (5, 101, 114, 94, 104),
        (6, 103, 124, 97, 116),
        (7, 119, 132, 116, 124),
    ]

    boxes = detect_boxes(rows, range_basis="body", max_range_pct=0.2)

    assert len(boxes) == 1
    assert boxes[0]["startTime"] == 1
    assert boxes[0]["endTime"] == 6
    assert boxes[0]["lower"] == 100
    assert boxes[0]["upper"] == 118
    assert boxes[0]["breakout"] == "up"


def test_detect_boxes_rejects_narrow_drift_not_box():
    rows = [
        (1, 100, 108, 98, 103),
        (2, 102, 109, 99, 104),
        (3, 101, 108, 98, 105),
        (4, 103, 110, 100, 106),
        (5, 104, 111, 101, 107),
        (6, 105, 112, 102, 108),
    ]

    assert detect_boxes(rows, range_basis="body", max_range_pct=0.2) == []


def test_detect_boxes_rejects_single_bar_supported_range():
    rows = [
        (1, 105, 119, 103, 118),
        (2, 100, 107, 99, 106),
        (3, 99, 122, 98, 118),
        (4, 106, 112, 104, 112),
        (5, 110, 119, 108, 117),
    ]

    assert detect_boxes(rows, range_basis="body", max_range_pct=0.2) == []


def test_detect_boxes_rejects_sloped_midline_not_box():
    rows = [
        (1, 115, 123, 111, 120),
        (2, 113, 121, 110, 119),
        (3, 105, 114, 102, 111),
        (4, 99, 108, 96, 105),
        (5, 95, 104, 91, 101),
    ]

    assert detect_boxes(rows, range_basis="body", max_range_pct=0.3) == []
