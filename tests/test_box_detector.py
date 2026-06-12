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


def test_detect_boxes_rejects_year_plus_swing_as_single_box():
    rows = [
        (202407, 985, 1040, 900, 1025),
        (202408, 1025, 1048, 1008, 1010),
        (202409, 1010, 1050, 988, 1045),
        (202410, 1045, 1055, 1008, 1020),
        (202411, 1042, 1060, 1030, 1045),
        (202412, 1048, 1080, 1008, 1078),
        (202501, 1090, 1165, 1070, 1155),
        (202502, 1172, 1240, 1148, 1188),
        (202503, 1125, 1220, 1118, 1200),
        (202504, 1160, 1202, 1008, 1126),
        (202505, 1090, 1160, 1078, 1160),
        (202506, 1100, 1110, 1090, 1095),
        (202507, 1130, 1140, 1086, 1095),
        (202508, 1128, 1150, 1070, 1072),
        (202509, 1076, 1100, 1058, 1075),
        (202510, 1062, 1070, 1022, 1030),
        (202511, 1050, 1062, 1015, 1030),
        (202512, 1052, 1058, 1038, 1050),
        (202601, 1068, 1085, 1040, 1060),
        (202602, 1082, 1110, 1062, 1070),
        (202603, 1070, 1104, 1040, 1098),
        (202604, 1070, 1072, 1040, 1050),
        (202605, 1052, 1088, 1048, 1056),
        (202606, 1070, 1072, 1040, 1050),
    ]

    boxes = detect_boxes(rows, range_basis="body", max_range_pct=0.2)

    assert all(box["endIndex"] - box["startIndex"] + 1 <= 12 for box in boxes)
    assert not any(box["startTime"] <= 202410 and box["endTime"] >= 202606 for box in boxes)
