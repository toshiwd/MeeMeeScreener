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

    boxes_body = detect_boxes(rows, range_basis="body", max_range_pct=0.2)
    assert len(boxes_body) == 1

    box = boxes_body[0]
    assert box["startTime"] == 202401
    assert box["endTime"] == 202405
    assert box["lower"] == 99
    assert box["upper"] == 104
