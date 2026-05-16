from pathlib import Path


def test_pan_export_does_not_open_output_file_for_zero_row_append() -> None:
    script = Path("tools/export_pan.vbs").read_text(encoding="utf-8")

    no_op_guard = "If canAppend And startPos > endPos Then"
    open_output = "Set tsOut = fso.OpenTextFile(outPath, ForAppending, True)"

    assert no_op_guard in script
    assert open_output in script
    assert script.index(no_op_guard) < script.index(open_output)
    assert "Exit Function" in script[script.index(no_op_guard) : script.index(open_output)]
