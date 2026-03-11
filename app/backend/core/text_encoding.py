from __future__ import annotations


def japanese_char_count(text: str) -> int:
    count = 0
    for ch in text:
        code = ord(ch)
        if (
            0x3040 <= code <= 0x30FF
            or 0x4E00 <= code <= 0x9FFF
            or 0x3400 <= code <= 0x4DBF
            or 0xF900 <= code <= 0xFAFF
        ):
            count += 1
    return count


def repair_cp932_mojibake(text: str) -> str:
    """
    Recover strings that were originally CP932 bytes but decoded as cp1252-like text.

    Safety rule:
    - Keep the original unless the repaired candidate increases Japanese character count.
    """
    source = str(text or "")
    if not source:
        return source

    raw = bytearray()
    for ch in source:
        try:
            raw.extend(ch.encode("cp1252"))
            continue
        except UnicodeEncodeError:
            pass

        code = ord(ch)
        if code <= 0xFF:
            raw.append(code)
            continue

        return source

    try:
        repaired = bytes(raw).decode("cp932")
    except UnicodeDecodeError:
        return source

    if repaired == source:
        return source
    if japanese_char_count(repaired) <= japanese_char_count(source):
        return source
    return repaired
