from __future__ import annotations

import argparse
import json
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any


VALID_DECISIONS = {"SIDEWAYS", "NOT_SIDEWAYS", "BORDERLINE"}
VALID_CONFIDENCE = {"LOW", "MEDIUM", "HIGH"}


def validate_payload(payload: Any, *, expected_count: int = 120) -> dict[str, Any]:
    if not isinstance(payload, dict) or payload.get("schema_version") != "tradex_sideways_blind_annotation_v1":
        raise ValueError("invalid schema_version")
    annotations = payload.get("annotations")
    if not isinstance(annotations, list) or len(annotations) != expected_count:
        raise ValueError(f"expected {expected_count} annotations")
    case_ids: set[str] = set()
    for row in annotations:
        if not isinstance(row, dict):
            raise ValueError("annotation must be an object")
        case_id = str(row.get("case_id") or "")
        if not case_id or case_id in case_ids:
            raise ValueError("case_id must be present and unique")
        case_ids.add(case_id)
        if row.get("sideways_decision") not in VALID_DECISIONS:
            raise ValueError(f"invalid sideways_decision for {case_id}")
        if row.get("confidence") not in VALID_CONFIDENCE:
            raise ValueError(f"invalid confidence for {case_id}")
    return payload


def build_handler(directory: Path, save_path: Path, *, expected_count: int = 120):
    class ReviewHandler(SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=str(directory), **kwargs)

        def do_POST(self) -> None:  # noqa: N802
            if self.path != "/save-annotations":
                self.send_error(HTTPStatus.NOT_FOUND)
                return
            try:
                length = int(self.headers.get("Content-Length") or "0")
                if length <= 0 or length > 2_000_000:
                    raise ValueError("invalid content length")
                payload = validate_payload(json.loads(self.rfile.read(length)), expected_count=expected_count)
                save_path.parent.mkdir(parents=True, exist_ok=True)
                temporary = save_path.with_suffix(save_path.suffix + ".tmp")
                temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
                temporary.replace(save_path)
                body = json.dumps({"saved": True, "saved_path": str(save_path)}, ensure_ascii=False).encode("utf-8")
                self.send_response(HTTPStatus.OK)
            except (ValueError, json.JSONDecodeError) as exc:
                body = json.dumps({"saved": False, "error": str(exc)}, ensure_ascii=False).encode("utf-8")
                self.send_response(HTTPStatus.BAD_REQUEST)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return ReviewHandler


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--save-path", type=Path, required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8768)
    parser.add_argument("--expected-count", type=int, default=120)
    args = parser.parse_args()
    handler = build_handler(args.directory.resolve(), args.save_path.resolve(), expected_count=args.expected_count)
    ThreadingHTTPServer((args.host, args.port), handler).serve_forever()


if __name__ == "__main__":
    main()
