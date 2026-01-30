from __future__ import annotations

import csv
import os
from typing import List, Dict, Any

from app.backend.domain.positions.parser import TradeParser
# Re-using config from legacy for now, or we can move it.
# Ideally we should minimize deps on app.core if we want pure infra, 
# but for now reusing app.core.config is practical.
from app.core.config import resolve_trade_csv_paths, canonical_trade_csv_path, resolve_trade_csv_dir

class TradeRepository:
    """
    Handles file I/O for trade CSVs.
    """
    
    @staticmethod
    def get_canonical_path(broker: str) -> str:
        return canonical_trade_csv_path(broker)

    @staticmethod
    def ensure_dir(path: str):
        os.makedirs(os.path.dirname(path), exist_ok=True)

    @staticmethod
    def save_raw_content(path: str, content: bytes):
        TradeRepository.ensure_dir(path)
        with open(path, "wb") as f:
            f.write(content)

    @staticmethod
    def read_csv_rows(path: str) -> tuple[List[List[str]], str]:
        """
        Reads CSV rows from path, auto-detecting encoding (cp932 or utf-8).
        Returns (rows, encoding_used)
        """
        if not os.path.exists(path):
            return [], ""
            
        # Try cp932 first (most common for Japanese CSVs)
        encodings = ["cp932", "utf-8-sig", "utf-8"]
        
        for encoding in encodings:
            try:
                with open(path, "r", encoding=encoding, newline="") as f:
                    reader = csv.reader(f)
                    rows = list(reader)
                    return rows, encoding
            except (UnicodeDecodeError, Exception):
                continue
                
        return [], ""

    @staticmethod
    def detect_broker_from_bytes(content: bytes, filename: str = "") -> tuple[str, str]:
        # Simple heuristic wrapper reusing domain logic where possible, 
        # but detection often needs raw string or bytes.
        
        # 1. Filename check
        fname = filename.lower()
        if "sbi" in fname: return "sbi", "filename"
        if "rakuten" in fname or "楽天" in fname: return "rakuten", "filename"
        
        # 2. Content check (decode snippet)
        head = b""
        for enc in ("cp932", "utf-8"):
            try:
                head_str = content[:4096].decode(enc, errors="ignore")
                
                if "受渡金額/決済損益" in head_str or "信用新規買" in head_str:
                    return "sbi", f"header:{enc}"
                if "口座" in head_str and "手数料" in head_str:
                    return "rakuten", f"header:{enc}"
            except:
                pass
                
        return "rakuten", "default"

    @staticmethod
    def load_and_parse_all() -> Dict[str, Any]:
        """
        Loads all configured trade CSVs and parses them.
        """
        paths = resolve_trade_csv_paths()
        existing_paths = [p for p in paths if os.path.isfile(p)]
        
        all_rows = []
        all_warnings = []
        
        if not existing_paths:
            missing = ", ".join(paths)
            all_warnings.append({"type": "trade_csv_missing", "message": f"trade_csv_missing:{missing}"})
            return {"rows": [], "warnings": all_warnings}

        for path in existing_paths:
            rows_raw, encoding = TradeRepository.read_csv_rows(path)
            if not rows_raw:
                continue
                
            # Detect type for this specific file
            if TradeParser.looks_like_sbi(rows_raw):
                result = TradeParser.parse_sbi_rows(rows_raw, encoding)
            else:
                result = TradeParser.parse_rakuten_rows(rows_raw, encoding)
                
            all_rows.extend(result["rows"])
            all_warnings.extend(result["warnings"])
            
        # Global deduplication could happen here if needed, 
        # but the parser handles per-batch deduplication.
        # Ideally we dedup across all files.
        
        # Re-deduplicate globally
        final_rows = []
        dedup_keys = set()
        
        for row in all_rows:
            # We can reconstruct a simple signature or use the one from parser if we exposed it.
            # For now, let's assume if it came from the parser it's valid, 
            # but if multiple files cover same range, we might duplicate.
            # Let's trust the parser's local dedup for now, assuming files don't overlap heavily 
            # or if they do, users manage it. 
            # (Legacy logic did global dedup, so we should probably match it).
            
            # Let's rebuild the key to be safe
            raw = row.get("raw", {})
            key_parts = [
                str(row.get("code")),
                str(row.get("tradeDate")),
                str(row.get("memo")), 
                str(row.get("qty")),
                str(row.get("price")),
                str(row.get("account"))
            ]
            global_key = "|".join(key_parts)
            
            if global_key in dedup_keys:
                continue
            dedup_keys.add(global_key)
            final_rows.append(row)
            
        final_rows.sort(key=lambda r: (r.get("tradeDate", ""), r.get("_event_order", 0)))
        
        return {"rows": final_rows, "warnings": all_warnings}

    @staticmethod
    def ingest_bytes(content: bytes, broker_override: str = None) -> Dict[str, Any]:
        """
        Parses raw bytes directly (used for upload preview/processing).
        """
        # We need to decode to list of lists
        lines = []
        enc = ""
        for encoding in ["cp932", "utf-8-sig", "utf-8"]:
            try:
                decoded = content.decode(encoding)
                enc = encoding
                lines = list(csv.reader(decoded.splitlines()))
                break
            except:
                continue
        
        if not lines:
            return {"rows": [], "warnings": [{"type": "decode_error", "message": "Could not decode file"}]}
            
        # Detect broker if not provided
        is_sbi = False
        if broker_override == "sbi":
            is_sbi = True
        elif broker_override == "rakuten":
            is_sbi = False
        else:
            is_sbi = TradeParser.looks_like_sbi(lines)
            
        if is_sbi:
            return TradeParser.parse_sbi_rows(lines, enc)
        else:
            return TradeParser.parse_rakuten_rows(lines, enc)
