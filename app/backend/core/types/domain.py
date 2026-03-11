from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from datetime import date, datetime

@dataclass
class StockPrice:
    code: str
    date: date
    open: float
    high: float
    low: float
    close: float
    volume: float

@dataclass
class ScoreBreakdown:
    score_a: float
    score_b: float
    details: Dict[str, Any]

@dataclass
class GridRow:
    code: str
    name: str
    price: float
    change_rate: float
    score: Optional[float]
    # Add other fields as they become clear during refactoring
