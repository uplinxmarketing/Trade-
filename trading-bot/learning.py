"""Pattern learning — runs after every trade closes."""

import database
from typing import List, Dict, Any


def _rsi_bucket(rsi: float) -> str:
    """Round RSI down to nearest 10 for bucketing."""
    if rsi is None:
        return "unknown"
    base = int(rsi // 10) * 10
    return f"{base}-{base + 10}"


def learn_from_trade(trade: dict):
    """
    Extract market conditions from a closed trade and update the patterns table.
    """
    coin           = trade.get("coin", "UNKNOWN")
    rsi            = trade.get("entry_rsi")
    bb_position    = trade.get("entry_bb_position", "unknown")
    volume_trend   = trade.get("entry_volume_trend", "unknown")
    ma_position    = trade.get("entry_ma_position", "unknown")
    net_profit     = trade.get("net_profit", 0.0) or 0.0
    budget         = trade.get("budget_usdt", 100.0) or 100.0
    profitable     = 1 if net_profit > 0 else 0

    rsi_b      = _rsi_bucket(rsi) if rsi is not None else "unknown"
    pattern_key = f"{coin}|rsi:{rsi_b}|bb:{bb_position}|vol:{volume_trend}|ma:{ma_position}"
    profit_pct  = (net_profit / budget) * 100 if budget else 0.0

    database.upsert_pattern({
        "coin":               coin,
        "pattern_key":        pattern_key,
        "rsi_range":          rsi_b,
        "bb_position":        bb_position,
        "volume_trend":       volume_trend,
        "ma_position":        ma_position,
        "outcome_profitable": profitable,
        "avg_profit_pct":     profit_pct,
    })


def get_pattern_summary(min_occurrences: int = 3) -> List[dict]:
    """Returns patterns sorted by confidence_score DESC."""
    return database.get_patterns(min_occurrences=min_occurrences)
