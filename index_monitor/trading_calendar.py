from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, Optional, Set


# Exchange holiday guards used by automatic jobs.  Weekends are always closed
# for both A-share and US equity markets; public-holiday weekend adjustment days
# are not trading days for the exchanges.
#
# 2026 entries are intentionally explicit because 2026-06-19 is both China's
# Dragon Boat Festival break and the US Juneteenth market holiday.
CN_MARKET_HOLIDAYS: Set[str] = {
    # New Year's Day
    "2026-01-01",
    "2026-01-02",
    "2026-01-03",
    # Spring Festival
    "2026-02-15",
    "2026-02-16",
    "2026-02-17",
    "2026-02-18",
    "2026-02-19",
    "2026-02-20",
    "2026-02-21",
    "2026-02-22",
    "2026-02-23",
    # Qingming
    "2026-04-04",
    "2026-04-05",
    "2026-04-06",
    # Labour Day
    "2026-05-01",
    "2026-05-02",
    "2026-05-03",
    "2026-05-04",
    "2026-05-05",
    # Dragon Boat Festival
    "2026-06-19",
    "2026-06-20",
    "2026-06-21",
    # Mid-Autumn Festival
    "2026-09-25",
    "2026-09-26",
    "2026-09-27",
    # National Day
    "2026-10-01",
    "2026-10-02",
    "2026-10-03",
    "2026-10-04",
    "2026-10-05",
    "2026-10-06",
    "2026-10-07",
}


US_MARKET_HOLIDAYS: Set[str] = {
    "2026-01-01",  # New Year's Day
    "2026-01-19",  # Martin Luther King Jr. Day
    "2026-02-16",  # Washington's Birthday
    "2026-04-03",  # Good Friday
    "2026-05-25",  # Memorial Day
    "2026-06-19",  # Juneteenth National Independence Day
    "2026-07-03",  # Independence Day observed
    "2026-09-07",  # Labor Day
    "2026-11-26",  # Thanksgiving Day
    "2026-12-25",  # Christmas Day
}


# Hong Kong securities-market holidays published by HKEX for 2026.
# Keep this exchange calendar separate from the mainland calendar: there are
# dates where one market is open while the other is closed.
HK_MARKET_HOLIDAYS: Set[str] = {
    "2026-01-01",  # The first day of January
    "2026-02-17",  # Lunar New Year's Day
    "2026-02-18",  # The second day of Lunar New Year
    "2026-02-19",  # The third day of Lunar New Year
    "2026-04-03",  # Good Friday
    "2026-04-06",  # The day following Ching Ming Festival
    "2026-04-07",  # The day following Easter Monday
    "2026-05-01",  # Labour Day
    "2026-05-25",  # The day following the Birthday of the Buddha
    "2026-06-19",  # Tuen Ng Festival
    "2026-07-01",  # HKSAR Establishment Day
    "2026-10-01",  # National Day
    "2026-10-19",  # The day following Chung Yeung Festival
    "2026-12-25",  # Christmas Day
}


def _env_dates(name: str) -> Set[str]:
    raw = os.environ.get(name, "")
    return {part.strip()[:10] for part in raw.replace(";", ",").split(",") if part.strip()}


def _as_date(value: Optional[date | datetime | str] = None) -> date:
    if value is None:
        return datetime.now().date()
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()[:10]
    return datetime.strptime(text, "%Y-%m-%d").date()


def _date_text(value: Optional[date | datetime | str] = None) -> str:
    return _as_date(value).isoformat()


def _is_weekday(day: date) -> bool:
    return day.weekday() < 5


def _reason(day: date, holidays: Iterable[str], label: str) -> str:
    text = day.isoformat()
    if not _is_weekday(day):
        return f"{label}周末休市"
    if text in set(holidays):
        return f"{label}节假日休市"
    return f"{label}交易日"


def cn_market_holidays() -> Set[str]:
    return set(CN_MARKET_HOLIDAYS) | _env_dates("CN_MARKET_EXTRA_HOLIDAYS")


def us_market_holidays() -> Set[str]:
    return set(US_MARKET_HOLIDAYS) | _env_dates("US_MARKET_EXTRA_HOLIDAYS")


def hk_market_holidays() -> Set[str]:
    return set(HK_MARKET_HOLIDAYS) | _env_dates("HK_MARKET_EXTRA_HOLIDAYS")


def is_cn_trading_day(value: Optional[date | datetime | str] = None) -> bool:
    day = _as_date(value)
    return _is_weekday(day) and day.isoformat() not in cn_market_holidays()


def is_us_trading_day(value: Optional[date | datetime | str] = None) -> bool:
    day = _as_date(value)
    return _is_weekday(day) and day.isoformat() not in us_market_holidays()


def is_hk_trading_day(value: Optional[date | datetime | str] = None) -> bool:
    day = _as_date(value)
    return _is_weekday(day) and day.isoformat() not in hk_market_holidays()


def next_trading_day(
    value: Optional[date | datetime | str] = None,
    *,
    market: str = "CN",
) -> date:
    """Return the next exchange day after *value* for the selected market."""

    market_key = str(market or "CN").strip().upper()
    predicates = {
        "CN": is_cn_trading_day,
        "A": is_cn_trading_day,
        "HK": is_hk_trading_day,
        "US": is_us_trading_day,
    }
    predicate = predicates.get(market_key)
    if predicate is None:
        raise ValueError(f"unsupported market: {market}")
    candidate = _as_date(value) + timedelta(days=1)
    while not predicate(candidate):
        candidate += timedelta(days=1)
    return candidate


def market_day_status(value: Optional[date | datetime | str] = None) -> Dict[str, Any]:
    day = _as_date(value)
    return {
        "date": day.isoformat(),
        "cn": {
            "market": "A股",
            "tradingDay": is_cn_trading_day(day),
            "reason": _reason(day, cn_market_holidays(), "A股"),
        },
        "hk": {
            "market": "港股",
            "tradingDay": is_hk_trading_day(day),
            "reason": _reason(day, hk_market_holidays(), "港股"),
        },
        "us": {
            "market": "美股",
            "tradingDay": is_us_trading_day(day),
            "reason": _reason(day, us_market_holidays(), "美股"),
        },
    }


def cn_market_closed_payload(
    task: str,
    *,
    trigger: str = "auto",
    value: Optional[date | datetime | str] = None,
) -> Dict[str, Any]:
    status = market_day_status(value)
    reason = status["cn"]["reason"]
    return {
        "ok": True,
        "skipped": True,
        "marketClosed": True,
        "task": task,
        "trigger": trigger,
        "generatedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "marketCalendar": status,
        "message": f"{status['date']} {reason}，自动任务暂停",
        "summary": f"{reason}，跳过{task}",
    }
