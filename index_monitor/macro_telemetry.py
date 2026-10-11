"""Pure parsing and item-building helpers for macro telemetry sources."""

from __future__ import annotations

import csv
import io
import json
import math
import os
import re
import tempfile
import threading
import time
from collections.abc import Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from .paths import DATA_DIR
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo
from .central_bank_gold import fetch_china_current, fetch_turkey_history
from .china_bond_yields import fetch_chinabond_yield_rows


__all__ = [
    "build_korea_item",
    "build_macro_telemetry_bundle",
    "build_chinabond_items",
    "build_treasury_items",
    "build_yield_risk",
    "parse_kofia_payload",
    "parse_treasury_csv",
    "build_gold_item",
    "build_cb_gold_items",
]


_TREASURY_DATE_FORMATS = ("%m/%d/%Y",)
_TREASURY_DATE_PATTERN = re.compile(r"^\d{2}/\d{2}/\d{4}$")
_ISO_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")

TREASURY_CSV_URL = (
    "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/"
    "daily-treasury-rates.csv/{year}/all"
)
KOFIA_URL = "https://freesis.kofia.or.kr/meta/getMetaDataList.do"
GOLD_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/GC=F"
GOLD_RANGE = "1y"
GOLD_INTERVAL = "1d"

_CACHE_PATH = DATA_DIR / "macro_telemetry_cache.json"
_CACHE_TTL_SECONDS = 1800
_CACHE_SCHEMA = 3
_REQUEST_TIMEOUT_SECONDS = 15
_MAX_REQUEST_ATTEMPTS = 2
_MAX_RESPONSE_BYTES = 5 * 1024 * 1024
_ITEM_ORDER = (
    "us10y",
    "us30y",
    "cn10y",
    "cn30y",
    "kr_margin_financing",
    "gold",
    "cn_gold_reserves",
    "tr_gold_reserves",
)
_ITEM_SPECS = {
    "us10y": {"name": "10年美债收益率", "region": "US", "source": "U.S. Treasury", "unit": "%", "changeUnit": "bp"},
    "us30y": {"name": "30年美债收益率", "region": "US", "source": "U.S. Treasury", "unit": "%", "changeUnit": "bp"},
    "cn10y": {"name": "10年中国国债收益率", "region": "CN", "source": "ChinaBond（中央国债登记结算有限责任公司）", "unit": "%", "changeUnit": "bp"},
    "cn30y": {"name": "30年中国国债收益率", "region": "CN", "source": "ChinaBond（中央国债登记结算有限责任公司）", "unit": "%", "changeUnit": "bp"},
    "kr_margin_financing": {"name": "韩国信用融资余额", "region": "KR", "source": "KOFIA FreeSIS", "unit": "万亿韩元", "changeUnit": "万亿韩元"},
    "gold": {"name": "黄金（COMEX）", "region": "Global", "source": "Yahoo Finance", "unit": "USD/oz", "changeUnit": "USD"},
    "cn_gold_reserves": {"name": "中国官方黄金储备（央行）", "region": "CN", "source": "State Administration of Foreign Exchange (SAFE)", "unit": "吨", "changeUnit": "吨"},
    "tr_gold_reserves": {"name": "土耳其央行黄金储备", "region": "TR", "source": "Central Bank of the Republic of Türkiye (TCMB) EVDS3", "unit": "吨", "changeUnit": "吨"},
}

_CACHE_LOCK = threading.RLock()
_REFRESH_LOCK = threading.Lock()
_MEMORY_RECORD: dict[str, Any] | None = None
_REFRESH_THREAD: threading.Thread | None = None


def _date_text(value: Any, formats: tuple[str, ...]) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    for date_format in formats:
        try:
            return datetime.strptime(text, date_format).date().isoformat()
        except ValueError:
            continue
    return None


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        number = float(text)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _yield_value(value: Any) -> float | None:
    number = _number(value)
    if number is None or not 0 < number < 20:
        return None
    return number


def _has_value(value: Any) -> bool:
    return value is not None and str(value).strip() != ""


def _kofia_date(value: Any) -> str | None:
    text = "" if value is None else str(value).strip()
    if len(text) != 8 or not text.isdigit():
        return None
    return _date_text(text, ("%Y%m%d",))


def parse_treasury_csv(text: str) -> list[dict[str, Any]]:
    """Parse the U.S. Treasury daily-yield CSV into normalized date rows."""

    if not isinstance(text, str):
        raise ValueError("Treasury response must be CSV text")
    probe = text.lstrip("\ufeff \t\r\n").lower()
    if not probe or any(marker in probe for marker in ("<!doctype html", "<html", "<body", "access denied")):
        raise ValueError("Treasury response is not a usable CSV")

    reader = csv.DictReader(io.StringIO(text.lstrip("\ufeff")))
    if not reader.fieldnames:
        raise ValueError("Treasury CSV has no header")
    columns = {str(column).strip(): column for column in reader.fieldnames if column is not None}
    if "Date" not in columns or "10 Yr" not in columns:
        raise ValueError("Treasury CSV is missing Date or 10 Yr")

    date_column = columns["Date"]
    ten_year_column = columns["10 Yr"]
    thirty_year_column = columns.get("30 Yr")
    rows_by_date: dict[str, dict[str, Any]] = {}
    for raw_row in reader:
        raw_date = str(raw_row.get(date_column) or "").strip()
        date_text = (
            _date_text(raw_date, _TREASURY_DATE_FORMATS)
            if _TREASURY_DATE_PATTERN.fullmatch(raw_date)
            else None
        )
        if date_text is None:
            continue
        ten_year = _yield_value(raw_row.get(ten_year_column))
        thirty_year = _yield_value(raw_row.get(thirty_year_column)) if thirty_year_column is not None else None
        if ten_year is None and thirty_year is None:
            continue
        rows_by_date[date_text] = {"date": date_text, "us10y": ten_year, "us30y": thirty_year}

    if not rows_by_date:
        raise ValueError("Treasury CSV contains no valid yield rows")
    return [rows_by_date[date_text] for date_text in sorted(rows_by_date)]


def _observations(rows: Iterable[Mapping[str, Any]], value_key: str) -> list[tuple[str, float]]:
    observations_by_date: dict[str, float] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        date_text = _date_text(row.get("date"), ("%Y-%m-%d",))
        value = _yield_value(row.get(value_key))
        if date_text is not None and value is not None:
            observations_by_date[date_text] = value
    return sorted(observations_by_date.items())


_YIELD_KEYS = frozenset(("us10y", "us30y", "cn10y", "cn30y"))


def _yield_item(rows: Iterable[Mapping[str, Any]], *, key: str) -> dict[str, Any]:
    observations = _observations(rows, key)
    latest_date = latest_value = previous_date = previous_value = None
    if observations:
        latest_date, latest_value = observations[-1]
    if len(observations) >= 2:
        previous_date, previous_value = observations[-2]

    change = None
    if latest_value is not None and previous_value is not None:
        change = round((latest_value - previous_value) * 100, 10)
        if change == 0:
            change = 0.0

    spec = _ITEM_SPECS[key]
    return {
        "key": key,
        "name": spec["name"],
        "region": spec["region"],
        "source": spec["source"],
        "value": latest_value,
        "unit": spec["unit"],
        "change": change,
        "changeUnit": spec["changeUnit"],
        "changePct": None,
        "asOf": latest_date,
        "previousAsOf": previous_date,
        "stale": False,
    }


def build_yield_risk(
    ten_year: Mapping[str, Any], thirty_year: Mapping[str, Any]
) -> dict[str, str]:
    """Classify one country's 10Y/30Y curve from raw percent values and bp moves."""

    ten_value = _yield_value(ten_year.get("value"))
    thirty_value = _yield_value(thirty_year.get("value"))
    ten_change = _number(ten_year.get("change"))
    thirty_change = _number(thirty_year.get("change"))
    if (
        ten_value is None
        or thirty_value is None
        or ten_change is None
        or thirty_change is None
    ):
        raise ValueError("yield risk requires two valid values and changes")

    spread_bp = (thirty_value - ten_value) * 100
    max_move_bp = max(abs(ten_change), abs(thirty_change))
    spread_text = f"{spread_bp:+.3g}"
    move_text = f"{max_move_bp:.3g}"
    if spread_bp <= 0:
        return {
            "level": "high",
            "message": (
                f"10Y/30Y期限利差{spread_text}bp（≤0bp），曲线倒挂或持平，"
                "触发高等级收益率风险提示。"
            ),
        }
    if max_move_bp >= 8:
        return {
            "level": "high",
            "message": (
                f"10Y/30Y期限利差{spread_text}bp；相邻有效观测最大变动"
                f"{move_text}bp（≥8bp），触发高等级收益率风险提示。"
            ),
        }
    if max_move_bp >= 4:
        return {
            "level": "watch",
            "message": (
                f"10Y/30Y期限利差{spread_text}bp；相邻有效观测最大变动"
                f"{move_text}bp（≥4bp），触发收益率波动观察。"
            ),
        }
    return {
        "level": "normal",
        "message": (
            f"10Y/30Y期限利差{spread_text}bp；相邻有效观测变动均小于4bp，"
            "未触发收益率风险提示。"
        ),
    }


def _attach_yield_pair_risk(
    items: Iterable[Mapping[str, Any]], *, ten_key: str, thirty_key: str
) -> list[dict[str, Any]]:
    """Attach a pair-level risk only when fresh source observations align."""

    normalized = [dict(item) for item in items]
    by_key = {str(item.get("key")): item for item in normalized}
    ten_year = by_key.get(ten_key)
    thirty_year = by_key.get(thirty_key)
    if ten_year is None or thirty_year is None:
        return normalized
    if ten_year.get("stale") or thirty_year.get("stale"):
        return normalized
    if (
        ten_year.get("asOf") != thirty_year.get("asOf")
        or ten_year.get("previousAsOf") != thirty_year.get("previousAsOf")
    ):
        return normalized
    try:
        risk = build_yield_risk(ten_year, thirty_year)
    except ValueError:
        return normalized
    ten_year["risk"] = dict(risk)
    thirty_year["risk"] = dict(risk)
    return normalized


def build_treasury_items(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Build the fixed 10-year and 30-year Treasury telemetry items."""

    materialized = list(rows)
    return _attach_yield_pair_risk(
        [
            _yield_item(materialized, key="us10y"),
            _yield_item(materialized, key="us30y"),
        ],
        ten_key="us10y",
        thirty_key="us30y",
    )


def build_chinabond_items(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Build ChinaBond government-curve 10Y/30Y telemetry and pair risk."""

    materialized = list(rows)
    return _attach_yield_pair_risk(
        [
            _yield_item(materialized, key="cn10y"),
            _yield_item(materialized, key="cn30y"),
        ],
        ten_key="cn10y",
        thirty_key="cn30y",
    )


def parse_kofia_payload(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Parse KOFIA FreeSIS ``ds1`` rows and validate reported components."""

    if not isinstance(payload, Mapping):
        raise ValueError("KOFIA payload must be an object")
    dataset = payload.get("ds1")
    if not isinstance(dataset, list):
        raise ValueError("KOFIA payload has no ds1 rows")

    rows_by_date: dict[str, dict[str, Any]] = {}
    for raw_row in dataset:
        if not isinstance(raw_row, Mapping):
            continue
        date_text = _kofia_date(raw_row.get("TMPV1"))
        total = _number(raw_row.get("TMPV2"))
        if date_text is None or total is None or total <= 0:
            continue

        part_three_raw = raw_row.get("TMPV3")
        part_four_raw = raw_row.get("TMPV4")
        if _has_value(part_three_raw) and _has_value(part_four_raw):
            part_three = _number(part_three_raw)
            part_four = _number(part_four_raw)
            if part_three is None or part_four is None:
                continue
            tolerance = max(1.0, total * 0.001)
            if abs((part_three + part_four) - total) > tolerance:
                raise ValueError("KOFIA component total does not match TMPV2")

        rows_by_date[date_text] = {"date": date_text, "totalMillionKrw": total}

    if not rows_by_date:
        raise ValueError("KOFIA payload contains no valid rows")
    return [rows_by_date[date_text] for date_text in sorted(rows_by_date)]


def build_korea_item(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Build the Korean margin-financing item in trillion won."""

    observations_by_date: dict[str, float] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        date_text = _date_text(row.get("date"), ("%Y-%m-%d",))
        total = _number(row.get("totalMillionKrw"))
        if date_text is not None and total is not None and total > 0:
            observations_by_date[date_text] = total

    observations = sorted(observations_by_date.items())
    if len(observations) < 2:
        raise ValueError("Korean margin-financing telemetry requires at least two rows")
    previous_date, previous_total = observations[-2]
    latest_date, latest_total = observations[-1]
    spec = _ITEM_SPECS["kr_margin_financing"]
    return {
        "key": "kr_margin_financing",
        "name": spec["name"],
        "region": spec["region"],
        "source": spec["source"],
        "value": round(latest_total / 1_000_000, 3),
        "unit": spec["unit"],
        "change": round((latest_total - previous_total) / 1_000_000, 3),
        "changeUnit": spec["changeUnit"],
        "changePct": round((latest_total - previous_total) / previous_total * 100, 2),
        "asOf": latest_date,
        "previousAsOf": previous_date,
        "stale": False,
    }


def _strict_iso_date(value: Any) -> str | None:
    text = "" if value is None else str(value).strip()
    if not _ISO_DATE_PATTERN.fullmatch(text):
        return None
    return _date_text(text, ("%Y-%m-%d",))


def _validated_item(raw_item: Mapping[str, Any], expected_key: str) -> dict[str, Any]:
    if not isinstance(raw_item, Mapping) or raw_item.get("key") != expected_key:
        raise ValueError(f"Macro telemetry item {expected_key} is invalid")
    spec = _ITEM_SPECS[expected_key]
    for field in ("name", "region", "source", "unit", "changeUnit"):
        if raw_item.get(field) != spec[field]:
            raise ValueError(f"Macro telemetry item {expected_key} has invalid {field}")

    value = _number(raw_item.get("value"))
    if value is None or value <= 0:
        raise ValueError(f"Macro telemetry item {expected_key} has invalid value")
    if expected_key in _YIELD_KEYS and value >= 20:
        raise ValueError(f"Macro telemetry item {expected_key} has invalid yield")

    change_raw = raw_item.get("change")
    change = None if change_raw is None else _number(change_raw)
    if change_raw is not None and change is None:
        raise ValueError(f"Macro telemetry item {expected_key} has invalid change")
    change_pct_raw = raw_item.get("changePct")
    change_pct = None if change_pct_raw is None else _number(change_pct_raw)
    if change_pct_raw is not None and change_pct is None:
        raise ValueError(f"Macro telemetry item {expected_key} has invalid changePct")

    as_of = _strict_iso_date(raw_item.get("asOf"))
    if as_of is None:
        raise ValueError(f"Macro telemetry item {expected_key} has invalid asOf")
    previous_raw = raw_item.get("previousAsOf")
    previous_as_of = None if previous_raw is None else _strict_iso_date(previous_raw)
    if previous_raw is not None and previous_as_of is None:
        raise ValueError(f"Macro telemetry item {expected_key} has invalid previousAsOf")
    previous_value_raw = raw_item.get("previousValue")
    previous_value = None if previous_value_raw is None else _number(previous_value_raw)
    if previous_value_raw is not None and (previous_value is None or previous_value <= 0):
        raise ValueError(f"Macro telemetry item {expected_key} has invalid previousValue")
    if expected_key in ("cn_gold_reserves", "tr_gold_reserves"):
        if previous_value is None or previous_as_of is None or previous_as_of >= as_of:
            raise ValueError(f"Macro telemetry item {expected_key} requires an ordered prior observation")
    stale = raw_item.get("stale")
    if not isinstance(stale, bool):
        raise ValueError(f"Macro telemetry item {expected_key} has invalid stale flag")

    result = {
        "key": expected_key,
        "name": spec["name"],
        "region": spec["region"],
        "source": spec["source"],
        "value": value,
        "unit": spec["unit"],
        "change": change,
        "changeUnit": spec["changeUnit"],
        "changePct": change_pct,
        "asOf": as_of,
        "previousAsOf": previous_as_of,
        "stale": stale,
    }
    if previous_value is not None:
        result["previousValue"] = previous_value
    risk = raw_item.get("risk")
    if risk is not None:
        if expected_key not in _YIELD_KEYS or not isinstance(risk, Mapping):
            raise ValueError(f"Macro telemetry item {expected_key} has invalid risk")
        level = risk.get("level")
        message = risk.get("message")
        if set(risk) != {"level", "message"} or level not in {"normal", "watch", "high"}:
            raise ValueError(f"Macro telemetry item {expected_key} has invalid risk")
        if not isinstance(message, str) or not message.strip() or len(message) > 500:
            raise ValueError(f"Macro telemetry item {expected_key} has invalid risk")
        result["risk"] = {"level": level, "message": message.strip()}
    for extra_field in (
        "trend",
        "topSignal",
        "bottomSignal",
        "ma20",
        "ma60",
        "rsi14",
        "actionSignal",
        "frequency",
        "sourceUrl",
    ):
        extra_value = raw_item.get(extra_field)
        if extra_value is not None:
            result[extra_field] = extra_value
    return result


def _validated_items(raw_items: Any, required_keys: tuple[str, ...]) -> list[dict[str, Any]]:
    if not isinstance(raw_items, list):
        raise ValueError("Macro telemetry items must be a list")
    by_key: dict[str, dict[str, Any]] = {}
    for raw_item in raw_items:
        if not isinstance(raw_item, Mapping):
            raise ValueError("Macro telemetry item must be an object")
        key = str(raw_item.get("key") or "")
        if key not in required_keys or key in by_key:
            raise ValueError("Macro telemetry item keys are invalid")
        by_key[key] = _validated_item(raw_item, key)
    if set(by_key) != set(required_keys):
        raise ValueError("Macro telemetry item set is incomplete")
    return [by_key[key] for key in required_keys]


def _request_bytes(request: Request) -> tuple[bytes, str]:
    last_error: Exception | None = None
    for attempt in range(_MAX_REQUEST_ATTEMPTS):
        try:
            with urlopen(request, timeout=_REQUEST_TIMEOUT_SECONDS) as response:
                status = int(getattr(response, "status", 200) or 200)
                if status >= 400:
                    raise RuntimeError(f"HTTP {status}")
                body = response.read(_MAX_RESPONSE_BYTES + 1)
                if len(body) > _MAX_RESPONSE_BYTES:
                    raise ValueError("Macro telemetry response is too large")
                content_type = str(response.headers.get("Content-Type", "")).lower()
                return body, content_type
        except Exception as exc:  # urllib raises several transport-specific subclasses
            last_error = exc
            if attempt + 1 < _MAX_REQUEST_ATTEMPTS:
                time.sleep(0.2 * (attempt + 1))
    raise RuntimeError("Macro telemetry request failed: " + type(last_error).__name__) from last_error


# ---- Gold telemetry helpers -------------------------------------------------

def _ma(values, period):
    if len(values) < period:
        return None
    window = values[-period:]
    return sum(window) / len(window)


def _rsi(prices, period=14):
    if len(prices) < period + 1:
        return None
    gains = 0.0
    losses = 0.0
    for i in range(-period, 0):
        diff = prices[i] - prices[i - 1]
        if diff > 0:
            gains += diff
        else:
            losses -= diff
    avg_gain = gains / period
    avg_loss = losses / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def _compute_gold_signals(prices, highs, lows):
    if len(prices) < 60:
        return '数据不足', None, None

    ma5 = _ma(prices, 5)
    ma20 = _ma(prices, 20)
    ma60 = _ma(prices, 60)
    rsi14 = _rsi(prices, 14)

    if ma5 is None or ma20 is None or ma60 is None or rsi14 is None:
        return '数据不足', None, None

    current = prices[-1]
    high_1y = max(highs[-252:]) if len(highs) >= 252 else max(highs)
    low_1y = min(lows[-252:]) if len(lows) >= 252 else min(lows)
    dist_high = (current - high_1y) / high_1y if high_1y else 0
    dist_low = (current - low_1y) / low_1y if low_1y else 0
    dist_ma20 = (current - ma20) / ma20 if ma20 else 0

    if ma5 > ma20 > ma60:
        trend = '上升趋势'
    elif ma5 < ma20 < ma60:
        trend = '下降趋势'
    else:
        trend = '震荡整理'

    top_signal = None
    if rsi14 > 70:
        if dist_high > -0.05:
            top_signal = f'RSI={rsi14:.0f} 超买·逼近1年高点'
        elif dist_ma20 > 0.08:
            top_signal = f'RSI={rsi14:.0f} 超买·偏离MA20 +{dist_ma20*100:.1f}%'
    elif rsi14 > 65 and dist_high > -0.03:
        top_signal = f'RSI={rsi14:.0f} 偏强·接近1年高点'

    bottom_signal = None
    if rsi14 < 35:
        if dist_low < 0.05:
            bottom_signal = f'RSI={rsi14:.0f} 超卖·逼近1年低点'
        elif dist_ma20 < -0.08:
            bottom_signal = f'RSI={rsi14:.0f} 超卖·偏离MA20 {dist_ma20*100:.1f}%'
    elif rsi14 < 40 and dist_low < 0.03:
        bottom_signal = f'RSI={rsi14:.0f} 偏弱·接近1年低点'

    return trend, top_signal, bottom_signal


def build_gold_item(raw_data: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(raw_data, Mapping):
        raise ValueError("Gold response is not an object")
    chart = raw_data.get("chart")
    if not isinstance(chart, Mapping):
        raise ValueError("Gold response missing chart")
    results = chart.get("result")
    if not isinstance(results, list) or not results or not isinstance(results[0], Mapping):
        raise ValueError("Gold response has no result")
    result = results[0]
    meta = result.get("meta") if isinstance(result.get("meta"), Mapping) else {}
    price = _number(meta.get("regularMarketPrice"))
    previous = _number(meta.get("chartPreviousClose"))
    if price is None or price <= 0:
        raise ValueError("Gold price unavailable")

    quotes = result.get("indicators", {}).get("quote", [])
    quote = quotes[0] if isinstance(quotes, list) and quotes and isinstance(quotes[0], Mapping) else {}
    closes = [_number(value) for value in (quote.get("close") or []) if _number(value) is not None]
    highs = [_number(value) for value in (quote.get("high") or []) if _number(value) is not None]
    lows = [_number(value) for value in (quote.get("low") or []) if _number(value) is not None]
    trend, top_signal, bottom_signal = _compute_gold_signals(closes, highs, lows)

    timestamp_list = result.get("timestamp", [])
    latest_ts = timestamp_list[-1] if timestamp_list else None
    as_of = (
        datetime.fromtimestamp(latest_ts, tz=ZoneInfo("America/New_York")).date().isoformat()
        if latest_ts else datetime.now(ZoneInfo("America/New_York")).date().isoformat()
    )
    previous_as_of = (
        datetime.fromtimestamp(timestamp_list[-2], tz=ZoneInfo("America/New_York")).date().isoformat()
        if len(timestamp_list) >= 2 else None
    )
    prev_close = closes[-2] if len(closes) >= 2 else previous
    change = round(price - prev_close, 2) if prev_close is not None and prev_close > 0 else None
    change_pct = round(change / prev_close * 100, 2) if change is not None and prev_close else None
    spec = _ITEM_SPECS["gold"]
    ma20 = _ma(closes, 20)
    ma60 = _ma(closes, 60)
    rsi14 = _rsi(closes, 14)
    return {
        "key": "gold",
        "name": spec["name"],
        "region": spec["region"],
        "source": spec["source"],
        "unit": spec["unit"],
        "changeUnit": spec["changeUnit"],
        "value": price,
        "change": change,
        "changePct": change_pct,
        "asOf": as_of,
        "previousAsOf": previous_as_of,
        "stale": False,
        "trend": trend,
        "topSignal": top_signal,
        "bottomSignal": bottom_signal,
        "ma20": round(ma20, 2) if ma20 is not None else None,
        "ma60": round(ma60, 2) if ma60 is not None else None,
        "rsi14": round(rsi14, 1) if rsi14 is not None else None,
    }


def _fetch_gold_item():
    import urllib.parse
    query = urllib.parse.urlencode({
        'range': GOLD_RANGE,
        'interval': GOLD_INTERVAL,
    })
    url = f'{GOLD_CHART_URL}?{query}'
    request = Request(
        url,
        headers={
            'Accept': 'application/json',
            'User-Agent': 'StockSignalDashboard/0.5.6',
        },
    )
    raw_body, content_type = _request_bytes(request)
    text = raw_body.decode('utf-8-sig')
    if 'html' in content_type or not text.lstrip().startswith('{'):
        raise ValueError('Gold response is not JSON')
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError('Gold response is invalid JSON') from exc
    item = build_gold_item(payload)
    return _validated_items([item], ('gold',))[0]



def _central_bank_trend_label(trend: Any) -> str:
    return {
        "up": "储备增加",
        "down": "储备减少",
        "flat": "储备持平",
    }.get(str(trend or ""), "数据积累中")


def _build_central_bank_gold_item(key: str, bundle: Mapping[str, Any]) -> dict[str, Any]:
    """Convert one official reserve-history bundle into the macro card contract."""

    spec = _ITEM_SPECS[key]
    snapshot = bundle.get("snapshot") if isinstance(bundle, Mapping) else None
    if not isinstance(snapshot, Mapping):
        raise ValueError(f"{key} official history has no snapshot")
    value = _number(snapshot.get("value"))
    previous_value = _number(snapshot.get("previousValue"))
    as_of = _strict_iso_date(snapshot.get("asOf"))
    previous_as_of = _strict_iso_date(snapshot.get("previousAsOf"))
    if value is None or value <= 0 or previous_value is None or previous_value <= 0:
        raise ValueError(f"{key} official history requires latest and prior values")
    if as_of is None or previous_as_of is None or previous_as_of >= as_of:
        raise ValueError(f"{key} official history has invalid observation dates")

    value = round(value, 3)
    previous_value = round(previous_value, 3)
    change = round(value - previous_value, 3)
    change_pct = round(change / previous_value * 100, 3)
    source_url = str(bundle.get("sourceUrl") or "").strip()
    frequency = str(bundle.get("frequency") or "monthly").strip().lower()
    if not source_url.startswith("https://") or frequency != "monthly":
        raise ValueError(f"{key} official history metadata is invalid")
    return {
        "key": key,
        "name": spec["name"],
        "region": spec["region"],
        "source": spec["source"],
        "value": value,
        "unit": spec["unit"],
        "change": change,
        "changeUnit": spec["changeUnit"],
        "changePct": change_pct,
        "asOf": as_of,
        "previousValue": previous_value,
        "previousAsOf": previous_as_of,
        "stale": False,
        "trend": _central_bank_trend_label(snapshot.get("trend")),
        "frequency": frequency,
        "sourceUrl": source_url,
    }


def build_cb_gold_items(china_history: Mapping[str, Any], turkey_history: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Build China and Türkiye official-reserve cards from verified history bundles."""
    return [
        _build_central_bank_gold_item("cn_gold_reserves", china_history),
        _build_central_bank_gold_item("tr_gold_reserves", turkey_history),
    ]


def _fetch_china_gold_item() -> dict[str, Any]:
    item = _build_central_bank_gold_item("cn_gold_reserves", fetch_china_current())
    return _validated_items([item], ("cn_gold_reserves",))[0]


def _fetch_turkey_gold_item() -> dict[str, Any]:
    item = _build_central_bank_gold_item("tr_gold_reserves", fetch_turkey_history())
    return _validated_items([item], ("tr_gold_reserves",))[0]


def _fetch_cb_gold_both() -> list[dict[str, Any]]:
    """Compatibility helper used by integrations that refresh both reserve cards."""
    return [_fetch_china_gold_item(), _fetch_turkey_gold_item()]


def _fetch_treasury_items() -> list[dict[str, Any]]:
    current_year = datetime.now(ZoneInfo("America/New_York")).year
    rows: list[dict[str, Any]] = []
    failures: list[str] = []
    for year in (current_year - 1, current_year):
        query = urlencode(
            {
                "type": "daily_treasury_yield_curve",
                "field_tdr_date_value": str(year),
                "page": "",
                "_format": "csv",
            }
        )
        request = Request(
            f"{TREASURY_CSV_URL.format(year=year)}?{query}",
            headers={"Accept": "text/csv,*/*;q=0.8", "User-Agent": "StockSignalDashboard/0.5.6"},
        )
        try:
            body, content_type = _request_bytes(request)
            text = body.decode("utf-8-sig")
            first_line = text.splitlines()[0] if text.splitlines() else ""
            if "html" in content_type or "Date" not in first_line or "10 Yr" not in first_line:
                raise ValueError("Treasury response is not CSV")
            rows.extend(parse_treasury_csv(text))
        except Exception as exc:
            failures.append(f"{year}: {exc}")
    if not rows:
        raise RuntimeError("Treasury unavailable: " + "; ".join(failures))
    items = build_treasury_items(rows)
    return _validated_items(items, ("us10y", "us30y"))


def _fetch_chinabond_items() -> list[dict[str, Any]]:
    """Fetch the latest pair of ChinaBond government-curve observations."""

    end_date = datetime.now(ZoneInfo("Asia/Shanghai")).date()
    last_error: Exception | None = None
    for attempt in range(_MAX_REQUEST_ATTEMPTS):
        try:
            rows = fetch_chinabond_yield_rows(end_date - timedelta(days=45), end_date)
            items = build_chinabond_items(rows)
            return _validated_items(items, ("cn10y", "cn30y"))
        except Exception as exc:
            last_error = exc
            if attempt + 1 < _MAX_REQUEST_ATTEMPTS:
                time.sleep(0.2 * (attempt + 1))
    raise RuntimeError("ChinaBond unavailable") from last_error


def _fetch_korea_item() -> dict[str, Any]:
    end_date = datetime.now(ZoneInfo("Asia/Seoul")).date()
    start_date = end_date - timedelta(days=44)
    body = json.dumps(
        {
            "dmSearch": {
                "OBJ_NM": "STATSCU0100000070BO",
                "tmpV1": "D",
                "tmpV40": "1000000",
                "tmpV41": "1",
                "tmpV45": start_date.strftime("%Y%m%d"),
                "tmpV46": end_date.strftime("%Y%m%d"),
            }
        },
        separators=(",", ":"),
    ).encode("utf-8")
    request = Request(
        KOFIA_URL,
        data=body,
        method="POST",
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json; charset=UTF-8",
            "User-Agent": "StockSignalDashboard/0.5.6",
        },
    )
    raw_body, content_type = _request_bytes(request)
    text = raw_body.decode("utf-8-sig")
    if "html" in content_type or not text.lstrip().startswith("{"):
        raise ValueError("KOFIA response is not JSON")
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError("KOFIA response is invalid JSON") from exc
    item = build_korea_item(parse_kofia_payload(payload))
    return _validated_items([item], ("kr_margin_financing",))[0]


def _generated_at(epoch: float) -> str:
    return datetime.fromtimestamp(epoch).astimezone().strftime("%Y-%m-%d %H:%M:%S")


def _load_disk_record() -> dict[str, Any] | None:
    try:
        raw = json.loads(_CACHE_PATH.read_text(encoding="utf-8"))
        if not isinstance(raw, Mapping) or raw.get("schema") != _CACHE_SCHEMA:
            return None
        epoch = _number(raw.get("generatedAtEpoch"))
        generated_at = raw.get("generatedAt")
        if epoch is None or epoch <= 0 or not isinstance(generated_at, str) or not generated_at.strip():
            return None
        items = _validated_items(raw.get("items"), _ITEM_ORDER)
        return {
            "generatedAt": generated_at,
            "generatedAtEpoch": epoch,
            "items": items,
            "errors": [],
        }
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return None


def _write_disk_record(record: Mapping[str, Any]) -> None:
    cache_path = Path(_CACHE_PATH)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema": _CACHE_SCHEMA,
        "generatedAt": record["generatedAt"],
        "generatedAtEpoch": record["generatedAtEpoch"],
        "items": _validated_items(record.get("items"), _ITEM_ORDER),
    }
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=cache_path.parent,
            prefix=f".{cache_path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
            temporary_path = Path(handle.name)
        os.replace(temporary_path, cache_path)
        temporary_path = None
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def _cached_record() -> dict[str, Any] | None:
    global _MEMORY_RECORD
    with _CACHE_LOCK:
        if _MEMORY_RECORD is not None:
            return _MEMORY_RECORD
    disk_record = _load_disk_record()
    if disk_record is None:
        return None
    with _CACHE_LOCK:
        if _MEMORY_RECORD is None:
            _MEMORY_RECORD = disk_record
        return _MEMORY_RECORD


def _source_error(source: str, exc: Exception) -> dict[str, str]:
    message = str(exc).strip() or exc.__class__.__name__
    return {"source": source, "message": message[:500]}


def _refresh_record() -> dict[str, Any]:
    global _MEMORY_RECORD
    with _REFRESH_LOCK:
        previous_record = _cached_record()
        previous_items = {
            str(item.get("key")): dict(item)
            for item in (previous_record or {}).get("items", [])
            if isinstance(item, Mapping)
        }
        fetched: dict[str, list[dict[str, Any]]] = {}
        failures: dict[str, Exception] = {}
        with ThreadPoolExecutor(max_workers=6, thread_name_prefix="macro-telemetry") as executor:
            futures = {
                executor.submit(_fetch_treasury_items): "treasury",
                executor.submit(_fetch_chinabond_items): "chinabond",
                executor.submit(_fetch_korea_item): "kofia",
                executor.submit(_fetch_gold_item): "gold",
                executor.submit(_fetch_china_gold_item): "cn_gold",
                executor.submit(_fetch_turkey_gold_item): "tr_gold",
            }
            for future in as_completed(futures):
                source_key = futures[future]
                try:
                    result = future.result()
                    fetched[source_key] = result if isinstance(result, list) else [result]
                except Exception as exc:
                    failures[source_key] = exc

        new_items: dict[str, dict[str, Any]] = {}
        errors: list[dict[str, str]] = []
        required_items = (
            ("treasury", ("us10y", "us30y")),
            ("chinabond", ("cn10y", "cn30y")),
            ("kofia", ("kr_margin_financing",)),
            ("gold", ("gold",)),
            ("cn_gold", ("cn_gold_reserves",)),
            ("tr_gold", ("tr_gold_reserves",)),
        )
        for source_key, keys in required_items:
            try:
                validated = _validated_items(fetched.get(source_key), keys)
                new_items.update((item["key"], item) for item in validated)
            except Exception as exc:
                failures.setdefault(source_key, exc)

        for source_key, source_name, keys in (
            ("treasury", "U.S. Treasury", ("us10y", "us30y")),
            (
                "chinabond",
                _ITEM_SPECS["cn10y"]["source"],
                ("cn10y", "cn30y"),
            ),
            ("kofia", "KOFIA FreeSIS", ("kr_margin_financing",)),
            ("gold", "Yahoo Finance", ("gold",)),
            (
                "cn_gold",
                _ITEM_SPECS["cn_gold_reserves"]["source"],
                ("cn_gold_reserves",),
            ),
            (
                "tr_gold",
                _ITEM_SPECS["tr_gold_reserves"]["source"],
                ("tr_gold_reserves",),
            ),
        ):
            failure = failures.get(source_key)
            if failure is None:
                continue
            errors.append(_source_error(source_name, failure))
            for key in keys:
                cached_item = previous_items.get(key)
                if cached_item is not None:
                    cached_item["stale"] = True
                    new_items[key] = _validated_item(cached_item, key)

        ordered_items = [new_items[key] for key in _ITEM_ORDER if key in new_items]
        now = time.time()
        record = {
            "generatedAt": _generated_at(now),
            "generatedAtEpoch": now,
            "items": ordered_items,
            "errors": errors,
        }
        with _CACHE_LOCK:
            _MEMORY_RECORD = record

        if not errors and len(ordered_items) == len(_ITEM_ORDER):
            try:
                _write_disk_record(record)
            except OSError as exc:
                record["errors"] = [_source_error("Macro telemetry cache", exc)]
        return record


def _refreshing() -> bool:
    with _CACHE_LOCK:
        return _REFRESH_THREAD is not None and _REFRESH_THREAD.is_alive()


def _payload(record: Mapping[str, Any], *, state: str) -> dict[str, Any]:
    epoch = _number(record.get("generatedAtEpoch")) or time.time()
    items = [dict(item) for item in record.get("items", []) if isinstance(item, Mapping)]
    errors = [dict(error) for error in record.get("errors", []) if isinstance(error, Mapping)]
    return {
        "ok": bool(items),
        "generatedAt": str(record.get("generatedAt") or _generated_at(epoch)),
        "items": items,
        "errors": errors,
        "cache": {
            "state": state,
            "ageSeconds": max(0, int(time.time() - epoch)),
            "refreshing": _refreshing(),
        },
    }


def _record_state(record: Mapping[str, Any]) -> str:
    items = record.get("items", [])
    if not items:
        return "empty"
    if record.get("errors") or any(bool(item.get("stale")) for item in items if isinstance(item, Mapping)):
        return "stale"
    return "fresh"


def _background_refresh() -> None:
    global _REFRESH_THREAD
    try:
        _refresh_record()
    finally:
        with _CACHE_LOCK:
            if _REFRESH_THREAD is threading.current_thread():
                _REFRESH_THREAD = None


def _start_background_refresh() -> None:
    global _REFRESH_THREAD
    with _CACHE_LOCK:
        if _REFRESH_THREAD is not None and _REFRESH_THREAD.is_alive():
            return
        _REFRESH_THREAD = threading.Thread(
            target=_background_refresh,
            name="macro-telemetry-refresh",
            daemon=True,
        )
        _REFRESH_THREAD.start()


def build_macro_telemetry_bundle(
    force_refresh: bool = False,
    allow_stale: bool = True,
) -> dict[str, Any]:
    """Return the isolated macro-telemetry payload with stale-while-revalidate caching."""

    record = _cached_record()
    if force_refresh or not allow_stale or record is None:
        refreshed = _refresh_record()
        return _payload(refreshed, state=_record_state(refreshed))

    epoch = _number(record.get("generatedAtEpoch")) or 0.0
    if time.time() - epoch < max(0, _CACHE_TTL_SECONDS):
        return _payload(record, state=_record_state(record))

    _start_background_refresh()
    return _payload(record, state="stale")


def _reset_cache_for_tests() -> None:
    """Reset only process-local cache state; never delete the configured cache file."""

    global _MEMORY_RECORD, _REFRESH_THREAD
    thread = None
    with _CACHE_LOCK:
        thread = _REFRESH_THREAD
    if thread is not None and thread.is_alive():
        thread.join(timeout=2)
    with _CACHE_LOCK:
        _MEMORY_RECORD = None
        _REFRESH_THREAD = None


def _wait_for_background_refresh_for_tests(timeout: float) -> None:
    with _CACHE_LOCK:
        thread = _REFRESH_THREAD
    if thread is not None:
        thread.join(timeout=timeout)
