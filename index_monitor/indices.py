from __future__ import annotations

import copy
import json
import calendar
import math
import threading
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import requests

from .index_signals import TIMEFRAME_FETCH_BARS, confirmation_contract_for_state, latest_indicator_metrics
from .providers import TENCENT_KLINE_URL, TENCENT_KLINE_FALLBACK_URL, fetch_tencent_quotes, _http_json, _int, _num
from .paths import DATA_DIR


YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
SINA_CN_KLINE_URL = "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData"
WIND_INDEX_API_BASE = "https://indexapi.wind.com.cn/indicesWebsite/api"
WIND_SMALL_CAP_INDEX_ID = "e2d5a98547c3ee7c923a0259cee963e4"
WIND_SMALL_CAP_CODE = "8841425.WI"
WIND_SMALL_CAP_DETAIL_URL = (
    "https://www.windindices.com/indices/zh/IndexF9/"
    f"{WIND_SMALL_CAP_INDEX_ID}"
)
LOOKBACK_MONTHS = 3
CN_HISTORY_DAYS = 320
INDEX_SIGNAL_ASSET_COUNT = 27
INDEX_ASSET_COUNT = 28
_INDEX_CACHE_SCHEMA_VERSION = 4
_INDEX_CACHE_PATH = DATA_DIR / "index_market_cache.json"
_INDEX_CACHE: Dict[str, Any] = {"loaded": False, "ts": 0.0, "payload": None, "lastError": None}
_INDEX_TTL_SECONDS = 300
_INDEX_CACHE_LOCK = threading.RLock()
_INDEX_REFRESH_LOCK = threading.Lock()
_INDEX_BACKGROUND_LOCK = threading.Lock()
_INDEX_BACKGROUND_REFRESHING = False
_SIGNAL_TIMEFRAME_CACHE: Dict[str, Dict[str, Any]] = {}
_SIGNAL_TIMEFRAME_CACHE_LOCK = threading.RLock()
_SIGNAL_TIMEFRAME_TTL_SECONDS = {"short": 300, "medium": 1800}


def _valid_index_cache_payload(payload: Any) -> bool:
    if (
        not isinstance(payload, dict)
        or payload.get("ok") is not True
        or not isinstance(payload.get("groups"), list)
        or payload.get("errors")
    ):
        return False

    expected_keys = {
        "US": {spec.key for spec in US_INDEXES},
        "CN": {spec.key for spec in CN_INDEXES + CN_REFERENCE_INDEXES},
        "HK": {spec.key for spec in HK_INDEXES},
        "JP_KR": {spec.key for spec in JP_KR_INDEXES},
    }
    groups_by_key: Dict[str, Dict[str, Any]] = {}
    all_items: List[Dict[str, Any]] = []
    for group in payload["groups"]:
        if not isinstance(group, dict) or not isinstance(group.get("items"), list):
            return False
        group_key = str(group.get("key") or "")
        if group_key not in expected_keys or group_key in groups_by_key:
            return False
        items = group["items"]
        item_keys = [str(item.get("key") or "") for item in items if isinstance(item, dict)]
        if len(item_keys) != len(items) or len(item_keys) != len(set(item_keys)):
            return False
        if set(item_keys) != expected_keys[group_key]:
            return False
        groups_by_key[group_key] = group
        all_items.extend(items)

    if set(groups_by_key) != set(expected_keys) or len(all_items) != INDEX_ASSET_COUNT:
        return False
    reference_items = [item for item in all_items if item.get("key") == "wi8841425"]
    if len(reference_items) != 1 or reference_items[0].get("signalEligible") is not False:
        return False
    return all(
        item.get("signalEligible") is not False
        for item in all_items
        if item.get("key") != "wi8841425"
    )


def _cache_result(
    payload: Dict[str, Any],
    *,
    state: str,
    age_seconds: float,
    refreshing: bool,
) -> Dict[str, Any]:
    result = copy.deepcopy(payload)
    result["cache"] = {
        "state": state,
        "ageSeconds": max(0, int(age_seconds)),
        "refreshing": bool(refreshing),
    }
    return result


def _persist_index_cache(payload: Dict[str, Any], saved_at: Optional[float] = None) -> bool:
    if not _valid_index_cache_payload(payload):
        return False
    timestamp = float(saved_at if saved_at is not None else time.time())
    record = {
        "schemaVersion": _INDEX_CACHE_SCHEMA_VERSION,
        "savedAt": timestamp,
        "payload": copy.deepcopy(payload),
    }
    temp_path = _INDEX_CACHE_PATH.with_name(
        f"{_INDEX_CACHE_PATH.name}.{threading.get_ident()}.tmp"
    )
    try:
        _INDEX_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        temp_path.write_text(
            json.dumps(record, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8",
        )
        temp_path.replace(_INDEX_CACHE_PATH)
        return True
    except OSError:
        try:
            temp_path.unlink(missing_ok=True)
        except OSError:
            pass
        return False


def _restore_index_cache() -> bool:
    restored = False
    timestamp = 0.0
    payload: Optional[Dict[str, Any]] = None
    try:
        record = json.loads(_INDEX_CACHE_PATH.read_text(encoding="utf-8"))
        if (
            isinstance(record, dict)
            and record.get("schemaVersion") == _INDEX_CACHE_SCHEMA_VERSION
            and _valid_index_cache_payload(record.get("payload"))
        ):
            timestamp = float(record.get("savedAt") or 0.0)
            payload = copy.deepcopy(record["payload"])
            restored = True
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        restored = False

    with _INDEX_CACHE_LOCK:
        _INDEX_CACHE["loaded"] = True
        if restored:
            _INDEX_CACHE["ts"] = timestamp
            _INDEX_CACHE["payload"] = payload
            _INDEX_CACHE["lastError"] = None
    return restored


def _ensure_index_cache_loaded() -> None:
    with _INDEX_CACHE_LOCK:
        loaded = bool(_INDEX_CACHE.get("loaded"))
    if not loaded:
        _restore_index_cache()


@dataclass(frozen=True)
class IndexSpec:
    key: str
    name: str
    region: str
    region_label: str
    secid: Optional[str] = None
    tencent_code: Optional[str] = None
    yahoo_symbol: Optional[str] = None
    market_timezone: str = "UTC"


US_INDEXES: List[IndexSpec] = [
    IndexSpec("spy", "SPY", "US", "美国市场", yahoo_symbol="SPY", market_timezone="America/New_York"),
    IndexSpec("dow", "Dow Jones", "US", "美国市场", yahoo_symbol="^DJI", market_timezone="America/New_York"),
    IndexSpec("qqq", "QQQ", "US", "美国市场", yahoo_symbol="QQQ", market_timezone="America/New_York"),
    IndexSpec("vix", "VIX（标普500波动率指数）", "US", "美国市场", yahoo_symbol="^VIX", market_timezone="America/New_York"),
    IndexSpec("apple", "Apple 苹果", "US", "美国市场", yahoo_symbol="AAPL", market_timezone="America/New_York"),
    IndexSpec("microsoft", "Microsoft 微软", "US", "美国市场", yahoo_symbol="MSFT", market_timezone="America/New_York"),
    IndexSpec("alphabet", "Alphabet 谷歌", "US", "美国市场", yahoo_symbol="GOOGL", market_timezone="America/New_York"),
    IndexSpec("amazon", "Amazon 亚马逊", "US", "美国市场", yahoo_symbol="AMZN", market_timezone="America/New_York"),
    IndexSpec("nvidia", "NVIDIA 英伟达", "US", "美国市场", yahoo_symbol="NVDA", market_timezone="America/New_York"),
    IndexSpec("meta", "Meta", "US", "美国市场", yahoo_symbol="META", market_timezone="America/New_York"),
    IndexSpec("tesla", "Tesla 特斯拉", "US", "美国市场", yahoo_symbol="TSLA", market_timezone="America/New_York"),
]

CN_INDEXES: List[IndexSpec] = [
    IndexSpec("sh000001", "上证指数", "CN", "中国市场", secid="1.000001", tencent_code="sh000001", yahoo_symbol="000001.SS", market_timezone="Asia/Shanghai"),
    IndexSpec("sz399001", "深证成指", "CN", "中国市场", secid="0.399001", tencent_code="sz399001", yahoo_symbol="399001.SZ", market_timezone="Asia/Shanghai"),
    IndexSpec("sh000300", "沪深300", "CN", "中国市场", secid="1.000300", tencent_code="sh000300", yahoo_symbol="000300.SS", market_timezone="Asia/Shanghai"),
    IndexSpec("sh000688", "科创50", "CN", "中国市场", secid="1.000688", tencent_code="sh000688", yahoo_symbol="000688.SS", market_timezone="Asia/Shanghai"),
    IndexSpec("sh000985", "中证全指", "CN", "中国市场", secid="1.000985", tencent_code="sh000985", yahoo_symbol="000985.SS", market_timezone="Asia/Shanghai"),
    IndexSpec("sh000905", "中证500", "CN", "中国市场", secid="1.000905", tencent_code="sh000905", yahoo_symbol="000905.SS", market_timezone="Asia/Shanghai"),
    IndexSpec("sh000852", "中证1000", "CN", "中国市场", secid="1.000852", tencent_code="sh000852", yahoo_symbol="000852.SS", market_timezone="Asia/Shanghai"),
    IndexSpec("sz399006", "创业板指", "CN", "中国市场", secid="0.399006", tencent_code="sz399006", yahoo_symbol="399006.SZ", market_timezone="Asia/Shanghai"),
    IndexSpec("sh000922", "中证红利", "CN", "中国市场", secid="1.000922", tencent_code="sh000922", yahoo_symbol="000922.SS", market_timezone="Asia/Shanghai"),
    IndexSpec("sh000510", "中证A500", "CN", "中国市场", secid="1.000510", tencent_code="sh000510", yahoo_symbol="000510.SS", market_timezone="Asia/Shanghai"),
]

# Display-only references are deliberately kept outside CN_INDEXES.  The
# signal scheduler builds its 27-asset universe from CN_INDEXES and therefore
# cannot silently treat this official website series as a fully provisioned
# 15m/60m/daily signal source.
CN_REFERENCE_INDEXES: List[IndexSpec] = [
    IndexSpec(
        "wi8841425",
        "万得小市值指数",
        "CN",
        "中国市场",
        market_timezone="Asia/Shanghai",
    ),
]

HK_INDEXES: List[IndexSpec] = [
    IndexSpec("hkHSI", "恒生指数", "HK", "香港市场", secid="100.HSI", tencent_code="hkHSI", yahoo_symbol="^HSI", market_timezone="Asia/Hong_Kong"),
    IndexSpec("hkHSTECH", "恒生科技指数", "HK", "香港市场", secid="124.HSTECH", tencent_code="hkHSTECH", yahoo_symbol="HSTECH.HK", market_timezone="Asia/Hong_Kong"),
]

JP_KR_INDEXES: List[IndexSpec] = [
    IndexSpec("nikkei225", "日经225", "JP_KR", "日韩市场", yahoo_symbol="^N225", market_timezone="Asia/Tokyo"),
    IndexSpec("kospi", "韩国综合指数", "JP_KR", "日韩市场", yahoo_symbol="^KS11", market_timezone="Asia/Seoul"),
    IndexSpec("sk_hynix", "SK海力士", "JP_KR", "日韩市场", yahoo_symbol="000660.KS", market_timezone="Asia/Seoul"),
    IndexSpec("samsung_electronics", "三星电子", "JP_KR", "日韩市场", yahoo_symbol="005930.KS", market_timezone="Asia/Seoul"),
]


def _headers(referer: str = "https://gu.qq.com/") -> Dict[str, str]:
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": referer,
        "Accept": "application/json,text/plain,*/*",
    }


def _plain_http_json(url: str, params: Dict[str, Any] | None = None, timeout: int = 14, retries: int = 3) -> Dict[str, Any]:
    query = urllib.parse.urlencode(params or {}, doseq=True)
    full_url = f"{url}?{query}" if query else url
    last_error: Optional[Exception] = None
    for i in range(retries):
        try:
            req = urllib.request.Request(full_url, headers=_headers("https://finance.yahoo.com/"), method="GET")
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
            return json.loads(raw)
        except Exception as exc:  # noqa: BLE001 - market data provider resilience
            last_error = exc
            time.sleep(0.3 + i * 0.25)
    raise RuntimeError(f"HTTP请求失败：{last_error}")


def _round(value: Optional[float], digits: int = 2) -> Optional[float]:
    normalized = _num(value)
    return round(normalized, digits) if normalized is not None else None


def _pct(current: Optional[float], base: Optional[float]) -> Optional[float]:
    current, base = _num(current), _num(base)
    if current is None or base is None or base <= 0:
        return None
    return (current / base - 1) * 100


def _make_history_bundle(
    rows: List[Dict[str, Any]],
    current_price: Optional[float],
    complete_close: bool,
    *,
    signal_rows: Optional[List[Dict[str, Any]]] = None,
    daily_close_finality: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Return display rows plus immutable completed rows for signal evaluation.

    ``rows`` remains the display series and may carry a same-day live quote on
    its final bar.  ``signalRows`` deliberately retains the unmodified provider
    close series.  When provider daily-close finality is pending, the terminal
    daily row is not eligible for an index signal calculation.
    """

    copied_rows = [dict(row) for row in rows]
    completed_rows = [dict(row) for row in (signal_rows if signal_rows is not None else rows)]
    if signal_rows is None and not complete_close and completed_rows:
        completed_rows.pop()
    normalized_price = _num(current_price)
    if copied_rows and normalized_price is not None:
        copied_rows[-1]["close"] = normalized_price
    if copied_rows and not complete_close:
        # The display row may carry a live quote, but any downstream caller
        # that accidentally passes it to the evaluator can still fail closed.
        # ``signalRows`` remains the canonical unmodified completed series.
        copied_rows[-1]["_signalEligible"] = False
    finality = dict(daily_close_finality or {})
    finality.setdefault("status", "complete" if complete_close else "pending")
    finality.setdefault("source", "unknown")
    finality.setdefault("reason", "provider_close_final" if complete_close else "provider_close_pending")
    finality.setdefault("providerAsOf", None)
    finality["completeClose"] = bool(complete_close)
    return {
        "rows": copied_rows,
        "signalRows": completed_rows,
        "latestDate": str(copied_rows[-1].get("date")) if copied_rows else None,
        "currentPrice": normalized_price,
        "completeClose": bool(complete_close),
        "dailyCloseFinality": finality,
        "rowCount": len(copied_rows),
        "signalRowCount": len(completed_rows),
    }


_YAHOO_FINAL_MARKET_STATES = frozenset({"CLOSED", "POST", "POSTPOST"})


def _market_now(spec: IndexSpec) -> datetime:
    try:
        return datetime.now(ZoneInfo(spec.market_timezone))
    except Exception:  # pragma: no cover - Windows without timezone database
        return datetime.now()


def _official_close_epoch(spec: IndexSpec, market_day: date) -> Optional[int]:
    close_hour, close_minute = {
        "CN": (15, 0),
        "HK": (16, 0),
        "JP_KR": (15, 30),
        "US": (16, 0),
    }.get(spec.region)
    if close_hour is None:
        return None
    try:
        timezone = ZoneInfo(spec.market_timezone)
        return int(datetime.combine(market_day, datetime.min.time(), tzinfo=timezone).replace(
            hour=close_hour,
            minute=close_minute,
        ).timestamp())
    except Exception:  # pragma: no cover - Windows without timezone database
        return None


def _daily_close_finality(
    spec: IndexSpec,
    rows: List[Dict[str, Any]],
    *,
    provider: str,
    market_state: Optional[str] = None,
    provider_as_of_epoch: Optional[int] = None,
    quote_date: Optional[str] = None,
    quote_price: Optional[float] = None,
) -> Dict[str, Any]:
    """Describe whether the latest daily provider close is safe for signals.

    This intentionally has no "clock passed close" branch.  A current-day
    Yahoo daily candle requires a final Yahoo market state.  The CN/HK
    Tencent quote + daily-history path requires quote timestamp/date/price
    agreement before the last close can become signal input.
    """

    provider_as_of = _int(provider_as_of_epoch)
    result: Dict[str, Any] = {
        "status": "pending",
        "source": str(provider or "unknown"),
        "reason": "missing_daily_rows",
        "providerAsOf": provider_as_of,
        "completeClose": False,
    }
    if not rows:
        return result
    try:
        latest_date = date.fromisoformat(str(rows[-1].get("date"))[:10])
    except (TypeError, ValueError):
        result["reason"] = "invalid_daily_date"
        return result

    market_today = _market_now(spec).date()
    if latest_date < market_today:
        result.update({"status": "complete", "reason": "historic_provider_row", "completeClose": True})
        return result
    if latest_date > market_today:
        result["reason"] = "future_dated_daily_row"
        return result

    normalized_provider = str(provider or "").lower()
    if normalized_provider == "yahoo":
        state = str(market_state or "").upper()
        if state in _YAHOO_FINAL_MARKET_STATES:
            result.update({"status": "complete", "reason": f"yahoo_market_state_{state.lower()}", "completeClose": True})
        else:
            result["reason"] = f"yahoo_market_state_{state.lower() or 'unknown'}"
        return result

    if normalized_provider in {"tencent"}:
        expected_close_epoch = _official_close_epoch(spec, latest_date)
        observed_quote_date = str(quote_date or "")[:10]
        latest_close = _num(rows[-1].get("close"))
        observed_quote_price = _num(quote_price)
        if provider_as_of is None:
            result["reason"] = "tencent_quote_timestamp_missing"
        elif expected_close_epoch is None or provider_as_of < expected_close_epoch:
            result["reason"] = "tencent_quote_before_official_close"
        elif observed_quote_date != latest_date.isoformat():
            result["reason"] = "tencent_quote_date_mismatch"
        elif latest_close is None or observed_quote_price is None:
            result["reason"] = "tencent_close_missing"
        elif abs(observed_quote_price - latest_close) > max(0.01, abs(latest_close) * 0.0005):
            result["reason"] = "tencent_close_mismatch"
        else:
            result.update({"status": "complete", "reason": "tencent_post_close_agreement", "completeClose": True})
        return result

    if normalized_provider in {"wind", "wind_web"}:
        expected_close_epoch = _official_close_epoch(spec, latest_date)
        observed_quote_date = str(quote_date or "")[:10]
        latest_close = _num(rows[-1].get("close"))
        observed_quote_price = _num(quote_price)
        if provider_as_of is None:
            result["reason"] = "wind_snapshot_timestamp_missing"
        elif expected_close_epoch is None or provider_as_of < expected_close_epoch:
            result["reason"] = "wind_snapshot_before_official_close"
        elif observed_quote_date != latest_date.isoformat():
            result["reason"] = "wind_snapshot_kline_date_mismatch"
        elif latest_close is None or observed_quote_price is None:
            result["reason"] = "wind_snapshot_kline_close_missing"
        elif abs(observed_quote_price - latest_close) > max(0.01, abs(latest_close) * 0.0005):
            result["reason"] = "wind_snapshot_kline_close_mismatch"
        else:
            result.update(
                {
                    "status": "complete",
                    "reason": "wind_snapshot_kline_post_close_agreement",
                    "completeClose": True,
                }
            )
        return result

    result["reason"] = "provider_finality_unknown"
    return result


def _market_history_complete(
    spec: IndexSpec,
    rows: List[Dict[str, Any]],
    market_state: Optional[str] = None,
    *,
    provider: str = "yahoo",
    provider_as_of_epoch: Optional[int] = None,
    quote_date: Optional[str] = None,
    quote_price: Optional[float] = None,
) -> bool:
    return bool(
        _daily_close_finality(
            spec,
            rows,
            provider=provider,
            market_state=market_state,
            provider_as_of_epoch=provider_as_of_epoch,
            quote_date=quote_date,
            quote_price=quote_price,
        ).get("completeClose")
    )


def _latest_row_is_market_today(spec: IndexSpec, rows: List[Dict[str, Any]]) -> bool:
    if not rows:
        return False
    try:
        latest_date = date.fromisoformat(str(rows[-1].get("date")))
    except (TypeError, ValueError):
        return False
    try:
        market_today = datetime.now(ZoneInfo(spec.market_timezone)).date()
    except Exception:  # pragma: no cover - Windows without timezone database
        market_today = date.today()
    return latest_date == market_today


def _market_date_from_timestamp(spec: IndexSpec, timestamp: Optional[int]) -> Optional[date]:
    if not timestamp:
        return None
    try:
        return datetime.fromtimestamp(timestamp, ZoneInfo(spec.market_timezone)).date()
    except Exception:  # pragma: no cover - Windows without timezone database
        return datetime.fromtimestamp(timestamp).date()


def _row_date(row: Dict[str, Any]) -> Optional[date]:
    try:
        return date.fromisoformat(str(row.get("date")))
    except (TypeError, ValueError):
        return None


def _reference_market_date(
    spec: IndexSpec,
    rows: List[Dict[str, Any]],
    reference_date: Optional[date] = None,
) -> date:
    if reference_date is not None:
        return reference_date
    for row in reversed(rows):
        parsed = _row_date(row)
        if parsed is not None:
            return parsed
    try:
        return datetime.now(ZoneInfo(spec.market_timezone)).date()
    except Exception:  # pragma: no cover - Windows without timezone database
        return date.today()


def _previous_close_for_price(
    spec: IndexSpec,
    rows: List[Dict[str, Any]],
    price_date: Optional[date],
) -> Optional[float]:
    dated_closes = [
        (parsed, close)
        for row in rows
        if (parsed := _row_date(row)) is not None and (close := _num(row.get("close"))) is not None
    ]
    if price_date is not None:
        prior_rows = [(row_date, close) for row_date, close in dated_closes if row_date < price_date]
        if prior_rows:
            return max(prior_rows, key=lambda item: item[0])[1]
        return None
    if not dated_closes:
        return None
    if _latest_row_is_market_today(spec, rows):
        return dated_closes[-2][1] if len(dated_closes) >= 2 else None
    return dated_closes[-1][1]


def _trailing_one_year_close(
    rows: List[Dict[str, Any]],
    reference_date: date,
) -> tuple[Optional[float], Optional[str]]:
    anniversary = _months_ago(reference_date, 12)
    candidates = [
        (parsed, close)
        for row in rows
        if (parsed := _row_date(row)) is not None
        and parsed <= anniversary
        and (close := _num(row.get("close"))) is not None
    ]
    if not candidates:
        return None, None
    baseline_date, baseline_close = max(candidates, key=lambda item: item[0])
    return baseline_close, baseline_date.isoformat()


def _months_ago(day: date, months: int) -> date:
    month = day.month - months
    year = day.year
    while month <= 0:
        month += 12
        year -= 1
    max_day = calendar.monthrange(year, month)[1]
    return date(year, month, min(day.day, max_day))


def _classify_drawdown(drawdown_pct: Optional[float]) -> str:
    if drawdown_pct is None:
        return "unknown"
    depth = abs(drawdown_pct)
    if depth <= 3:
        return "near-high"
    if depth <= 8:
        return "moderate"
    return "deep"


def _make_item(
    spec: IndexSpec,
    price: Optional[float],
    change: Optional[float],
    pct: Optional[float],
    rows: List[Dict[str, Any]],
    data_time: Optional[str],
    source: str,
    complete_close: bool = True,
    reference_date: Optional[date] = None,
    daily_close_finality: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    # Never write a stale quote over a daily row from a different market date.
    history_price = price if rows and reference_date is not None and _row_date(rows[-1]) == reference_date else None
    history_bundle = _make_history_bundle(
        rows,
        current_price=history_price,
        complete_close=complete_close,
        daily_close_finality=daily_close_finality,
    )
    history_bundle["currentPrice"] = _num(price)
    normalized_rows = history_bundle["rows"]
    market_reference_date = _reference_market_date(spec, normalized_rows, reference_date)
    cutoff = _months_ago(market_reference_date, LOOKBACK_MONTHS)
    dated_highs = [
        (parsed, row) for row in normalized_rows
        if (parsed := _row_date(row)) is not None
        and (high := _num(row.get("high"))) is not None and high > 0
        and parsed <= market_reference_date
    ]
    filtered = [row for parsed, row in dated_highs if cutoff <= parsed <= market_reference_date]
    history_start = min((parsed for parsed, _ in dated_highs), default=None)
    history_end = max((parsed for parsed, _ in dated_highs), default=None)
    window_coverage = (
        "empty" if not filtered else
        "covered" if history_start is not None and history_start <= cutoff
        and history_end is not None and history_end >= market_reference_date else "partial"
    )

    peak_row = max(filtered, key=lambda item: float(item.get("high") or 0), default=None)
    peak_high = float(peak_row.get("high")) if peak_row and peak_row.get("high") is not None else None
    peak_date = str(peak_row.get("date")) if peak_row else None
    peak_source = "daily_high" if peak_high is not None else None
    normalized_price = _num(price)
    # A dated quote is an observed price, not a zero-clamp. It can establish a
    # new same-day window high while the daily bar update is still lagging.
    if (peak_high is not None and normalized_price is not None and normalized_price > peak_high
            and reference_date is not None and rows and _row_date(rows[-1]) == reference_date
            and cutoff <= reference_date <= market_reference_date):
        peak_high = normalized_price
        peak_date = reference_date.isoformat()
        peak_source = "current_quote"
    drawdown_status = "empty" if peak_high is None else "available"
    if peak_high is not None and normalized_price is not None and normalized_price > peak_high:
        drawdown_status = "inconsistent_quote_history"
    drawdown_pct = _pct(normalized_price, peak_high) if drawdown_status == "available" else None
    drawdown_points = (normalized_price - peak_high) if normalized_price is not None and peak_high is not None and drawdown_status == "available" else None
    one_year_base_close, one_year_base_date = _trailing_one_year_close(normalized_rows, market_reference_date)
    one_year_change = price - one_year_base_close if price is not None and one_year_base_close is not None else None
    one_year_pct = _pct(price, one_year_base_close)

    return {
        "key": spec.key,
        "name": spec.name,
        "region": spec.region,
        "regionLabel": spec.region_label,
        "marketTimezone": spec.market_timezone,
        "source": source,
        "price": _round(price, 2),
        "change": _round(change, 2),
        "pct": _round(pct, 2),
        "oneYearChange": _round(one_year_change, 2),
        "oneYearPct": _round(one_year_pct, 2),
        "oneYearBaseDate": one_year_base_date,
        "peakHigh": _round(peak_high, 2),
        "peakDate": peak_date,
        "peakSource": peak_source,
        "drawdownStatus": drawdown_status,
        "drawdownPct": _round(drawdown_pct, 2),
        "drawdownPoints": _round(drawdown_points, 2),
        "drawdownClass": _classify_drawdown(drawdown_pct),
        "dataTime": data_time,
        "latestDate": history_bundle["latestDate"],
        "completeClose": history_bundle["completeClose"],
        "dailyCloseFinality": copy.deepcopy(history_bundle["dailyCloseFinality"]),
        "historyCount": len(filtered),
        "windowStart": cutoff.isoformat(),
        "windowEnd": market_reference_date.isoformat(),
        "windowCoverage": window_coverage,
        "historyStart": history_start.isoformat() if history_start else None,
        "historyEnd": history_end.isoformat() if history_end else None,
        "windowCoverageNote": "边界覆盖不代表逐交易日无缺口；不足时仅为已取得样本的高点，并非历史最高点",
        "_historyBundle": history_bundle,
    }


def _fetch_yahoo_index(spec: IndexSpec) -> Dict[str, Any]:
    if not spec.yahoo_symbol:
        raise ValueError(f"{spec.name}缺少Yahoo代码")
    encoded = urllib.parse.quote(spec.yahoo_symbol, safe="")
    data = _plain_http_json(
        YAHOO_CHART_URL.format(symbol=encoded),
        {"range": "2y", "interval": "1d"},
        timeout=16,
        retries=3,
    )
    result = (((data.get("chart") or {}).get("result") or [None])[0]) or {}
    meta = result.get("meta") or {}
    if str(meta.get("symbol") or "").upper() != str(spec.yahoo_symbol).upper():
        raise RuntimeError(f"{spec.name} Yahoo response symbol mismatch or unavailable")
    timestamps = result.get("timestamp") or []
    quote = (((result.get("indicators") or {}).get("quote") or [None])[0]) or {}
    highs = quote.get("high") or []
    closes = quote.get("close") or []
    opens = quote.get("open") or []
    lows = quote.get("low") or []
    volumes = quote.get("volume") or []

    try:
        market_timezone = ZoneInfo(spec.market_timezone)
    except Exception:  # pragma: no cover - Windows without timezone database
        market_timezone = None

    rows: List[Dict[str, Any]] = []
    for index, ts in enumerate(timestamps):
        open_ = opens[index] if index < len(opens) else None
        high = _num(highs[index] if index < len(highs) else None)
        low = _num(lows[index] if index < len(lows) else None)
        close = _num(closes[index] if index < len(closes) else None)
        volume = volumes[index] if index < len(volumes) else None
        if high is None or close is None or high <= 0 or close <= 0:
            continue
        timestamp = datetime.fromtimestamp(int(ts), market_timezone) if market_timezone else datetime.fromtimestamp(int(ts))
        rows.append(
            {
                "date": timestamp.date().isoformat(),
                "open": _num(open_),
                "high": _num(high),
                "low": _num(low),
                "close": _num(close),
                "volume": _num(volume),
            }
        )

    if not rows:
        raise RuntimeError(f"{spec.name} Yahoo daily history unavailable")
    price = _num(meta.get("regularMarketPrice"))
    if price is None and rows:
        price = _num(rows[-1].get("close"))
    latest_row_is_today = _latest_row_is_market_today(spec, rows)
    ts = _int(meta.get("regularMarketTime"))
    price_date = _market_date_from_timestamp(spec, ts)
    previous_close = _previous_close_for_price(spec, rows, price_date)
    if previous_close is None:
        previous_close = _num(meta.get("previousClose"))
    change = price - previous_close if price is not None and previous_close is not None else None
    pct = _pct(price, previous_close)
    data_time = datetime.fromtimestamp(ts, ZoneInfo(spec.market_timezone)).isoformat(timespec="seconds") if ts else None

    # Live-day fields may only update a bar that belongs to the current market date.
    if rows and price_date is not None and _row_date(rows[-1]) == price_date:
        day_high = _num(meta.get("regularMarketDayHigh"))
        if day_high is not None:
            rows[-1]["high"] = max(float(rows[-1].get("high") or day_high), day_high)
        market_volume = _num(meta.get("regularMarketVolume"))
        if market_volume is not None and rows[-1].get("volume") is None:
            rows[-1]["volume"] = market_volume
    daily_close_finality = _daily_close_finality(
        spec,
        rows,
        provider="yahoo",
        market_state=str(meta.get("marketState") or ""),
        provider_as_of_epoch=ts,
    )
    complete_close = bool(daily_close_finality.get("completeClose"))
    return _make_item(
        spec,
        price,
        change,
        pct,
        rows,
        data_time,
        "yahoo",
        complete_close,
        reference_date=price_date,
        daily_close_finality=daily_close_finality,
    )


def _interval_seconds(interval: str) -> int:
    return {"15m": 15 * 60, "60m": 60 * 60}.get(str(interval or ""), 0)


def _market_timezone_or_none(market_timezone: Optional[str]) -> ZoneInfo | None:
    try:
        return ZoneInfo(str(market_timezone or "UTC"))
    except Exception:  # pragma: no cover - Windows without timezone database
        return None


def _normalized_interval_local_label(row: Dict[str, Any], market_timezone: Optional[str]) -> str:
    """Return the canonical market-local timestamp used for sort/de-duplication."""

    label = str(row.get("date") or "").strip()
    try:
        parsed = datetime.fromisoformat(label.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return label
    timezone = _market_timezone_or_none(market_timezone)
    if parsed.tzinfo is not None and timezone is not None:
        parsed = parsed.astimezone(timezone)
    return parsed.replace(tzinfo=None).isoformat(timespec="microseconds")


def _interval_row_chronology_key(
    row: Dict[str, Any],
    original_index: int,
    market_timezone: Optional[str] = None,
) -> tuple[int, str, str, int]:
    """Return a stable market-local chronology key for an interval row."""

    start_epoch = _int(row.get("barStartEpoch"))
    label = str(row.get("date") or "").strip()
    if start_epoch is None:
        try:
            parsed = datetime.fromisoformat(label.replace("Z", "+00:00"))
        except (TypeError, ValueError):
            parsed = None
        if parsed is not None:
            timezone = _market_timezone_or_none(market_timezone)
            if parsed.tzinfo is None and timezone is not None:
                parsed = parsed.replace(tzinfo=timezone)
            elif parsed.tzinfo is not None and timezone is not None:
                parsed = parsed.astimezone(timezone)
            if parsed.tzinfo is not None:
                start_epoch = int(parsed.timestamp())
    if start_epoch is not None:
        # Original provider order resolves same-slot duplicate revisions; raw
        # offset text must not reorder equal local candles.
        return (0, f"{start_epoch:020d}", "", original_index)
    return (1, "unparsed", label, original_index)


def _normalize_interval_rows(
    rows: List[Dict[str, Any]],
    *,
    count: int,
    market_timezone: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Copy, market-local chronological-sort, de-duplicate, then retain bars."""

    copied = [dict(row) for row in (rows or []) if isinstance(row, dict)]
    ordered = [
        row
        for _index, row in sorted(
            enumerate(copied),
            key=lambda item: _interval_row_chronology_key(item[1], item[0], market_timezone),
        )
    ]
    deduplicated: Dict[str, Dict[str, Any]] = {}
    for row in ordered:
        key = _normalized_interval_local_label(row, market_timezone)
        if not key:
            key = f"__unparsed__:{len(deduplicated)}:{row.get('date')}"
        # Keep the later provider update deterministically for duplicate slots.
        deduplicated[key] = row
    return list(deduplicated.values())[-max(int(count), 1) :]


def _fallback_interval_row(
    spec: IndexSpec,
    row: Dict[str, Any],
    *,
    interval: str,
    provider: str,
) -> Dict[str, Any]:
    """Annotate a fallback row without inventing current-session close proof."""

    copied = dict(row)
    label = str(copied.get("date") or "").strip()
    try:
        parsed = datetime.fromisoformat(label.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        parsed = None
    timezone = _market_timezone_or_none(spec.market_timezone)
    if parsed is not None and parsed.tzinfo is None and timezone is not None:
        parsed = parsed.replace(tzinfo=timezone)
    elif parsed is not None and parsed.tzinfo is not None and timezone is not None:
        parsed = parsed.astimezone(timezone)
    label_epoch = int(parsed.timestamp()) if parsed is not None else None
    if provider == "sina" and label_epoch is not None:
        end_epoch = label_epoch
        start_epoch = end_epoch - _interval_seconds(interval)
        copied["date"] = datetime.fromtimestamp(start_epoch, timezone).isoformat(timespec="minutes")
    else:
        start_epoch = label_epoch
        end_epoch = start_epoch + _interval_seconds(interval) if start_epoch is not None else None
    row_date = parsed.date() if parsed is not None else None
    # A current-date fallback row lacks provider close proof and must fail
    # closed, regardless of the machine's local time of day.
    is_closed = bool(row_date is not None and row_date < _market_now(spec).date())
    copied.update(
        {
            "barStartEpoch": start_epoch,
            "barEndEpoch": end_epoch,
            "providerAsOfEpoch": None,
            "isClosed": is_closed,
            "dataAgeSeconds": None,
            "provider": provider,
        }
    )
    return copied


def _fetch_yahoo_interval_rows(
    spec: IndexSpec,
    *,
    interval: str,
    range_value: str,
    count: int,
) -> List[Dict[str, Any]]:
    """Fetch completed Yahoo interval bars with provider finality evidence.

    Yahoo returns the active terminal candle in the same array as completed
    history.  It is excluded unless a later timestamp or explicit provider
    evidence proves that the candle is complete.
    """

    if not spec.yahoo_symbol:
        raise ValueError(f"{spec.name} missing Yahoo symbol")
    encoded = urllib.parse.quote(spec.yahoo_symbol, safe="")
    data = _plain_http_json(
        YAHOO_CHART_URL.format(symbol=encoded),
        {"range": range_value, "interval": interval, "includePrePost": "false"},
        timeout=16,
        retries=3,
    )
    result = (((data.get("chart") or {}).get("result") or [None])[0]) or {}
    meta = result.get("meta") or {}
    if str(meta.get("symbol") or "").upper() != str(spec.yahoo_symbol).upper():
        raise RuntimeError(f"{spec.name} Yahoo response symbol mismatch or unavailable")
    timestamps = result.get("timestamp") or []
    quote = (((result.get("indicators") or {}).get("quote") or [None])[0]) or {}
    opens = quote.get("open") or []
    highs = quote.get("high") or []
    lows = quote.get("low") or []
    closes = quote.get("close") or []
    volumes = quote.get("volume") or []
    market_timezone = _market_timezone_or_none(spec.market_timezone)
    provider_as_of = _int(meta.get("regularMarketTime"))
    market_state = str(meta.get("marketState") or "").upper()
    regular = ((meta.get("currentTradingPeriod") or {}).get("regular") or {})
    regular_start = _int(regular.get("start"))
    regular_end = _int(regular.get("end"))
    seconds = _interval_seconds(interval)
    if seconds <= 0:
        raise ValueError(f"unsupported Yahoo interval: {interval}")

    rows: List[Dict[str, Any]] = []
    for index, raw_timestamp in enumerate(timestamps):
        high = _num(highs[index] if index < len(highs) else None)
        low = _num(lows[index] if index < len(lows) else None)
        close = _num(closes[index] if index < len(closes) else None)
        if high is None or low is None or close is None:
            continue
        start_epoch = int(raw_timestamp)
        # Yahoo may append a marker at the regular close.  It is not a normal
        # interval bar and must never become a terminal signal candle.
        if regular_end is not None and start_epoch >= regular_end:
            continue
        timestamp = (
            datetime.fromtimestamp(start_epoch, market_timezone)
            if market_timezone
            else datetime.fromtimestamp(start_epoch)
        )
        end_epoch = start_epoch + seconds
        if regular_start is not None and regular_end is not None and regular_start <= start_epoch < regular_end:
            end_epoch = min(end_epoch, regular_end)
        rows.append(
            {
                "date": timestamp.isoformat(timespec="minutes"),
                "open": _num(opens[index] if index < len(opens) else close),
                "high": high,
                "low": low,
                "close": close,
                "volume": _num(volumes[index] if index < len(volumes) else None),
                "barStartEpoch": start_epoch,
                "barEndEpoch": end_epoch,
                "providerAsOfEpoch": provider_as_of,
                "isClosed": False,
                "dataAgeSeconds": max(0, provider_as_of - end_epoch) if provider_as_of is not None else None,
                "provider": "yahoo",
            }
        )

    normalized = _normalize_interval_rows(
        rows,
        count=max(len(rows), 1),
        market_timezone=spec.market_timezone,
    )
    completed: List[Dict[str, Any]] = []
    for index, row in enumerate(normalized):
        end_epoch = _int(row.get("barEndEpoch"))
        if end_epoch is None:
            continue
        later_start = _int(normalized[index + 1].get("barStartEpoch")) if index + 1 < len(normalized) else None
        if later_start is not None and later_start >= end_epoch:
            closed = True
        else:
            # The terminal candle needs provider-side proof.  Do not replace
            # this with a local wall-clock condition: delayed Yahoo updates are
            # precisely the state this guard prevents from entering a signal.
            closed_by_asof = provider_as_of is not None and provider_as_of >= end_epoch + 60
            closed_by_final_state = (
                market_state in _YAHOO_FINAL_MARKET_STATES
                and regular_end is not None
                and end_epoch <= regular_end
                and (provider_as_of is None or provider_as_of >= regular_end)
            )
            closed = bool(closed_by_asof or closed_by_final_state)
        row["isClosed"] = closed
        if closed:
            completed.append(row)

    selected = _normalize_interval_rows(completed, count=count, market_timezone=spec.market_timezone)
    if not selected:
        raise RuntimeError(f"{spec.name} {interval} awaiting_completed_bar")
    return selected

def _fetch_sina_cn_interval_rows(
    spec: IndexSpec,
    *,
    interval: str,
    count: int,
) -> List[Dict[str, Any]]:
    """Fetch verified CN index 15/60-minute history from Sina Finance.

    The endpoint accepts the same ``sh000001``/``sz399001`` symbols already
    present in :class:`IndexSpec`.  It avoids Tencent's version-only minute
    response and remains available for explicit no-Yahoo/fallback specs.
    All CN frames use this source, including the regional benchmark. Sina
    timestamps label interval ends; they are converted to interval starts.
    A later provider bar proves the preceding bar complete. The terminal
    current-day bar remains provisional without an independent close marker.
    """

    if not spec.tencent_code:
        raise ValueError(f"{spec.name}缺少新浪分钟K线代码")
    normalized_interval = str(interval or "").strip()
    scale = {"15m": 15, "60m": 60}.get(normalized_interval)
    if scale is None:
        raise ValueError(f"不支持的指数分钟周期：{normalized_interval}")
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            response = requests.get(
                SINA_CN_KLINE_URL,
                params={"symbol": spec.tencent_code, "scale": scale, "ma": "no", "datalen": max(int(count), 1)},
                headers=_headers("https://finance.sina.com.cn/"),
                timeout=16,
            )
            response.raise_for_status()
            payload = response.json()
            raw_rows = payload if isinstance(payload, list) else []
            rows: List[Dict[str, Any]] = []
            for raw in raw_rows:
                if not isinstance(raw, dict):
                    continue
                open_ = _num(raw.get("open"))
                high = _num(raw.get("high"))
                low = _num(raw.get("low"))
                close = _num(raw.get("close"))
                if None in (open_, high, low, close):
                    continue
                rows.append(
                    _fallback_interval_row(
                        spec,
                        {
                            "date": str(raw.get("day") or ""),
                            "open": open_,
                            "high": high,
                            "low": low,
                            "close": close,
                            "volume": _num(raw.get("volume")),
                        },
                        interval=normalized_interval,
                        provider="sina",
                    )
                )
            if rows:
                normalized = _normalize_interval_rows(rows, count=count, market_timezone=spec.market_timezone)
                for index, row in enumerate(normalized[:-1]):
                    end_epoch = _int(row.get("barEndEpoch"))
                    next_start = _int(normalized[index + 1].get("barStartEpoch"))
                    if end_epoch is not None and next_start is not None and next_start >= end_epoch:
                        row["isClosed"] = True
                return normalized
            last_error = RuntimeError("未返回K线")
        except (requests.RequestException, TypeError, ValueError) as exc:
            last_error = exc
        if attempt < 2:
            time.sleep(0.35 + attempt * 0.25)
    raise RuntimeError(f"{spec.name} {normalized_interval} 分钟K线为空：{last_error or '未返回K线'}")


def _fetch_cn_quotes() -> Dict[str, Dict[str, Any]]:
    return fetch_tencent_quotes([spec.tencent_code for spec in CN_INDEXES if spec.tencent_code])


def _fetch_hk_quotes() -> Dict[str, Dict[str, Any]]:
    return fetch_tencent_quotes([spec.tencent_code for spec in HK_INDEXES if spec.tencent_code])


def _fetch_tencent_index_rows(
    spec: IndexSpec,
    *,
    interval: str = "day",
    count: int = CN_HISTORY_DAYS,
) -> List[Dict[str, Any]]:
    if not spec.tencent_code:
        raise ValueError(f"{spec.name} missing Tencent code")
    if interval != "day":
        raise ValueError("Tencent index adapter supports daily bars only; minute bars are never replaced with daily bars")
    param = f"{spec.tencent_code},day,,,{max(int(count), 1)},qfq"
    errors = []
    for endpoint in (TENCENT_KLINE_URL, TENCENT_KLINE_FALLBACK_URL):
        try:
            data = _http_json(endpoint, {"param": param}, timeout=12, retries=1)
            payload = ((data.get("data") or {}).get(spec.tencent_code) or {})
            returned_quote = (payload.get("qt") or {}).get(spec.tencent_code)
            if returned_quote and (len(returned_quote) < 3 or str(returned_quote[2]).upper() != spec.tencent_code[2:].upper()):
                raise RuntimeError("Tencent history symbol mismatch")
            raw_rows = payload.get("day") or payload.get("qfqday") or []
            rows_by_date: Dict[str, Dict[str, Any]] = {}
            for item in raw_rows:
                if not isinstance(item, list) or len(item) < 5:
                    continue
                try:
                    label = date.fromisoformat(str(item[0])).isoformat()
                except ValueError:
                    continue
                open_, close, high, low = (_num(value) for value in item[1:5])
                if any(value is None or value <= 0 for value in (open_, close, high, low)):
                    continue
                if high < max(open_, close, low) or low > min(open_, close, high):
                    continue
                rows_by_date[label] = {
                    "date": label, "open": open_, "close": close, "high": high, "low": low,
                    "volume": _num(item[5]) if len(item) > 5 else None,
                }
            if rows_by_date:
                return [rows_by_date[label] for label in sorted(rows_by_date)][-max(int(count), 1):]
            raise RuntimeError("no valid daily bars for requested index")
        except Exception as exc:
            errors.append(str(exc))
    raise RuntimeError(f"{spec.name} Tencent daily history unavailable: {'; '.join(errors)}")


def _completed_interval_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return only rows carrying explicit provider completion evidence."""

    return [dict(row) for row in (rows or []) if isinstance(row, dict) and row.get("isClosed") is True]


def _cached_signal_rows(spec: IndexSpec, timeframe: str) -> List[Dict[str, Any]] | None:
    key = f"{spec.key}|{timeframe}"
    with _SIGNAL_TIMEFRAME_CACHE_LOCK:
        cached = _SIGNAL_TIMEFRAME_CACHE.get(key)
        if not isinstance(cached, dict):
            return None
        ttl = _SIGNAL_TIMEFRAME_TTL_SECONDS.get(timeframe, 0)
        if time.monotonic() - float(cached.get("fetchedAt") or 0) > ttl:
            return None
        rows = cached.get("rows")
        required_source_bars = TIMEFRAME_FETCH_BARS.get(timeframe, 1)
        if not isinstance(rows, list) or len(rows) < required_source_bars:
            return None
        normalized = _normalize_interval_rows(
            rows,
            count=required_source_bars,
            market_timezone=spec.market_timezone,
        )
        if len(normalized) < required_source_bars or any(row.get("isClosed") is not True for row in normalized):
            return None
        return normalized


def _latest_trading_day(rows: List[Dict[str, Any]]) -> str:
    for row in reversed(rows or []):
        label = str(row.get("date") or "").strip() if isinstance(row, dict) else ""
        if label:
            return label[:10]
    return ""


def _validate_intraday_trading_day(
    *,
    spec: IndexSpec,
    interval: str,
    rows: List[Dict[str, Any]],
    daily_rows: List[Dict[str, Any]],
) -> None:
    if not rows or any(row.get("isClosed") is not True for row in rows):
        raise RuntimeError(f"{spec.key} {interval} awaiting_completed_bar")
    expected_day = _latest_trading_day(daily_rows)
    actual_day = _latest_trading_day(rows)
    if expected_day and actual_day != expected_day:
        raise RuntimeError(
            f"{spec.key} {interval} intraday trading-day mismatch: {actual_day or '--'} != {expected_day}"
        )


def _discard_cached_signal_rows(spec: IndexSpec, timeframe: str) -> None:
    with _SIGNAL_TIMEFRAME_CACHE_LOCK:
        _SIGNAL_TIMEFRAME_CACHE.pop(f"{spec.key}|{timeframe}", None)


def _store_signal_rows(spec: IndexSpec, timeframe: str, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    required_source_bars = TIMEFRAME_FETCH_BARS.get(timeframe, 1)
    copied = _normalize_interval_rows(
        rows,
        count=required_source_bars,
        market_timezone=spec.market_timezone,
    )
    completed = _completed_interval_rows(copied)
    # A partial or provisional response must never be pinned for the cache TTL:
    # it would suppress the next provider check precisely when a close becomes
    # available.  This also retains the Task2 600/400 source-history contract.
    if len(completed) != len(copied) or len(completed) < required_source_bars:
        return completed
    key = f"{spec.key}|{timeframe}"
    with _SIGNAL_TIMEFRAME_CACHE_LOCK:
        _SIGNAL_TIMEFRAME_CACHE[key] = {"fetchedAt": time.monotonic(), "rows": completed}
    return [dict(row) for row in completed]


def _timeframe_quality(*, status: str, reason: str, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {"status": status, "reason": reason, "rowCount": len(rows or [])}


def get_index_signal_timeframes(
    spec: IndexSpec,
    *,
    daily_rows: List[Dict[str, Any]],
    use_cache: bool = True,
) -> Dict[str, Any]:
    """Return independent 15m/60m/daily bars plus private per-frame quality.

    A failed or unfinished 60m retrieval must not erase a valid 15m frame (or
    vice versa).  Existing callers can ignore ``_quality`` and continue to read
    the named frames only.
    """

    long_rows = [dict(row) for row in (daily_rows or [])]
    result: Dict[str, Any] = {
        "long": long_rows,
        "_quality": {
            "long": _timeframe_quality(
                status="available" if long_rows else "unavailable",
                reason="daily_rows_supplied" if long_rows else "daily_rows_missing",
                rows=long_rows,
            )
        },
    }
    request_plan = {
        # Retain independent source history for same-local-clock-slot RVOL;
        # this is intentionally wider than the 200-bar shape window.
        "short": ("15m", "60d", TIMEFRAME_FETCH_BARS["short"]),
        "medium": ("60m", "730d", TIMEFRAME_FETCH_BARS["medium"]),
    }
    expected_day = _latest_trading_day(daily_rows)
    for timeframe, (interval, yahoo_range, count) in request_plan.items():
        try:
            if use_cache and (cached := _cached_signal_rows(spec, timeframe)) is not None:
                _validate_intraday_trading_day(
                    spec=spec,
                    interval=interval,
                    rows=cached,
                    daily_rows=daily_rows,
                )
                result[timeframe] = cached
                result["_quality"][timeframe] = _timeframe_quality(
                    status="available",
                    reason="cached_completed_frame",
                    rows=cached,
                )
                continue

            if spec.region.upper() == "CN":
                rows = _fetch_sina_cn_interval_rows(spec, interval=interval, count=count)
            elif spec.yahoo_symbol:
                rows = _fetch_yahoo_interval_rows(
                    spec, interval=interval, range_value=yahoo_range, count=count,
                )
            else:
                raise RuntimeError(f"{spec.key} {interval} no supported minute-data source")

            # Normalize even monkeypatched/fallback provider output before we
            # validate, slice, or cache it.  Same-slot RVOL requires a stable
            # market-local sequence independent of delivery order/offset form.
            normalized = _normalize_interval_rows(rows, count=count, market_timezone=spec.market_timezone)
            completed = _completed_interval_rows(normalized)
            explicitly_provisional = [row for row in normalized if row.get("isClosed") is not True]
            if not completed:
                raise RuntimeError(f"{spec.key} {interval} awaiting_completed_bar")
            _validate_intraday_trading_day(
                spec=spec,
                interval=interval,
                rows=completed,
                daily_rows=daily_rows,
            )
            result_rows = (
                _store_signal_rows(spec, timeframe, completed)
                if use_cache and len(completed) >= TIMEFRAME_FETCH_BARS[timeframe] and not explicitly_provisional
                else completed
            )
            result[timeframe] = result_rows
            result["_quality"][timeframe] = _timeframe_quality(
                status="available" if len(result_rows) >= TIMEFRAME_FETCH_BARS[timeframe] else "partial",
                reason="provider_completed_frame" if not explicitly_provisional else "provisional_terminal_excluded",
                rows=result_rows,
            )
        except Exception as exc:  # noqa: BLE001 - frame-level failure isolation
            _discard_cached_signal_rows(spec, timeframe)
            message = str(exc)
            result[timeframe] = []
            result["_quality"][timeframe] = _timeframe_quality(
                status="awaiting_completed_bar" if "awaiting_completed_bar" in message else "unavailable",
                reason=message,
                rows=[],
            )
    return result

def _fetch_tencent_index(spec: IndexSpec, quote_map: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    raw = quote_map.get(spec.tencent_code or "")
    if not raw:
        raise RuntimeError(f"{spec.name}没有取到实时行情")
    rows = _fetch_tencent_index_rows(spec)
    price = _num(raw.get("price"))
    change = _num(raw.get("change"))
    pct = _num(raw.get("pct"))
    ts = _int(raw.get("quoteTimestamp"))
    data_time = datetime.fromtimestamp(ts, ZoneInfo(spec.market_timezone)).isoformat(timespec="seconds") if ts else None
    price_date = _market_date_from_timestamp(spec, ts)
    if rows and price_date is not None and _row_date(rows[-1]) == price_date:
        day_high = _num(raw.get("high"))
        if day_high is not None:
            rows[-1]["high"] = max(float(rows[-1].get("high") or day_high), day_high)
    daily_close_finality = _daily_close_finality(
        spec,
        rows,
        provider="tencent",
        provider_as_of_epoch=ts,
        quote_date=price_date.isoformat() if price_date else None,
        quote_price=price,
    )
    complete_close = bool(daily_close_finality.get("completeClose"))
    return _make_item(
        spec,
        price,
        change,
        pct,
        rows,
        data_time,
        "tencent",
        complete_close,
        reference_date=price_date,
        daily_close_finality=daily_close_finality,
    )


def _fetch_cn_index(spec: IndexSpec, quote_map: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    return _fetch_tencent_index(spec, quote_map)


def _fetch_hk_index(spec: IndexSpec, quote_map: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    return _fetch_tencent_index(spec, quote_map)


def _wind_api_result(endpoint: str, params: Dict[str, Any]) -> Any:
    """Read one public response used by the official Wind index website."""

    url = f"{WIND_INDEX_API_BASE}/{endpoint.lstrip('/')}"
    last_error: Optional[Exception] = None
    for attempt in range(2):
        try:
            response = requests.get(
                url,
                params=params,
                headers=_headers(WIND_SMALL_CAP_DETAIL_URL),
                timeout=10,
            )
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict) or payload.get("Success") is not True:
                message = payload.get("Message") if isinstance(payload, dict) else "响应格式错误"
                raise RuntimeError(str(message or "请求失败"))
            return payload.get("Result")
        except (requests.RequestException, TypeError, ValueError, RuntimeError) as exc:
            last_error = exc
            if attempt < 1:
                time.sleep(0.35)
    raise RuntimeError(f"Wind官网 {endpoint} 请求失败：{last_error}")


def _wind_date_label(value: Any) -> Optional[str]:
    raw = str(value or "").strip()
    if not raw:
        return None
    if len(raw) >= 10 and raw[4:5] == "-" and raw[7:8] == "-":
        raw = raw[:10]
        try:
            return date.fromisoformat(raw).isoformat()
        except ValueError:
            return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if not math.isfinite(float(value)):
            return None
        raw = str(int(value))
    if len(raw) != 8 or not raw.isdigit():
        return None
    try:
        return datetime.strptime(raw, "%Y%m%d").date().isoformat()
    except ValueError:
        return None


def _wind_time_label(value: Any) -> Optional[str]:
    raw = str(value or "").strip()
    if not raw:
        return None
    if ":" in raw:
        parts = raw.split(":")
        if len(parts) in {2, 3} and all(part.isdigit() for part in parts):
            hour, minute = int(parts[0]), int(parts[1])
            second = int(parts[2]) if len(parts) == 3 else 0
            if 0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= second <= 59:
                return f"{hour:02d}:{minute:02d}:{second:02d}"
        return None
    normalized = _int(value)
    if normalized is None or normalized < 0:
        return None
    digits = f"{normalized:06d}"
    if len(digits) != 6:
        return None
    hour, minute, second = int(digits[:2]), int(digits[2:4]), int(digits[4:6])
    if hour > 23 or minute > 59 or second > 59:
        return None
    return f"{hour:02d}:{minute:02d}:{second:02d}"


def _wind_provider_timestamp(spec: IndexSpec, date_label: Optional[str], time_label: Optional[str]) -> Optional[int]:
    if not date_label or not time_label:
        return None
    try:
        timestamp = datetime.fromisoformat(f"{date_label}T{time_label}")
        return int(timestamp.replace(tzinfo=ZoneInfo(spec.market_timezone)).timestamp())
    except Exception:  # pragma: no cover - Windows without timezone database
        return None


def _wind_kline_rows(result: Any) -> List[Dict[str, Any]]:
    raw_rows = result.get("data") if isinstance(result, dict) else []
    rows_by_date: Dict[str, Dict[str, Any]] = {}
    for raw in raw_rows or []:
        if not isinstance(raw, dict):
            continue
        date_label = _wind_date_label(raw.get("tradeDate"))
        open_ = _num(raw.get("open"))
        close = _num(raw.get("close"))
        high = _num(raw.get("hight"))
        low = _num(raw.get("low"))
        prices = (open_, high, low, close)
        if (
            not date_label
            or any(value is None or not math.isfinite(value) or value <= 0 for value in prices)
        ):
            continue
        volume = _num(raw.get("volume"))
        if volume is not None and (not math.isfinite(volume) or volume < 0):
            volume = None
        rows_by_date[date_label] = {
            "date": date_label,
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
        }
    return [rows_by_date[key] for key in sorted(rows_by_date)]


def _wind_trailing_year_pct(result: Any) -> Optional[float]:
    for raw in result if isinstance(result, list) else []:
        if isinstance(raw, dict) and str(raw.get("windCode") or "").upper() == WIND_SMALL_CAP_CODE:
            return _num(raw.get("year1"))
    return None


def _fetch_wind_reference_index(spec: IndexSpec) -> Dict[str, Any]:
    request_plan = {
        "snapshot": ("snapshot", {"indexId": WIND_SMALL_CAP_INDEX_ID, "lan": "cn"}),
        "kline": ("Kline", {"indexId": WIND_SMALL_CAP_INDEX_ID, "period": "1Y", "lan": "cn"}),
        "return": ("indexreturn", {"indexid": WIND_SMALL_CAP_INDEX_ID, "lan": "cn"}),
    }
    results: Dict[str, Any] = {}
    request_errors: Dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=len(request_plan)) as pool:
        future_map = {
            pool.submit(_wind_api_result, endpoint, params): label
            for label, (endpoint, params) in request_plan.items()
        }
        for future in as_completed(future_map):
            label = future_map[future]
            try:
                results[label] = future.result()
            except Exception as exc:  # noqa: BLE001 - optional website facets fail independently
                request_errors[label] = str(exc)

    snapshot = results.get("snapshot")
    if not isinstance(snapshot, dict):
        raise RuntimeError(request_errors.get("snapshot") or "Wind官网快照为空")
    wind_code = str(snapshot.get("windCode") or "").upper()
    if wind_code != WIND_SMALL_CAP_CODE:
        raise RuntimeError(f"Wind官网代码不匹配：{wind_code}")

    raw_kline = results.get("kline")
    if not isinstance(raw_kline, dict):
        raise RuntimeError(request_errors.get("kline") or "Wind官网日K线为空")
    if str(raw_kline.get("indexId") or "").upper() != WIND_SMALL_CAP_CODE:
        raise RuntimeError("Wind官网日K线标识不匹配")
    rows = _wind_kline_rows(raw_kline)
    if not rows:
        raise RuntimeError(request_errors.get("kline") or "Wind官网日K线为空")
    price = _num(snapshot.get("new"))
    previous_close = _num(snapshot.get("preClose"))
    if price is None or not math.isfinite(price) or price <= 0:
        raise RuntimeError("Wind官网最新点位无效")
    if previous_close is not None and (not math.isfinite(previous_close) or previous_close <= 0):
        previous_close = None
    change = _num(snapshot.get("ud"))
    if change is None and price is not None and previous_close is not None:
        change = price - previous_close
    rate = _num(snapshot.get("udRate"))
    pct = rate * 100 if rate is not None and math.isfinite(rate) else _pct(price, previous_close)
    quote_date = _wind_date_label(snapshot.get("date"))
    quote_time = _wind_time_label(snapshot.get("time"))
    if not quote_date or not quote_time:
        raise RuntimeError("Wind官网快照时间无效")
    data_time = f"{quote_date} {quote_time}" if quote_date and quote_time else quote_date
    provider_as_of = _wind_provider_timestamp(spec, quote_date, quote_time)
    daily_close_finality = _daily_close_finality(
        spec,
        rows,
        provider="wind_web",
        provider_as_of_epoch=provider_as_of,
        quote_date=quote_date,
        quote_price=price,
    )
    try:
        reference_date = date.fromisoformat(quote_date) if quote_date else None
    except ValueError:
        reference_date = None
    item = _make_item(
        spec,
        price,
        change,
        pct,
        rows,
        data_time,
        "wind_web",
        bool(daily_close_finality.get("completeClose")),
        reference_date=reference_date,
        daily_close_finality=daily_close_finality,
    )

    trailing_year_pct = _wind_trailing_year_pct(results.get("return"))
    if (
        price is not None
        and trailing_year_pct is not None
        and math.isfinite(trailing_year_pct)
        and trailing_year_pct > -100
    ):
        trailing_year_base = price / (1 + trailing_year_pct / 100)
        item["oneYearPct"] = _round(trailing_year_pct, 2)
        item["oneYearChange"] = _round(price - trailing_year_base, 2)
        # The website supplies the interval return directly but not the exact
        # prior trading date selected by its calculation.  Do not label the
        # nominal anniversary as an observed baseline date.
        item["oneYearBaseDate"] = None
        item["oneYearMetricSource"] = "wind_indexreturn"
    else:
        request_errors.setdefault("return", "Wind官网近一年收益为空")

    optional_warnings = [
        f"{label}: {message}"
        for label, message in request_errors.items()
        if label != "snapshot"
    ]
    item.update(
        {
            "providerCode": WIND_SMALL_CAP_CODE,
            "officialUrl": WIND_SMALL_CAP_DETAIL_URL,
            "signalEligible": False,
            "availability": "partial" if optional_warnings or not rows else "available",
            "sourceWarnings": optional_warnings,
        }
    )
    return item


def _unavailable_wind_reference_item(spec: IndexSpec, error: str) -> Dict[str, Any]:
    finality = {
        "status": "unavailable",
        "source": "wind_web",
        "reason": "wind_reference_unavailable",
        "providerAsOf": None,
        "completeClose": False,
    }
    item = _make_item(
        spec,
        None,
        None,
        None,
        [],
        None,
        "wind_web",
        False,
        daily_close_finality=finality,
    )
    item.update(
        {
            "providerCode": WIND_SMALL_CAP_CODE,
            "officialUrl": WIND_SMALL_CAP_DETAIL_URL,
            "signalEligible": False,
            "availability": "unavailable",
            "sourceWarnings": [str(error)],
        }
    )
    return item


def _build_index_market_payload() -> Dict[str, Any]:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    errors: List[Dict[str, str]] = []
    reference_errors: List[Dict[str, str]] = []
    us_items: List[Dict[str, Any]] = []
    cn_items: List[Dict[str, Any]] = []
    hk_items: List[Dict[str, Any]] = []
    jp_kr_items: List[Dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=min(4, len(US_INDEXES))) as pool:
        future_map = {pool.submit(_fetch_yahoo_index, spec): spec for spec in US_INDEXES}
        for future in as_completed(future_map):
            spec = future_map[future]
            try:
                us_items.append(future.result())
            except Exception as exc:  # noqa: BLE001
                errors.append({"name": spec.name, "region": spec.region, "error": str(exc)})

    with ThreadPoolExecutor(max_workers=len(JP_KR_INDEXES)) as pool:
        future_map = {pool.submit(_fetch_yahoo_index, spec): spec for spec in JP_KR_INDEXES}
        for future in as_completed(future_map):
            spec = future_map[future]
            try:
                jp_kr_items.append(future.result())
            except Exception as exc:  # noqa: BLE001
                errors.append({"name": spec.name, "region": spec.region, "error": str(exc)})

    quote_map: Dict[str, Dict[str, Any]] = {}
    try:
        quote_map = _fetch_cn_quotes()
    except Exception as exc:  # noqa: BLE001
        errors.append({"name": "中国指数实时行情", "region": "CN", "error": str(exc)})

    if quote_map:
        with ThreadPoolExecutor(max_workers=min(4, len(CN_INDEXES))) as pool:
            future_map = {pool.submit(_fetch_cn_index, spec, quote_map): spec for spec in CN_INDEXES}
            for future in as_completed(future_map):
                spec = future_map[future]
                try:
                    cn_items.append(future.result())
                except Exception as exc:  # noqa: BLE001
                    errors.append({"name": spec.name, "region": spec.region, "error": str(exc)})

    for spec in CN_REFERENCE_INDEXES:
        try:
            reference_item = _fetch_wind_reference_index(spec)
            cn_items.append(reference_item)
            for warning in reference_item.get("sourceWarnings") or []:
                reference_errors.append(
                    {"name": spec.name, "region": spec.region, "error": str(warning)}
                )
        except Exception as exc:  # noqa: BLE001 - optional display source must not freeze the core 27
            message = str(exc)
            reference_errors.append({"name": spec.name, "region": spec.region, "error": message})
            cn_items.append(_unavailable_wind_reference_item(spec, message))

    hk_quote_map: Dict[str, Dict[str, Any]] = {}
    try:
        hk_quote_map = _fetch_hk_quotes()
    except Exception as exc:  # noqa: BLE001
        errors.append({"name": "香港指数实时行情", "region": "HK", "error": str(exc)})

    if hk_quote_map:
        with ThreadPoolExecutor(max_workers=len(HK_INDEXES)) as pool:
            future_map = {pool.submit(_fetch_hk_index, spec, hk_quote_map): spec for spec in HK_INDEXES}
            for future in as_completed(future_map):
                spec = future_map[future]
                try:
                    hk_items.append(future.result())
                except Exception as exc:  # noqa: BLE001
                    errors.append({"name": spec.name, "region": spec.region, "error": str(exc)})

    order = {
        spec.key: idx
        for idx, spec in enumerate(
            US_INDEXES + CN_INDEXES + CN_REFERENCE_INDEXES + HK_INDEXES + JP_KR_INDEXES
        )
    }
    us_items.sort(key=lambda item: order.get(str(item.get("key")), 999))
    cn_items.sort(key=lambda item: order.get(str(item.get("key")), 999))
    hk_items.sort(key=lambda item: order.get(str(item.get("key")), 999))
    jp_kr_items.sort(key=lambda item: order.get(str(item.get("key")), 999))

    payload = {
        "ok": True,
        "generatedAt": generated_at,
        "rangeLabel": "最近3个月",
        "groups": [
            {"key": "US", "displayCode": "US", "title": "美国市场", "subtitle": "SPY + QQQ + 道指/VIX + 美股七姐妹", "items": us_items},
            {
                "key": "CN",
                "displayCode": "CN",
                "title": "中国市场",
                "subtitle": f"{len(CN_INDEXES)} 个信号标的 + 1 个万得行情参考",
                "items": cn_items,
            },
            {"key": "HK", "displayCode": "HK", "title": "香港市场", "subtitle": f"{len(HK_INDEXES)}大主要股指", "items": hk_items},
            {"key": "JP_KR", "displayCode": "JP/KR", "title": "日韩市场", "subtitle": "2大主要指数 + 2只半导体龙头", "items": jp_kr_items},
        ],
        "errors": errors,
        "referenceErrors": reference_errors,
    }
    return payload


def _reuse_last_wind_reference(payload: Dict[str, Any], previous: Any) -> Dict[str, Any]:
    """Carry a last good Wind card when only the optional website source fails."""

    if not isinstance(previous, dict) or not payload.get("referenceErrors"):
        return payload
    previous_items = {
        str(item.get("key") or ""): item
        for group in previous.get("groups") or []
        if isinstance(group, dict)
        for item in group.get("items") or []
        if isinstance(item, dict) and item.get("signalEligible") is False
    }
    for group in payload.get("groups") or []:
        if not isinstance(group, dict) or not isinstance(group.get("items"), list):
            continue
        for index, item in enumerate(group["items"]):
            if not isinstance(item, dict) or item.get("signalEligible") is not False:
                continue
            if str(item.get("availability") or "") != "unavailable":
                continue
            previous_item = previous_items.get(str(item.get("key") or ""))
            if not isinstance(previous_item, dict) or _num(previous_item.get("price")) is None:
                continue
            replacement = copy.deepcopy(previous_item)
            replacement["availability"] = "stale"
            replacement["sourceWarnings"] = copy.deepcopy(item.get("sourceWarnings") or [])
            group["items"][index] = replacement
    return payload


def _refresh_index_cache(observed_ts: Optional[float] = None) -> Dict[str, Any]:
    """Refresh once, coalescing requests that observed the same stale snapshot."""

    with _INDEX_REFRESH_LOCK:
        _ensure_index_cache_loaded()
        with _INDEX_CACHE_LOCK:
            current_ts = float(_INDEX_CACHE.get("ts") or 0.0)
            current_payload = _INDEX_CACHE.get("payload")
        if (
            observed_ts is not None
            and current_payload is not None
            and current_ts > float(observed_ts)
        ):
            return _cache_result(
                current_payload,
                state="fresh",
                age_seconds=time.time() - current_ts,
                refreshing=False,
            )

        payload = _reuse_last_wind_reference(
            _build_index_market_payload(),
            current_payload,
        )
        if not _valid_index_cache_payload(payload):
            with _INDEX_CACHE_LOCK:
                fallback_payload = _INDEX_CACHE.get("payload")
            if fallback_payload is not None:
                item_count = sum(
                    len(group.get("items") or [])
                    for group in payload.get("groups") or []
                    if isinstance(group, dict) and isinstance(group.get("items"), list)
                )
                raise RuntimeError(
                    f"指数行情刷新不完整：{item_count}/{INDEX_ASSET_COUNT}，"
                    f"错误 {len(payload.get('errors') or [])}；保留上次有效缓存"
                )
            return _cache_result(payload, state="partial", age_seconds=0, refreshing=False)
        saved_at = time.time()
        with _INDEX_CACHE_LOCK:
            _INDEX_CACHE.update(
                {
                    "loaded": True,
                    "ts": saved_at,
                    "payload": copy.deepcopy(payload),
                    "lastError": None,
                }
            )
        _persist_index_cache(payload, saved_at=saved_at)
        return _cache_result(payload, state="fresh", age_seconds=0, refreshing=False)


def _background_index_refresh_worker(observed_ts: float) -> None:
    global _INDEX_BACKGROUND_REFRESHING
    try:
        _refresh_index_cache(observed_ts=observed_ts)
    except Exception as exc:  # noqa: BLE001 - preserve the last good market snapshot
        with _INDEX_CACHE_LOCK:
            _INDEX_CACHE["lastError"] = str(exc)
    finally:
        with _INDEX_BACKGROUND_LOCK:
            _INDEX_BACKGROUND_REFRESHING = False


def _start_background_index_refresh() -> bool:
    global _INDEX_BACKGROUND_REFRESHING
    with _INDEX_BACKGROUND_LOCK:
        if _INDEX_BACKGROUND_REFRESHING:
            return False
        with _INDEX_CACHE_LOCK:
            observed_ts = float(_INDEX_CACHE.get("ts") or 0.0)
        _INDEX_BACKGROUND_REFRESHING = True
        worker = threading.Thread(
            target=_background_index_refresh_worker,
            args=(observed_ts,),
            name="index-market-refresh",
            daemon=True,
        )
        try:
            worker.start()
        except Exception:
            _INDEX_BACKGROUND_REFRESHING = False
            raise
        return True


def _background_index_refresh_running() -> bool:
    with _INDEX_BACKGROUND_LOCK:
        return bool(_INDEX_BACKGROUND_REFRESHING)


def build_index_market_bundle(
    force_refresh: bool = False,
    allow_stale: bool = False,
    max_age_seconds: Optional[float] = None,
) -> Dict[str, Any]:
    """Return the internal market bundle, with an optional caller-specific freshness limit."""

    _ensure_index_cache_loaded()
    now = time.time()
    with _INDEX_CACHE_LOCK:
        cached = _INDEX_CACHE.get("payload")
        cached_ts = float(_INDEX_CACHE.get("ts") or 0.0)
    age_seconds = max(0.0, now - cached_ts)
    freshness_window = (
        float(_INDEX_TTL_SECONDS)
        if max_age_seconds is None
        else max(0.0, float(max_age_seconds))
    )

    if not force_refresh and cached is not None and age_seconds < freshness_window:
        return _cache_result(cached, state="fresh", age_seconds=age_seconds, refreshing=False)

    if not force_refresh and allow_stale and cached is not None:
        _start_background_index_refresh()
        return _cache_result(
            cached,
            state="stale",
            age_seconds=age_seconds,
            refreshing=_background_index_refresh_running(),
        )

    return _refresh_index_cache(observed_ts=cached_ts)


def _empty_public_signal(timeframe: str) -> Dict[str, Any]:
    return {
        "timeframe": timeframe,
        "sourceInterval": {"short": "15m", "medium": "60m", "long": "1d"}.get(timeframe),
        "barCount": 0,
        "availability": "not_scanned",
        "direction": None,
        "status": "none",
        "candidateStatus": "none",
        "lifecycleState": "none",
        "score": 0,
        "pattern": "",
        "patternId": "",
        "anchors": [],
        "confluence": {"level": "none", "supportingTimeframes": []},
        "keyLevel": None,
        "invalidationLevel": None,
        "targetRange": None,
        "changedAt": None,
        "stateChangedAt": None,
        "evaluatedAt": None,
        "confirmation": None,
        "confirmationCondition": None,
        "keyCrossState": None,
        "keyCrossEvent": None,
        "signalAgeBars": 0,
        "expiresAfterBars": None,
        "expiresAt": None,
        "latestCompletedBarAt": None,
        "stateLatestCompletedBarAt": None,
        "invalidationBuffer": None,
        "dataQuality": {},
        "currentDataQuality": {},
        "currentStateEligible": None,
        "evidence": [],
        "metrics": {"ma20": None, "ma60": None, "rsi14": None, "atr14": None},
    }


def _snapshot_records(signal_snapshot: Optional[Dict[str, Any]], bucket: str) -> List[Dict[str, Any]]:
    if not isinstance(signal_snapshot, dict):
        return []
    raw_records = signal_snapshot.get(bucket)
    records: List[Dict[str, Any]] = []
    if isinstance(raw_records, dict):
        records.extend(record for record in raw_records.values() if isinstance(record, dict))
    elif isinstance(raw_records, list):
        records.extend(record for record in raw_records if isinstance(record, dict))
    return records


def _completed_daily_close_at(
    completed_daily_rows: Optional[List[Dict[str, Any]]],
    completed_bar_at: Any,
) -> Optional[float]:
    """Return the close for one explicitly identified completed daily bar."""

    target = str(completed_bar_at or "").strip()
    if not target or not isinstance(completed_daily_rows, list):
        return None
    target_date = target[:10]
    for row in reversed(completed_daily_rows):
        if not isinstance(row, dict):
            continue
        row_at = str(row.get("date") or "").strip()
        if row_at != target and row_at[:10] != target_date:
            continue
        close = _num(row.get("close"))
        if close is not None:
            return close
    return None


def _stored_signal_for(
    signal_snapshot: Optional[Dict[str, Any]],
    index_key: str,
    timeframe: str,
    *,
    completed_daily_rows: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    def matching_records(bucket: str) -> List[Dict[str, Any]]:
        return [
            record
            for record in _snapshot_records(signal_snapshot, bucket)
            if str(record.get("indexKey") or "") == index_key
            and str(record.get("timeframe") or "").lower() == timeframe
        ]

    durable_candidates = matching_records("states")
    observation_candidates = matching_records("observations")

    # Also accept a compact, already-grouped snapshot for tests and future stores.
    if isinstance(signal_snapshot, dict):
        compact = signal_snapshot.get(index_key)
        if isinstance(compact, dict) and isinstance(compact.get(timeframe), dict):
            durable_candidates.append(compact[timeframe])

    if not durable_candidates and not observation_candidates:
        return _empty_public_signal(timeframe)

    priority = {
        "confirmed": 7,
        "warning": 6,
        "invalidated": 5,
        "expired": 4,
        "conflict": 3,
        "unavailable": 2,
        "none": 1,
    }

    def state_rank(record: Dict[str, Any]) -> tuple[int, str]:
        status = str(record.get("status") or record.get("candidateStatus") or "none").lower()
        return priority.get(status, 0), str(record.get("updatedAt") or record.get("changedAt") or "")

    def observation_rank(record: Dict[str, Any]) -> tuple[str, int]:
        status = str(record.get("status") or record.get("candidateStatus") or "none").lower()
        return str(record.get("changedAt") or record.get("updatedAt") or ""), priority.get(status, 0)

    durable_state = max(durable_candidates, key=state_rank) if durable_candidates else None
    observation = max(observation_candidates, key=observation_rank) if observation_candidates else None
    state = durable_state or observation
    if state is None:  # pragma: no cover - guarded by the candidate check above
        return _empty_public_signal(timeframe)

    status = str(state.get("status") or state.get("candidateStatus") or "none").lower()
    if status not in priority:
        status = "none"
    availability = str(state.get("availability") or "available").lower()
    if status == "none" and availability in {"unavailable", "insufficient"}:
        status = "unavailable"
    lifecycle_state = str(state.get("lifecycleState") or "").lower()
    if not lifecycle_state:
        lifecycle_state = {
            "warning": "candidate",
            "confirmed": "confirmed",
            "invalidated": "invalidated",
            "expired": "expired",
        }.get(status, "none")
    state_changed_at = durable_state.get("changedAt") if isinstance(durable_state, dict) else None
    evaluated_at = (
        observation.get("changedAt")
        if isinstance(observation, dict) and observation.get("changedAt")
        else state.get("updatedAt") or state.get("changedAt")
    )
    evidence = state.get("evidence") if isinstance(state.get("evidence"), list) else []
    metrics = state.get("metrics") if isinstance(state.get("metrics"), dict) else {}
    observation_metrics = (
        observation.get("metrics")
        if isinstance(observation, dict) and isinstance(observation.get("metrics"), dict)
        else {}
    )
    current_quality = (
        observation.get("dataQuality")
        if isinstance(observation, dict) and isinstance(observation.get("dataQuality"), dict)
        else state.get("currentDataQuality")
    )
    state_latest_completed_bar_at = (
        durable_state.get("latestCompletedBarAt") if isinstance(durable_state, dict) else None
    )
    evaluation_bar_at = (
        observation.get("latestCompletedBarAt")
        if isinstance(observation, dict) and observation.get("latestCompletedBarAt")
        else state.get("latestCompletedBarAt")
    )
    state_confirmation = state.get("confirmation") if isinstance(state.get("confirmation"), dict) else {}
    observation_confirmation = (
        observation.get("confirmation")
        if isinstance(observation, dict) and isinstance(observation.get("confirmation"), dict)
        else {}
    )
    evaluation_close = _num(observation_confirmation.get("latestClose"))
    if evaluation_close is None:
        evaluation_close = _completed_daily_close_at(completed_daily_rows, evaluation_bar_at)
    if evaluation_close is None:
        evaluation_close = _num(state_confirmation.get("latestClose"))
    evaluation_atr = _num(observation_metrics.get("atr14"))
    if evaluation_atr is None:
        evaluation_atr = _num(metrics.get("atr14"))

    active_long = timeframe == "long" and status in {"warning", "confirmed"}
    if active_long and observation is not None:
        confirmation = confirmation_contract_for_state(
            timeframe=timeframe,
            direction=state.get("direction"),
            status=status,
            lifecycle_state=lifecycle_state,
            score=state.get("score"),
            key_level=state.get("keyLevel"),
            latest_close=evaluation_close,
            latest_atr=evaluation_atr,
        )
    elif state_confirmation:
        # A current persisted contract was already built by the same evaluator;
        # retain it when no newer observation exists.
        confirmation = copy.deepcopy(state_confirmation)
    elif active_long:
        confirmation = confirmation_contract_for_state(
            timeframe=timeframe,
            direction=state.get("direction"),
            status=status,
            lifecycle_state=lifecycle_state,
            score=state.get("score"),
            key_level=state.get("keyLevel"),
            latest_close=evaluation_close,
            latest_atr=evaluation_atr,
        )
    else:
        confirmation = copy.deepcopy(state_confirmation) if state_confirmation else None
    projected_key_cross_state = (
        confirmation.get("keyCrossState")
        if isinstance(confirmation, dict) and confirmation.get("keyCrossState") is not None
        else state.get("keyCrossState")
    )
    return {
        "timeframe": timeframe,
        "sourceInterval": state.get("sourceInterval") or {"short": "15m", "medium": "60m", "long": "1d"}.get(timeframe),
        "barCount": state.get("barCount") or 0,
        "availability": availability,
        "direction": state.get("direction") if state.get("direction") in {"top", "bottom"} else None,
        "status": status,
        "candidateStatus": str(state.get("candidateStatus") or status).lower(),
        "lifecycleState": lifecycle_state,
        "score": int(round(float(state.get("score") or 0))),
        "pattern": str(state.get("pattern") or ""),
        "patternId": str(state.get("patternId") or state.get("pattern") or ""),
        "anchors": copy.deepcopy(state.get("anchors")) if isinstance(state.get("anchors"), list) else [],
        "confluence": copy.deepcopy(state.get("confluence")) if isinstance(state.get("confluence"), dict) else {"level": "none", "supportingTimeframes": []},
        "keyLevel": state.get("keyLevel"),
        "invalidationLevel": state.get("invalidationLevel"),
        "targetRange": copy.deepcopy(state.get("targetRange")),
        # ``changedAt`` remains as a compatibility alias, but now has durable
        # state-transition semantics.  A fresh observation timestamp is an
        # evaluation time and must never masquerade as a state transition.
        "changedAt": state_changed_at,
        "stateChangedAt": state_changed_at,
        "evaluatedAt": evaluated_at,
        "confirmation": confirmation,
        "confirmationCondition": copy.deepcopy(state.get("confirmationCondition")),
        "keyCrossState": projected_key_cross_state,
        "keyCrossEvent": state.get("keyCrossEvent"),
        "signalAgeBars": state.get("signalAgeBars") or 0,
        "expiresAfterBars": state.get("expiresAfterBars"),
        "expiresAt": state.get("expiresAt"),
        "latestCompletedBarAt": evaluation_bar_at,
        "stateLatestCompletedBarAt": state_latest_completed_bar_at,
        "invalidationBuffer": state.get("invalidationBuffer"),
        "dataQuality": copy.deepcopy(state.get("dataQuality")) if isinstance(state.get("dataQuality"), dict) else {},
        "currentDataQuality": copy.deepcopy(current_quality) if isinstance(current_quality, dict) else {},
        "currentStateEligible": (
            current_quality.get("stateEligible")
            if isinstance(current_quality, dict)
            else state.get("currentStateEligible")
        ),
        "evidence": copy.deepcopy(evidence),
        "metrics": {
            "ma20": metrics.get("ma20"),
            "ma60": metrics.get("ma60"),
            "rsi14": metrics.get("rsi14"),
            "atr14": metrics.get("atr14"),
        },
    }


def build_index_dashboard(
    force_refresh: bool = False,
    signal_snapshot: Optional[Dict[str, Any]] = None,
    allow_stale: bool = False,
) -> Dict[str, Any]:
    """Return the public dashboard payload without leaking full daily history."""

    if allow_stale:
        internal = build_index_market_bundle(
            force_refresh=force_refresh,
            allow_stale=True,
        )
    else:
        internal = build_index_market_bundle(force_refresh=force_refresh)
    public_payload = {
        key: copy.deepcopy(value)
        for key, value in internal.items()
        if key != "groups"
    }
    public_groups: List[Dict[str, Any]] = []
    for group in internal.get("groups") or []:
        public_group = {key: copy.deepcopy(value) for key, value in group.items() if key != "items"}
        public_items: List[Dict[str, Any]] = []
        for raw_item in group.get("items") or []:
            item = {key: copy.deepcopy(value) for key, value in raw_item.items() if key != "_historyBundle"}
            if item.get("signalEligible") is False:
                item["signals"] = {}
                public_items.append(item)
                continue
            index_key = str(item.get("key") or "")
            history_bundle = raw_item.get("_historyBundle") if isinstance(raw_item.get("_historyBundle"), dict) else {}
            completed_daily_rows = (
                history_bundle.get("signalRows")
                if isinstance(history_bundle.get("signalRows"), list)
                else []
            )
            signals = {
                "short": _stored_signal_for(
                    signal_snapshot,
                    index_key,
                    "short",
                    completed_daily_rows=completed_daily_rows,
                ),
                "medium": _stored_signal_for(
                    signal_snapshot,
                    index_key,
                    "medium",
                    completed_daily_rows=completed_daily_rows,
                ),
                "long": _stored_signal_for(
                    signal_snapshot,
                    index_key,
                    "long",
                    completed_daily_rows=completed_daily_rows,
                ),
            }
            fallback_metrics = latest_indicator_metrics(
                daily_rows=history_bundle.get("rows") if isinstance(history_bundle.get("rows"), list) else [],
                current_price=history_bundle.get("currentPrice", item.get("price")),
            )
            for signal in signals.values():
                metrics = signal.get("metrics") if isinstance(signal.get("metrics"), dict) else {}
                signal["metrics"] = {
                    name: metrics.get(name) if metrics.get(name) is not None else fallback_metrics.get(name)
                    for name in ("ma20", "ma60", "rsi14", "atr14")
                }
            item["signals"] = signals
            public_items.append(item)
        public_group["items"] = public_items
        public_groups.append(public_group)
    public_payload["groups"] = public_groups
    return public_payload
