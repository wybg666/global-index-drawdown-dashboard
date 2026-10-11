from __future__ import annotations

import math
import json
import os
import re
import tempfile
import threading
import time
from collections.abc import Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from pathlib import Path
from .paths import DATA_DIR
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from . import __version__
from .central_bank_gold import fetch_china_history, fetch_turkey_history
from .china_bond_yields import fetch_chinabond_yield_rows
from .macro_telemetry import (
    KOFIA_URL,
    TREASURY_CSV_URL,
    parse_kofia_payload,
    parse_treasury_csv,
)


HISTORY_KEYS = ("us10y", "us30y", "cn10y", "cn30y", "kr_margin_financing", "gold", "cn_gold_reserves", "tr_gold_reserves")
HISTORY_INTERVALS = ("D", "W", "M", "Y")

_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_HISTORY_CACHE_PATH = DATA_DIR / "macro_telemetry_history_cache.json"
_HISTORY_SCHEMA = 3
_HISTORY_TTL_SECONDS = 6 * 60 * 60
_MAX_HISTORY_WORKERS = 6
_REQUEST_TIMEOUT_SECONDS = 15
_MAX_REQUEST_ATTEMPTS = 2
_MAX_RESPONSE_BYTES = 5 * 1024 * 1024
_KEY_META = {
    "us10y": {"name": "10年美债收益率", "region": "US", "source": "U.S. Treasury", "unit": "%"},
    "us30y": {"name": "30年美债收益率", "region": "US", "source": "U.S. Treasury", "unit": "%"},
    "cn10y": {"name": "10年中国国债收益率", "region": "CN", "source": "ChinaBond（中央国债登记结算有限责任公司）", "unit": "%"},
    "cn30y": {"name": "30年中国国债收益率", "region": "CN", "source": "ChinaBond（中央国债登记结算有限责任公司）", "unit": "%"},
    "kr_margin_financing": {"name": "韩国信用融资余额", "region": "KR", "source": "KOFIA FreeSIS", "unit": "万亿韩元"},
    "gold": {"name": "黄金（COMEX）", "region": "Global", "source": "Yahoo Finance", "unit": "USD/oz"},
    "cn_gold_reserves": {"name": "中国官方黄金储备（央行）", "region": "CN", "source": "State Administration of Foreign Exchange (SAFE)", "unit": "吨"},
    "tr_gold_reserves": {"name": "土耳其央行黄金储备", "region": "TR", "source": "Central Bank of the Republic of Türkiye (TCMB) EVDS3", "unit": "吨"},
}

_HISTORY_CACHE_LOCK = threading.RLock()
_HISTORY_MEMORY: dict[str, Any] | None = None
_HISTORY_ERRORS: dict[str, dict[str, str]] = {}
_HISTORY_FLIGHTS: dict[str, threading.Event] = {}
_HISTORY_THREADS: dict[str, threading.Thread] = {}
_NETWORK_SLOTS = threading.BoundedSemaphore(_MAX_HISTORY_WORKERS)


def validate_history_request(key: Any, interval: Any) -> tuple[str, str]:
    key_text = str(key or "")
    interval_text = str(interval or "").upper()
    if key_text not in HISTORY_KEYS:
        raise ValueError("invalid macro telemetry history key")
    if interval_text not in HISTORY_INTERVALS:
        raise ValueError("invalid macro telemetry history interval")
    return key_text, interval_text


def normalize_points(
    rows: Iterable[Mapping[str, Any]], *, key: str
) -> list[dict[str, Any]]:
    key_text, _ = validate_history_request(key, "D")
    by_date: dict[str, float] = {}

    for row in rows:
        if not isinstance(row, Mapping):
            continue
        raw_date = str(row.get("date") or "")
        if not _ISO_DATE.fullmatch(raw_date):
            continue
        raw_value = row.get("value")
        if isinstance(raw_value, bool):
            continue
        try:
            parsed_date = date.fromisoformat(raw_date)
            value = float(raw_value)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(value) or value <= 0:
            continue
        if key_text in ("us10y", "us30y", "cn10y", "cn30y") and value >= 20:
            continue
        by_date[parsed_date.isoformat()] = value

    return [{"date": day, "value": by_date[day]} for day in sorted(by_date)]


def aggregate_period_end(
    points: Iterable[Mapping[str, Any]], interval: str
) -> list[dict[str, Any]]:
    interval_text = str(interval or "").upper()
    if interval_text == "D":
        return [dict(point) for point in points]
    if interval_text not in ("W", "M", "Y"):
        raise ValueError("invalid macro telemetry history interval")

    selected: dict[tuple[int, ...], tuple[date, dict[str, Any]]] = {}
    for point in points:
        point_copy = dict(point)
        point_date = date.fromisoformat(str(point_copy["date"]))
        if interval_text == "W":
            iso_year, iso_week, _ = point_date.isocalendar()
            bucket = (iso_year, iso_week)
        elif interval_text == "M":
            bucket = (point_date.year, point_date.month)
        else:
            bucket = (point_date.year,)

        current = selected.get(bucket)
        if current is None or point_date >= current[0]:
            selected[bucket] = (point_date, point_copy)

    return [
        point
        for _, point in sorted(selected.values(), key=lambda selected_point: selected_point[0])
    ]


def public_history_payload(
    *,
    key: Any,
    interval: Any,
    generated_at: Any,
    points: Iterable[Mapping[str, Any]],
    stale: Any,
    errors: Iterable[Any],
    cache: Mapping[str, Any],
) -> dict[str, Any]:
    key_text, interval_text = validate_history_request(key, interval)
    normalized = normalize_points(points, key=key_text)
    meta = _KEY_META[key_text]
    return {
        "ok": bool(normalized),
        "key": key_text,
        "name": meta["name"],
        "region": meta["region"],
        "source": meta["source"],
        "unit": meta["unit"],
        "interval": interval_text,
        "generatedAt": generated_at,
        "startDate": normalized[0]["date"] if normalized else None,
        "endDate": normalized[-1]["date"] if normalized else None,
        "pointCount": len(normalized),
        "points": normalized,
        "stale": bool(stale),
        "errors": list(errors),
        "cache": dict(cache),
    }


def _coverage_start(key: str, interval: str, today: date) -> date:
    key_text, interval_text = validate_history_request(key, interval)
    if key_text == "cn_gold_reserves":
        return date(2016, 1, 1)
    if key_text == "tr_gold_reserves":
        return date(2000, 8, 1)
    if key_text in ("cn10y", "cn30y"):
        return date(2006, 3, 1)
    if interval_text == "D":
        return date(today.year - 1, 1, 1)
    if interval_text == "W":
        try:
            return today.replace(year=today.year - 10)
        except ValueError:
            return today.replace(year=today.year - 10, day=28)
    if key_text in ("us10y", "us30y", "gold"):
        return date(1990, 1, 1)
    return date(1998, 1, 1)


def _source_today(key: str) -> date:
    key_text, _ = validate_history_request(key, "D")
    timezone_name = {
        "us10y": "America/New_York",
        "us30y": "America/New_York",
        "cn10y": "Asia/Shanghai",
        "cn30y": "Asia/Shanghai",
        "gold": "America/New_York",
        "kr_margin_financing": "Asia/Seoul",
        "cn_gold_reserves": "Asia/Shanghai",
        "tr_gold_reserves": "Europe/Istanbul",
    }[key_text]
    return datetime.now(ZoneInfo(timezone_name)).date()


def _calendar_year_chunks(start: date, end: date) -> list[tuple[date, date]]:
    if start > end:
        return []
    return [
        (
            max(start, date(year, 1, 1)),
            min(end, date(year, 12, 31)),
        )
        for year in range(start.year, end.year + 1)
    ]


def _read_limited(request: Request) -> tuple[bytes, str]:
    with _NETWORK_SLOTS:
        with urlopen(request, timeout=_REQUEST_TIMEOUT_SECONDS) as response:
            status = int(getattr(response, "status", 200) or 200)
            if status >= 400:
                raise RuntimeError("history source returned an HTTP error")
            body = response.read(_MAX_RESPONSE_BYTES + 1)
            if len(body) > _MAX_RESPONSE_BYTES:
                raise ValueError("history source response is too large")
            content_type = str(response.headers.get("Content-Type", "")).lower()
            return body, content_type


def _fetch_treasury_year(year: int) -> list[dict[str, Any]]:
    query = urlencode(
        {
            "type": "daily_treasury_yield_curve",
            "field_tdr_date_value": str(year),
            "page": "",
            "_format": "csv",
        }
    )
    last_error: Exception | None = None
    for attempt in range(_MAX_REQUEST_ATTEMPTS):
        request = Request(
            f"{TREASURY_CSV_URL.format(year=year)}?{query}",
            headers={
                "Accept": "text/csv,*/*;q=0.8",
                "User-Agent": f"StockSignalDashboard/{__version__}",
            },
        )
        try:
            body, content_type = _read_limited(request)
            text = body.decode("utf-8-sig")
            first_line = text.splitlines()[0] if text.splitlines() else ""
            if "html" in content_type or "Date" not in first_line or "10 Yr" not in first_line:
                raise ValueError("Treasury response is not CSV")
            return parse_treasury_csv(text)
        except Exception as exc:  # urllib exposes several transport-specific errors
            last_error = exc
            if attempt + 1 < _MAX_REQUEST_ATTEMPTS:
                time.sleep(0.2 * (attempt + 1))
    raise RuntimeError("Treasury history request failed") from last_error


def _fetch_treasury_years(years: Iterable[int]) -> list[dict[str, Any]]:
    ordered_years = sorted({int(year) for year in years})
    if not ordered_years:
        return []
    rows_by_year: dict[int, list[dict[str, Any]]] = {}
    with ThreadPoolExecutor(
        max_workers=min(_MAX_HISTORY_WORKERS, len(ordered_years)),
        thread_name_prefix="macro-history-treasury",
    ) as executor:
        futures = {executor.submit(_fetch_treasury_year, year): year for year in ordered_years}
        for future, year in ((future, futures[future]) for future in futures):
            rows_by_year[year] = future.result()
    rows_by_date: dict[str, dict[str, Any]] = {}
    for year in ordered_years:
        for row in rows_by_year[year]:
            if isinstance(row, Mapping) and row.get("date"):
                rows_by_date[str(row["date"])] = dict(row)
    return [rows_by_date[day] for day in sorted(rows_by_date)]


def _fetch_chinabond_rows(start: date, end: date) -> list[dict[str, Any]]:
    """Fetch official ChinaBond observations in source-safe annual chunks."""

    chunks = _calendar_year_chunks(start, end)
    if not chunks:
        return []
    rows_by_chunk: dict[tuple[date, date], list[dict[str, Any]]] = {}
    with ThreadPoolExecutor(
        max_workers=min(4, len(chunks)),
        thread_name_prefix="macro-history-chinabond",
    ) as executor:
        futures = {
            executor.submit(fetch_chinabond_yield_rows, chunk_start, chunk_end): (chunk_start, chunk_end)
            for chunk_start, chunk_end in chunks
        }
        for future, chunk in ((future, futures[future]) for future in futures):
            rows_by_chunk[chunk] = future.result()
    by_date: dict[str, dict[str, Any]] = {}
    for chunk in chunks:
        for row in rows_by_chunk[chunk]:
            if isinstance(row, Mapping) and row.get("date"):
                by_date[str(row["date"])] = dict(row)
    return [by_date[day] for day in sorted(by_date)]


def _fetch_kofia_payload(interval: str, start: date, end: date) -> Mapping[str, Any]:
    body = json.dumps(
        {
            "dmSearch": {
                "OBJ_NM": "STATSCU0100000070BO",
                "tmpV1": interval,
                "tmpV40": "1000000",
                "tmpV41": "1",
                "tmpV45": start.strftime("%Y%m%d"),
                "tmpV46": end.strftime("%Y%m%d"),
            }
        },
        separators=(",", ":"),
    ).encode("utf-8")
    last_error: Exception | None = None
    for attempt in range(_MAX_REQUEST_ATTEMPTS):
        request = Request(
            KOFIA_URL,
            data=body,
            method="POST",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json; charset=UTF-8",
                "User-Agent": f"StockSignalDashboard/{__version__}",
            },
        )
        try:
            raw_body, content_type = _read_limited(request)
            text = raw_body.decode("utf-8-sig")
            if "html" in content_type or not text.lstrip().startswith("{"):
                raise ValueError("KOFIA response is not JSON")
            payload = json.loads(text)
            if not isinstance(payload, Mapping):
                raise ValueError("KOFIA response is not an object")
            parse_kofia_payload(payload)
            return payload
        except Exception as exc:
            last_error = exc
            if attempt + 1 < _MAX_REQUEST_ATTEMPTS:
                time.sleep(0.2 * (attempt + 1))
    raise RuntimeError("KOFIA history request failed") from last_error


def _kofia_points(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    return normalize_points(
        [
            {
                "date": row.get("date"),
                "value": float(row.get("totalMillionKrw")) / 1_000_000,
            }
            for row in parse_kofia_payload(payload)
        ],
        key="kr_margin_financing",
    )


def _fetch_kofia_daily_points(start: date, end: date) -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []
    for chunk_start, chunk_end in _calendar_year_chunks(start, end):
        points.extend(_kofia_points(_fetch_kofia_payload("D", chunk_start, chunk_end)))
    return normalize_points(points, key="kr_margin_financing")


def _fetch_kofia_native_points(
    interval: str, start: date, end: date
) -> list[dict[str, Any]]:
    if interval not in ("M", "Y"):
        raise ValueError("KOFIA native history supports only M or Y")
    return _kofia_points(_fetch_kofia_payload(interval, start, end))


def _period_bucket(day_text: str, interval: str) -> tuple[int, ...]:
    parsed = date.fromisoformat(day_text)
    if interval == "W":
        iso_year, iso_week, _ = parsed.isocalendar()
        return (iso_year, iso_week)
    return (parsed.year, parsed.month) if interval == "M" else (parsed.year,)


def _merge_period_points(
    existing_points: Iterable[Mapping[str, Any]],
    fetched_points: Iterable[Mapping[str, Any]],
    *,
    key: str,
    interval: str,
    start: date,
    end: date,
) -> list[dict[str, Any]]:
    existing = normalize_points(existing_points, key=key)
    fetched = normalize_points(fetched_points, key=key)
    if interval == "D":
        merged = normalize_points([*existing, *fetched], key=key)
    else:
        fetched_buckets = {_period_bucket(point["date"], interval) for point in fetched}
        retained = [
            point
            for point in existing
            if _period_bucket(point["date"], interval) not in fetched_buckets
        ]
        merged = aggregate_period_end(
            normalize_points([*retained, *fetched], key=key),
            interval,
        )
    return [
        point
        for point in merged
        if start.isoformat() <= point["date"] <= end.isoformat()
    ]


def _validate_kofia_native_overlap(
    native_points: Iterable[Mapping[str, Any]],
    daily_points: Iterable[Mapping[str, Any]],
    interval: str,
) -> None:
    native = aggregate_period_end(
        normalize_points(native_points, key="kr_margin_financing"), interval
    )
    daily = aggregate_period_end(
        normalize_points(daily_points, key="kr_margin_financing"), interval
    )
    native_by_bucket = {_period_bucket(point["date"], interval): point for point in native}
    daily_by_bucket = {_period_bucket(point["date"], interval): point for point in daily}
    common = sorted(set(native_by_bucket) & set(daily_by_bucket))
    if len(common) < 2:
        raise ValueError("KOFIA native history has insufficient daily overlap")
    for bucket in common:
        native_value = float(native_by_bucket[bucket]["value"])
        daily_value = float(daily_by_bucket[bucket]["value"])
        tolerance = max(0.001, abs(daily_value) * 0.0001)
        if abs(native_value - daily_value) > tolerance:
            raise ValueError("KOFIA native history differs from daily period-end values")


def _fetch_source_points(
    key: str,
    interval: str,
    *,
    existing_points: Iterable[Mapping[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    key_text, interval_text = validate_history_request(key, interval)
    end = _source_today(key_text)
    start = _coverage_start(key_text, interval_text, end)
    existing = normalize_points(existing_points or [], key=key_text)
    incremental = bool(existing)
    if key_text in ("us10y", "us30y"):
        fetch_start = (
            date(max(start.year, end.year - 1), 1, 1)
            if incremental and interval_text == "W"
            else date(end.year, 1, 1)
            if incremental
            else start
        )
        rows = _fetch_treasury_years(range(fetch_start.year, end.year + 1))
        fetched = normalize_points(
            [
                {"date": row.get("date"), "value": row.get(key_text)}
                for row in rows
                if isinstance(row, Mapping)
            ],
            key=key_text,
        )
        fetched = aggregate_period_end(fetched, interval_text)
        return _merge_period_points(
            existing,
            fetched,
            key=key_text,
            interval=interval_text,
            start=start,
            end=end,
        )

    if key_text in ("cn10y", "cn30y"):
        fetch_start = (
            date(max(start.year, end.year - 1), 1, 1)
            if incremental
            else start
        )
        rows = _fetch_chinabond_rows(fetch_start, end)
        fetched = normalize_points(
            [
                {"date": row.get("date"), "value": row.get(key_text)}
                for row in rows
                if isinstance(row, Mapping)
            ],
            key=key_text,
        )
        fetched = aggregate_period_end(fetched, interval_text)
        return _merge_period_points(
            existing,
            fetched,
            key=key_text,
            interval=interval_text,
            start=start,
            end=end,
        )

    if key_text == "gold":
        fetch_start = (
            date(max(start.year, end.year - 1), 1, 1)
            if incremental and interval_text == "W"
            else date(end.year, 1, 1)
            if incremental
            else start
        )
        daily = _fetch_gold_history_points(fetch_start, end)
        fetched = aggregate_period_end(daily, interval_text)
        return _merge_period_points(
            existing,
            fetched,
            key=key_text,
            interval=interval_text,
            start=start,
            end=end,
        )

    if key_text in ("cn_gold_reserves", "tr_gold_reserves"):
        official = fetch_china_history() if key_text == "cn_gold_reserves" else fetch_turkey_history()
        raw_points = official.get("points") if isinstance(official, Mapping) else None
        if not isinstance(raw_points, list):
            raise ValueError(f"{key_text} official history has no points")
        fetched = normalize_points(
            [
                {"date": point.get("date"), "value": point.get("tonnes")}
                for point in raw_points
                if isinstance(point, Mapping)
            ],
            key=key_text,
        )
        fetched = aggregate_period_end(fetched, interval_text)
        return _merge_period_points(
            existing,
            fetched,
            key=key_text,
            interval=interval_text,
            start=start,
            end=end,
        )

    if interval_text in ("D", "W"):
        fetch_start = (
            date(max(start.year, end.year - 1), 1, 1)
            if incremental and interval_text == "W"
            else date(end.year, 1, 1)
            if incremental
            else start
        )
        daily = _fetch_kofia_daily_points(fetch_start, end)
        fetched = aggregate_period_end(daily, interval_text)
        return _merge_period_points(
            existing,
            fetched,
            key=key_text,
            interval=interval_text,
            start=start,
            end=end,
        )

    overlap_start = max(start, date(max(start.year, end.year - 2), 1, 1))
    native_start = overlap_start if incremental else start
    native = _fetch_kofia_native_points(interval_text, native_start, end)
    overlap_daily = _fetch_kofia_daily_points(overlap_start, end)
    try:
        _validate_kofia_native_overlap(native, overlap_daily, interval_text)
        fetched = aggregate_period_end(native, interval_text)
    except ValueError:
        if incremental:
            fetched = aggregate_period_end(overlap_daily, interval_text)
        else:
            full_daily = (
                overlap_daily
                if overlap_start == start
                else _fetch_kofia_daily_points(start, end)
            )
            fetched = aggregate_period_end(full_daily, interval_text)
    return _merge_period_points(
        existing,
        fetched,
        key=key_text,
        interval=interval_text,
        start=start,
        end=end,
    )


def _fetch_gold_history_points(fetch_start, fetch_end):
    """Fetch daily gold futures prices from Yahoo Finance for a date range."""
    from datetime import timezone as _dt_tz
    import urllib.parse as _up

    epoch_start = int(datetime.combine(fetch_start, datetime.min.time()).replace(tzinfo=_dt_tz.utc).timestamp())
    epoch_end = int(datetime.combine(fetch_end, datetime.min.time()).replace(tzinfo=_dt_tz.utc).timestamp())
    q = _up.urlencode({"period1": str(epoch_start), "period2": str(epoch_end), "interval": "1d"})
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/GC=F?{q}"
    req = Request(url, headers={"Accept": "application/json", "User-Agent": "StockSignalDashboard/0.5.6"})
    raw_body, ct = _read_limited(req)
    txt = raw_body.decode("utf-8-sig")
    if "html" in ct or not txt.lstrip().startswith("{"):
        raise ValueError("Gold history response is not JSON")
    try:
        pl = json.loads(txt)
    except json.JSONDecodeError as e:
        raise ValueError("Gold history response is invalid JSON") from e
    ch = pl.get("chart") if isinstance(pl, Mapping) else None
    rs = ch.get("result") if isinstance(ch, Mapping) else None
    if not isinstance(rs, list) or not rs:
        raise ValueError("Gold history has no result")
    r0 = rs[0]
    ts_list = r0.get("timestamp", [])
    qs = r0.get("indicators", {}).get("quote", [])
    q0 = qs[0] if isinstance(qs, list) and qs and isinstance(qs[0], Mapping) else {}
    cs = q0.get("close") or []
    pts = []
    for ts, cl in zip(ts_list, cs):
        if cl is None:
            continue
        try:
            v = float(str(cl).strip().replace(",", ""))
        except (ValueError, TypeError):
            continue
        if v <= 0:
            continue
        d = datetime.fromtimestamp(ts, tz=ZoneInfo("America/New_York")).date().isoformat()
        pts.append({"date": d, "value": v})
    return normalize_points(pts, key="gold")



def _series_id(key: str, interval: str) -> str:
    key_text, interval_text = validate_history_request(key, interval)
    return f"{key_text}:{interval_text}"


def _generated_at(epoch: float) -> str:
    return datetime.fromtimestamp(epoch).astimezone().strftime("%Y-%m-%d %H:%M:%S")


def _valid_series_record(series_id: str, raw: Any) -> dict[str, Any] | None:
    try:
        key, interval = series_id.rsplit(":", 1)
        validate_history_request(key, interval)
        if not isinstance(raw, Mapping):
            return None
        epoch = float(raw.get("generatedEpoch"))
        generated_at = raw.get("generatedAt")
        if not math.isfinite(epoch) or epoch <= 0 or not isinstance(generated_at, str) or not generated_at:
            return None
        raw_points = raw.get("points")
        if not isinstance(raw_points, list):
            return None
        points = normalize_points(raw_points, key=key)
        if not points or len(points) != len(raw_points) or points != raw_points:
            return None
        if interval != "D" and aggregate_period_end(points, interval) != points:
            return None
        return {
            "generatedEpoch": epoch,
            "generatedAt": generated_at,
            "points": points,
        }
    except (TypeError, ValueError):
        return None


def _load_history_cache() -> dict[str, Any]:
    try:
        raw = json.loads(Path(_HISTORY_CACHE_PATH).read_text(encoding="utf-8"))
        if not isinstance(raw, Mapping) or raw.get("schema") != _HISTORY_SCHEMA:
            return {"schema": _HISTORY_SCHEMA, "series": {}}
        raw_series = raw.get("series")
        if not isinstance(raw_series, Mapping):
            return {"schema": _HISTORY_SCHEMA, "series": {}}
        series = {}
        for series_id, candidate in raw_series.items():
            valid = _valid_series_record(str(series_id), candidate)
            if valid is not None:
                series[str(series_id)] = valid
        return {"schema": _HISTORY_SCHEMA, "series": series}
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return {"schema": _HISTORY_SCHEMA, "series": {}}


def _history_cache() -> dict[str, Any]:
    global _HISTORY_MEMORY
    with _HISTORY_CACHE_LOCK:
        if _HISTORY_MEMORY is None:
            _HISTORY_MEMORY = _load_history_cache()
        return _HISTORY_MEMORY


def _write_history_cache_locked() -> None:
    record = _history_cache()
    payload = {"schema": _HISTORY_SCHEMA, "series": record["series"]}
    cache_path = Path(_HISTORY_CACHE_PATH)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
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


def _source_error(key: str) -> dict[str, str]:
    return {
        "source": _KEY_META[key]["source"],
        "message": "历史数据源暂时不可用，请稍后重试",
    }


def _cache_error() -> dict[str, str]:
    return {
        "source": "Macro telemetry history cache",
        "message": "历史缓存暂时无法写入",
    }


def _claim_flight(series_id: str) -> tuple[threading.Event, bool]:
    with _HISTORY_CACHE_LOCK:
        existing = _HISTORY_FLIGHTS.get(series_id)
        if existing is not None:
            return existing, False
        event = threading.Event()
        _HISTORY_FLIGHTS[series_id] = event
        return event, True


def _finish_flight(series_id: str, event: threading.Event) -> None:
    with _HISTORY_CACHE_LOCK:
        if _HISTORY_FLIGHTS.get(series_id) is event:
            _HISTORY_FLIGHTS.pop(series_id, None)
        _HISTORY_THREADS.pop(series_id, None)
        event.set()


def _refresh_owned(series_id: str, key: str, interval: str, event: threading.Event) -> None:
    global _HISTORY_MEMORY
    try:
        try:
            with _HISTORY_CACHE_LOCK:
                cached = _history_cache()["series"].get(series_id)
                existing_points = (
                    [dict(point) for point in cached.get("points", [])]
                    if isinstance(cached, Mapping)
                    else []
                )
            points = normalize_points(
                _fetch_source_points(
                    key,
                    interval,
                    existing_points=existing_points,
                ),
                key=key,
            )
            if not points:
                raise ValueError("history source returned no valid points")
            if interval != "D":
                points = aggregate_period_end(points, interval)
            now = time.time()
            record = {
                "generatedEpoch": now,
                "generatedAt": _generated_at(now),
                "points": points,
            }
            with _HISTORY_CACHE_LOCK:
                cache = _history_cache()
                cache["series"][series_id] = record
                _HISTORY_ERRORS.pop(series_id, None)
                try:
                    _write_history_cache_locked()
                except OSError:
                    _HISTORY_ERRORS[series_id] = _cache_error()
        except Exception:
            with _HISTORY_CACHE_LOCK:
                _HISTORY_ERRORS[series_id] = _source_error(key)
    finally:
        _finish_flight(series_id, event)


def _refresh_series_sync(series_id: str, key: str, interval: str) -> None:
    event, owner = _claim_flight(series_id)
    if owner:
        _refresh_owned(series_id, key, interval, event)
    else:
        event.wait()


def _start_background_refresh(series_id: str, key: str, interval: str) -> None:
    event, owner = _claim_flight(series_id)
    if not owner:
        return
    thread = threading.Thread(
        target=_refresh_owned,
        args=(series_id, key, interval, event),
        name=f"macro-history-{series_id.replace(':', '-').lower()}",
        daemon=True,
    )
    with _HISTORY_CACHE_LOCK:
        _HISTORY_THREADS[series_id] = thread
    thread.start()


def _is_refreshing(series_id: str) -> bool:
    with _HISTORY_CACHE_LOCK:
        return series_id in _HISTORY_FLIGHTS


def _series_snapshot(series_id: str) -> tuple[dict[str, Any] | None, dict[str, str] | None]:
    with _HISTORY_CACHE_LOCK:
        series = _history_cache()["series"].get(series_id)
        error = _HISTORY_ERRORS.get(series_id)
        return (dict(series) if isinstance(series, Mapping) else None, dict(error) if error else None)


def _history_payload(
    *,
    key: str,
    interval: str,
    series_id: str,
    series: Mapping[str, Any] | None,
    error: Mapping[str, Any] | None,
    stale: bool,
    state: str,
) -> dict[str, Any]:
    if series is None:
        generated_at = _generated_at(time.time())
        points: list[dict[str, Any]] = []
        age_seconds = None
    else:
        epoch = float(series["generatedEpoch"])
        generated_at = str(series["generatedAt"])
        points = [dict(point) for point in series["points"]]
        age_seconds = max(0, int(time.time() - epoch))
    return public_history_payload(
        key=key,
        interval=interval,
        generated_at=generated_at,
        points=points,
        stale=stale,
        errors=[dict(error)] if error else [],
        cache={
            "state": state,
            "ageSeconds": age_seconds,
            "refreshing": _is_refreshing(series_id),
        },
    )


def build_macro_telemetry_history(
    key: Any,
    interval: Any,
    force_refresh: bool = False,
    allow_stale: bool = True,
) -> dict[str, Any]:
    key_text, interval_text = validate_history_request(key, interval)
    series_id = _series_id(key_text, interval_text)
    series, error = _series_snapshot(series_id)
    if force_refresh or not allow_stale or series is None:
        _refresh_series_sync(series_id, key_text, interval_text)
        series, error = _series_snapshot(series_id)
        stale = series is not None and error is not None
        state = "miss" if series is None else ("stale" if stale else "fresh")
        return _history_payload(
            key=key_text,
            interval=interval_text,
            series_id=series_id,
            series=series,
            error=error,
            stale=stale,
            state=state,
        )

    age = max(0.0, time.time() - float(series["generatedEpoch"]))
    if age < max(0, _HISTORY_TTL_SECONDS):
        return _history_payload(
            key=key_text,
            interval=interval_text,
            series_id=series_id,
            series=series,
            error=error,
            stale=False,
            state="fresh",
        )

    _start_background_refresh(series_id, key_text, interval_text)
    series, error = _series_snapshot(series_id)
    return _history_payload(
        key=key_text,
        interval=interval_text,
        series_id=series_id,
        series=series,
        error=error,
        stale=True,
        state="stale",
    )


def _wait_for_history_refresh_for_tests(series_id: str | None = None, timeout: float = 2) -> None:
    with _HISTORY_CACHE_LOCK:
        threads = [
            thread
            for candidate_id, thread in _HISTORY_THREADS.items()
            if series_id is None or candidate_id == series_id
        ]
    deadline = time.monotonic() + max(0, timeout)
    for thread in threads:
        thread.join(max(0, deadline - time.monotonic()))


def _reset_history_cache_for_tests() -> None:
    global _HISTORY_MEMORY
    _wait_for_history_refresh_for_tests(timeout=2)
    with _HISTORY_CACHE_LOCK:
        _HISTORY_MEMORY = None
        _HISTORY_ERRORS.clear()
        _HISTORY_FLIGHTS.clear()
        _HISTORY_THREADS.clear()
