"""Small, read-only public market-data adapters used by the index dashboard.

These are undocumented website endpoints, not a brokerage or trading API.
"""
from __future__ import annotations

import json
import math
import re
import time
import urllib.parse
import urllib.request
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

TENCENT_QUOTE_URL = "https://qt.gtimg.cn/q="
TENCENT_KLINE_URL = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
TENCENT_KLINE_FALLBACK_URL = "https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get"
QUOTE_FIELDS = ("code", "name", "price", "previousClose", "change", "pct", "high", "low", "quoteTimestamp")


class ProviderError(RuntimeError):
    pass


def _num(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _int(value: Any) -> int | None:
    number = _num(value)
    return int(number) if number is not None else None


def _read(url: str, *, timeout: int = 12) -> bytes:
    request = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://gu.qq.com/",
        "Accept": "application/json,text/plain,*/*",
    })
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read()


def _http_json(url: str, params: dict[str, Any], timeout: int = 12, retries: int = 2) -> dict[str, Any]:
    query = urllib.parse.urlencode(params, doseq=True)
    full_url = f"{url}?{query}" if query else url
    error: Exception | None = None
    for attempt in range(max(1, retries)):
        try:
            raw = _read(full_url, timeout=timeout)
            try:
                text = raw.decode("utf-8")
            except UnicodeDecodeError:
                text = raw.decode("gb18030")
            payload = json.loads(text)
            if not isinstance(payload, dict):
                raise ValueError("expected a JSON object")
            return payload
        except Exception as exc:
            error = exc
            if attempt + 1 < max(1, retries):
                time.sleep(0.35 + attempt * 0.25)
    raise ProviderError(f"Public market-data request failed: {error}")


def _quote_timestamp(value: str, code: str) -> int | None:
    timezone = ZoneInfo("Asia/Hong_Kong" if code.startswith("hk") else "Asia/Shanghai")
    for format_ in ("%Y%m%d%H%M%S", "%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            parsed = datetime.strptime(value.strip(), format_).replace(tzinfo=timezone)
            return int(parsed.timestamp())
        except ValueError:
            continue
    return None


def fetch_tencent_quotes(codes: list[str]) -> dict[str, dict[str, Any]]:
    """Normalize public quote fields; never substitute a request time for quote time."""
    requested = list(dict.fromkeys(codes))
    if not requested:
        return {}
    if any(re.fullmatch(r"(?:sh|sz)[0-9]{6}|hk[A-Z0-9]+", code) is None for code in requested):
        raise ValueError("Unsupported Tencent index code")
    raw = _read(TENCENT_QUOTE_URL + ",".join(requested))
    text = raw.decode("gb18030")
    quotes: dict[str, dict[str, Any]] = {}
    for match in re.finditer(r'v_([A-Za-z0-9]+)="([^"\r\n]*)";', text):
        code, body = match.groups()
        if code not in requested:
            continue
        parts = body.split("~")
        if len(parts) <= 34 or parts[2].upper() != code[2:].upper():
            continue
        price = _num(parts[3])
        timestamp = _quote_timestamp(parts[30], code)
        if price is None or price <= 0 or timestamp is None:
            continue
        quotes[code] = {
            "code": code, "name": parts[1], "price": price,
            "previousClose": _num(parts[4]), "change": _num(parts[31]),
            "pct": _num(parts[32]), "high": _num(parts[33]),
            "low": _num(parts[34]), "quoteTimestamp": timestamp,
            "source": "tencent",
        }
    if not quotes:
        raise ProviderError("Tencent returned no valid requested index quotes")
    return quotes
