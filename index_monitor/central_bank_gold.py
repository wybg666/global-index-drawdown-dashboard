"""Official China and Türkiye central-bank gold-reserve history providers.

The collectors deliberately return reserve *holdings* rather than calling a
change a purchase.  Both official series include deposits/swaps in their
published definitions, so a positive change is a holdings trend only.
"""

from __future__ import annotations

import calendar
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import math
import re
from datetime import date
from io import BytesIO
from html.parser import HTMLParser
from typing import Any, Callable, Mapping
from urllib.parse import urljoin
from urllib.request import Request, urlopen


__all__ = [
    "SAFE_INDEX_URL",
    "TCMB_EVDS_DATA_URL",
    "TCMB_GOLD_VOLUME_SERIES",
    "fetch_china_history",
    "fetch_china_current",
    "fetch_turkey_history",
]


SAFE_INDEX_URL = "https://www.safe.gov.cn/en/ForexReserves/index.html"
SAFE_BASE_URL = "https://www.safe.gov.cn"
TCMB_EVDS_DATA_URL = "https://evds3.tcmb.gov.tr/igmevdsms-dis/fe"
TCMB_GOLD_VOLUME_SERIES = "TP.REZVARPD.K11"

DEFAULT_TIMEOUT_SECONDS = 20.0
MAX_RESPONSE_BYTES = 5 * 1024 * 1024
TONNES_PER_MILLION_FINE_TROY_OUNCES = 31.1034768
TONNES_PER_TEN_THOUSAND_TROY_OUNCES = 0.311034768

_YEAR_RE = re.compile(r"(?:19|20)\d{2}")
_MONTH_RE = re.compile(r"\b((?:19|20)\d{2})\s*[./-]\s*(0?[1-9]|1[0-2])\b")
_ISO_MONTH_RE = re.compile(r"^((?:19|20)\d{2})-(0[1-9]|1[0-2])$")
_TEN_THOUSAND_OUNCE_RE = re.compile(r"(-?\d+(?:\.\d+)?)\s*万\s*盎司")

_ISO_DATE_RE = re.compile(r"^(?:19|20)\d{2}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\d|3[01])$")

HttpGet = Callable[..., str | bytes]
HttpPost = Callable[..., Mapping[str, Any] | str | bytes]


class _AnchorParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: list[tuple[str, str]] = []
        self._href: str | None = None
        self._text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() == "a":
            self._href = dict(attrs).get("href")
            self._text = []

    def handle_data(self, data: str) -> None:
        if self._href is not None:
            self._text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "a" and self._href is not None:
            self.links.append((self._href, " ".join(self._text)))
            self._href = None
            self._text = []


class _TableParser(HTMLParser):
    """Small HTML-table reader adequate for SAFE's Excel-exported pages."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.rows: list[list[tuple[str, int]]] = []
        self._row: list[tuple[str, int]] | None = None
        self._cell_parts: list[str] | None = None
        self._colspan = 1

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        lowered = tag.lower()
        if lowered == "tr":
            self._row = []
        elif lowered in {"td", "th"} and self._row is not None:
            values = dict(attrs)
            try:
                self._colspan = max(1, int(values.get("colspan") or "1"))
            except ValueError:
                self._colspan = 1
            self._cell_parts = []
        elif lowered == "br" and self._cell_parts is not None:
            self._cell_parts.append(" ")

    def handle_data(self, data: str) -> None:
        if self._cell_parts is not None:
            self._cell_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        lowered = tag.lower()
        if lowered in {"td", "th"} and self._row is not None and self._cell_parts is not None:
            self._row.append((" ".join(self._cell_parts), self._colspan))
            self._cell_parts = None
            self._colspan = 1
        elif lowered == "tr" and self._row is not None:
            self.rows.append(self._row)
            self._row = None


def _checked_timeout(timeout: float) -> float:
    try:
        result = float(timeout)
    except (TypeError, ValueError) as exc:
        raise ValueError("timeout must be a positive number") from exc
    if not math.isfinite(result) or result <= 0:
        raise ValueError("timeout must be a positive number")
    return result


def _default_get(url: str, *, timeout: float) -> bytes:
    request = Request(url, headers={"Accept": "text/html,*/*;q=0.8", "User-Agent": "StockSignalDashboard/1.0"})
    with urlopen(request, timeout=timeout) as response:  # nosec B310 -- fixed public source URLs
        payload = response.read(MAX_RESPONSE_BYTES + 1)
    if len(payload) > MAX_RESPONSE_BYTES:
        raise ValueError("source response exceeds size limit")
    return payload


def _default_post(url: str, payload: Mapping[str, Any], *, timeout: float) -> Mapping[str, Any]:
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    request = Request(
        url,
        data=body,
        method="POST",
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "StockSignalDashboard/1.0",
        },
    )
    with urlopen(request, timeout=timeout) as response:  # nosec B310 -- fixed public source URL
        raw = response.read(MAX_RESPONSE_BYTES + 1)
    if len(raw) > MAX_RESPONSE_BYTES:
        raise ValueError("source response exceeds size limit")
    try:
        decoded = json.loads(raw.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("TCMB response is not valid JSON") from exc
    if not isinstance(decoded, Mapping):
        raise ValueError("TCMB response must be a JSON object")
    return decoded


def _text(raw: str | bytes, source_name: str) -> str:
    if isinstance(raw, bytes):
        if len(raw) > MAX_RESPONSE_BYTES:
            raise ValueError(f"{source_name} response exceeds size limit")
        try:
            raw = raw.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            raise ValueError(f"{source_name} response is not UTF-8 HTML") from exc
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError(f"{source_name} response is empty or malformed")
    return raw


def _links(html: str) -> list[tuple[str, str]]:
    parser = _AnchorParser()
    parser.feed(html)
    parser.close()
    return parser.links


def _safe_year_pages(index_html: str) -> list[tuple[int, str]]:
    candidates: dict[int, str] = {}
    for href, label in _links(index_html):
        text = " ".join(label.split())
        if "official reserve assets" not in text.lower():
            continue
        years = [int(value) for value in _YEAR_RE.findall(text)]
        if not years:
            continue
        candidates[min(years)] = urljoin(SAFE_BASE_URL, href)
    return sorted(candidates.items())


def _safe_data_resources(entry_html: str, entry_url: str) -> list[tuple[str, str]]:
    """Return the official HTML/XLSX/XLS data files linked by one SAFE entry."""

    resources: list[tuple[str, str]] = []
    seen: set[str] = set()
    for href, label in _links(entry_html):
        target = urljoin(entry_url, href)
        lowered = target.lower().split("?", 1)[0]
        label_text = " ".join(label.lower().split())
        if lowered.endswith(".xlsx") and "official reserve asset" in label_text:
            kind = "xlsx"
        elif lowered.endswith(".xls") and "official reserve asset" in label_text:
            kind = "xls"
        elif (label_text == "html" or "/safe/" in lowered) and lowered.endswith(".html"):
            kind = "html"
        else:
            continue
        if target not in seen:
            resources.append((target, kind))
            seen.add(target)
    return resources


def _expanded_row(row: list[tuple[str, int]]) -> list[str]:
    output: list[str] = []
    for value, colspan in row:
        output.extend([" ".join(value.split())] * colspan)
    return output


def _month_positions(row: list[str]) -> dict[int, str]:
    positions: dict[int, str] = {}
    for position, cell in enumerate(row):
        match = _MONTH_RE.search(cell)
        if match:
            positions[position] = f"{match.group(1)}-{int(match.group(2)):02d}-01"
    return positions


def _safe_gold_points_from_rows(rows: list[list[str]]) -> dict[str, float]:
    gold_marker = chr(0x9EC4) + chr(0x91D1)
    points: dict[str, float] = {}
    for row_index, row in enumerate(rows):
        label = " ".join(row).lower()
        if gold_marker not in " ".join(row) and "gold" not in label:
            continue
        header: dict[int, str] = {}
        for candidate in reversed(rows[:row_index]):
            header = _month_positions(candidate)
            if header:
                break
        if not header:
            continue
        value_row: list[str] | None = None
        for candidate in rows[row_index : row_index + 5]:
            if any(_TEN_THOUSAND_OUNCE_RE.search(cell) for cell in candidate):
                value_row = candidate
                break
        if value_row is None:
            continue
        for position, period in header.items():
            if position >= len(value_row):
                continue
            match = _TEN_THOUSAND_OUNCE_RE.search(value_row[position])
            if not match:
                continue
            raw_ounces = float(match.group(1))
            tonnes = raw_ounces * TONNES_PER_TEN_THOUSAND_TROY_OUNCES
            if math.isfinite(tonnes) and tonnes > 0:
                points[period] = tonnes
        if points:
            break
    return points


def _safe_gold_points(page_html: str) -> dict[str, float]:
    parser = _TableParser()
    parser.feed(page_html)
    parser.close()
    return _safe_gold_points_from_rows([_expanded_row(row) for row in parser.rows])


def _binary(raw: str | bytes, source_name: str) -> bytes:
    if isinstance(raw, bytes):
        if not raw or len(raw) > MAX_RESPONSE_BYTES:
            raise ValueError(f"{source_name} response is empty, malformed, or too large")
        return raw
    raise ValueError(f"{source_name} must be a binary spreadsheet")


def _safe_spreadsheet_gold_points(raw: str | bytes, kind: str) -> dict[str, float]:
    binary = _binary(raw, f"SAFE {kind}")
    try:
        if kind == "xlsx":
            import openpyxl
            workbook = openpyxl.load_workbook(BytesIO(binary), read_only=True, data_only=True)
            sheets = [
                [["" if value is None else str(value) for value in row] for row in sheet.iter_rows(values_only=True)]
                for sheet in workbook.worksheets
            ]
        elif kind == "xls":
            import xlrd
            workbook = xlrd.open_workbook(file_contents=binary)
            sheets = [
                [["" if value is None else str(value) for value in sheet.row_values(index)] for index in range(sheet.nrows)]
                for sheet in workbook.sheets()
            ]
        else:
            raise ValueError("unsupported SAFE spreadsheet type")
    except ImportError as exc:
        raise RuntimeError(f"SAFE {kind} parser dependency is missing") from exc
    except Exception as exc:
        raise ValueError(f"SAFE {kind} spreadsheet is unreadable") from exc
    points: dict[str, float] = {}
    for rows in sheets:
        points.update(_safe_gold_points_from_rows(rows))
    return points


def _fetch_safe_entry(entry_url: str, get: HttpGet, timeout: float) -> dict[str, float]:
    entry_html = _text(get(entry_url, timeout=timeout), "SAFE annual page")
    resources = _safe_data_resources(entry_html, entry_url)
    if not resources:
        return {}
    points: dict[str, float] = {}
    for resource_url, kind in resources:
        raw = get(resource_url, timeout=timeout)
        if kind == "html":
            points.update(_safe_gold_points(_text(raw, "SAFE data page")))
        else:
            points.update(_safe_spreadsheet_gold_points(raw, kind))
    return points


def _month_to_iso(value: Any) -> str | None:
    match = _ISO_MONTH_RE.fullmatch(str(value).strip())
    if not match:
        return None
    return f"{match.group(1)}-{match.group(2)}-01"


def _finite_positive(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    text = str(value).strip().replace(",", "")
    if not text or text.upper() in {"NA", "N/A", "ND", "NULL", "NONE"}:
        return None
    try:
        number = float(text)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) and number > 0 else None


def _bundle(country: str, source: str, source_url: str, frequency: str, points_by_date: Mapping[str, float]) -> dict[str, Any]:
    points = [
        {"date": point_date, "tonnes": tonnes}
        for point_date, tonnes in sorted(points_by_date.items())
        if _ISO_DATE_RE.fullmatch(point_date) and math.isfinite(tonnes) and tonnes > 0
    ]
    if not points:
        raise ValueError(f"{country} official gold history has no valid points")
    latest = points[-1]
    previous = points[-2] if len(points) > 1 else None
    change = None if previous is None else latest["tonnes"] - previous["tonnes"]
    trend = None if change is None else ("up" if change > 0 else "down" if change < 0 else "flat")
    return {
        "country": country,
        "source": source,
        "sourceUrl": source_url,
        "frequency": frequency,
        "unit": "tonnes",
        "points": points,
        "snapshot": {
            "value": latest["tonnes"],
            "asOf": latest["date"],
            "previousValue": None if previous is None else previous["tonnes"],
            "previousAsOf": None if previous is None else previous["date"],
            "change": change,
            "trend": trend,
        },
    }


def _fetch_china_history(
    *,
    http_get: HttpGet | None = None,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    latest_only: bool = False,
) -> dict[str, Any]:
    timeout = _checked_timeout(timeout)
    get = _default_get if http_get is None else http_get
    index_html = _text(get(SAFE_INDEX_URL, timeout=timeout), "SAFE index")
    year_pages = _safe_year_pages(index_html)
    if not year_pages:
        raise ValueError("SAFE index has no official reserve-assets pages")
    if latest_only:
        year_pages = year_pages[-1:]

    points: dict[str, float] = {}
    failures: list[str] = []
    with ThreadPoolExecutor(max_workers=min(4, len(year_pages)), thread_name_prefix="safe-gold") as executor:
        futures = {
            executor.submit(_fetch_safe_entry, entry_url, get, timeout): year
            for year, entry_url in year_pages
        }
        for future in as_completed(futures):
            year = futures[future]
            try:
                points.update(future.result())
            except Exception as exc:
                failures.append(f"{year}: {exc}")
    if not points:
        detail = "; ".join(sorted(failures))
        raise ValueError("SAFE official gold history has no readable annual data" + (f": {detail}" if detail else ""))
    if failures and not latest_only:
        raise ValueError("SAFE official gold history is incomplete: " + "; ".join(sorted(failures)))
    return _bundle(
        "CN",
        "State Administration of Foreign Exchange (SAFE)",
        SAFE_INDEX_URL,
        "monthly",
        points,
    )


def fetch_china_history(*, http_get: HttpGet | None = None, timeout: float = DEFAULT_TIMEOUT_SECONDS) -> dict[str, Any]:
    """Fetch all readable official SAFE annual tables and normalize gold holdings."""
    return _fetch_china_history(http_get=http_get, timeout=timeout, latest_only=False)


def fetch_china_current(*, http_get: HttpGet | None = None, timeout: float = DEFAULT_TIMEOUT_SECONDS) -> dict[str, Any]:
    """Fetch the latest official SAFE annual table for the dashboard's live card."""
    return _fetch_china_history(http_get=http_get, timeout=timeout, latest_only=True)


def _turkey_payload() -> dict[str, Any]:
    return {
        "type": "json",
        "series": TCMB_GOLD_VOLUME_SERIES,
        "aggregationTypes": "last",
        "formulas": "0",
        "startDate": "01-01-2000",
        "endDate": date.today().strftime("%d-%m-%Y"),
        "frequency": "5",
        "decimalSeperator": ".",
        "decimal": "3",
        "dateFormat": "0",
        "lang": "en",
        "yon": "1",
        "sira": "1",
        "ozelFormuller": [],
        "groupSeperator": True,
        "isRaporSayfasi": False,
    }


def fetch_turkey_history(*, http_post: HttpPost | None = None, timeout: float = DEFAULT_TIMEOUT_SECONDS) -> dict[str, Any]:
    """Fetch TCMB EVDS3 monthly official gold-volume history without an API key."""

    timeout = _checked_timeout(timeout)
    post = _default_post if http_post is None else http_post
    response = post(TCMB_EVDS_DATA_URL, _turkey_payload(), timeout=timeout)
    if isinstance(response, (str, bytes)):
        try:
            response = json.loads(_text(response, "TCMB EVDS3"))
        except json.JSONDecodeError as exc:
            raise ValueError("TCMB response is not valid JSON") from exc
    if not isinstance(response, Mapping):
        raise ValueError("TCMB response must be a JSON object")
    rows = response.get("items")
    if not isinstance(rows, list):
        raise ValueError("TCMB response has no items list")

    points: dict[str, float] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        period = _month_to_iso(row.get("Tarih"))
        million_ounces = _finite_positive(
            row.get(TCMB_GOLD_VOLUME_SERIES)
            if TCMB_GOLD_VOLUME_SERIES in row
            else row.get(TCMB_GOLD_VOLUME_SERIES.replace(".", "_"))
        )
        if period is None or million_ounces is None:
            continue
        points[period] = million_ounces * TONNES_PER_MILLION_FINE_TROY_OUNCES
    return _bundle(
        "TR",
        "Central Bank of the Republic of Türkiye (TCMB) EVDS3",
        TCMB_EVDS_DATA_URL,
        "monthly",
        points,
    )
