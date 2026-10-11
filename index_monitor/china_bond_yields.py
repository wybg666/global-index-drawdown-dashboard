"""Official ChinaBond government-bond yield-curve collector.

The source publishes one curve per valuation date.  This module deliberately
extracts only the 10-year and 30-year nodes from the official curve, rather
than treating either value as a quote for a single on-the-run bond.
"""

from __future__ import annotations

import json
import math
from datetime import date, datetime
from io import BytesIO
from typing import Any, Callable, Mapping
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from xml.etree import ElementTree
from zipfile import BadZipFile, ZipFile


__all__ = [
    "CHINABOND_GOVERNMENT_CURVE_ID",
    "CHINABOND_HISTORY_DOWN_URL",
    "CHINABOND_QUERY_XYZ_URL",
    "fetch_chinabond_yield_rows",
    "parse_chinabond_history_xlsx",
    "parse_chinabond_query_json",
]


CHINABOND_QUERY_XYZ_URL = "https://yield.chinabond.com.cn/cbweb-mn/yc/queryXyz"
CHINABOND_HISTORY_DOWN_URL = "https://yield.chinabond.com.cn/cbweb-pbc-web/pbc/historyDown"
CHINABOND_GOVERNMENT_CURVE_ID = "2c9081e50a2f9606010a3068cae70001"

DEFAULT_TIMEOUT_SECONDS = 20.0
MAX_RESPONSE_BYTES = 5 * 1024 * 1024
_YIELD_MIN = 0.0
_YIELD_MAX = 20.0
_HISTORY_SHEET_PATH = "xl/worksheets/sheet1.xml"
_SHARED_STRINGS_PATH = "xl/sharedStrings.xml"
_GOVERNMENT_CURVE_NAME = "中债国债收益率曲线"
_HISTORY_REQUIRED_HEADERS = ("曲线名称", "日期", "10年", "30年")
HttpPost = Callable[..., str | bytes]
HttpGet = Callable[..., str | bytes]


def _checked_timeout(timeout: float) -> float:
    try:
        result = float(timeout)
    except (TypeError, ValueError) as exc:
        raise ValueError("timeout must be a positive number") from exc
    if not math.isfinite(result) or result <= 0:
        raise ValueError("timeout must be a positive number")
    return result


def _as_date(value: Any) -> str | None:
    text = str(value or "").strip()
    try:
        return datetime.strptime(text, "%Y-%m-%d").date().isoformat()
    except ValueError:
        return None


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(str(value).strip().replace(",", ""))
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _yield(value: Any) -> float | None:
    result = _number(value)
    if result is None or not _YIELD_MIN < result < _YIELD_MAX:
        return None
    return result


def _tenor_index(tenors: list[Any], target: float) -> int | None:
    for index, tenor in enumerate(tenors):
        value = _number(tenor)
        if value is not None and math.isclose(value, target, rel_tol=0.0, abs_tol=1e-8):
            return index
    return None


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _xlsx_cell_text(cell: ElementTree.Element, shared_strings: list[str]) -> str:
    cell_type = str(cell.attrib.get("t") or "")
    if cell_type == "s":
        value_node = next((node for node in cell if _local_name(node.tag) == "v"), None)
        try:
            return shared_strings[int((value_node.text or "").strip())].strip() if value_node is not None else ""
        except (IndexError, TypeError, ValueError):
            return ""
    if cell_type == "inlineStr":
        inline = next((node for node in cell if _local_name(node.tag) == "is"), None)
        return "".join(inline.itertext()).strip() if inline is not None else ""
    value_node = next((node for node in cell if _local_name(node.tag) == "v"), None)
    return str(value_node.text or "").strip() if value_node is not None else ""


def _xlsx_row_cells(row: ElementTree.Element, shared_strings: list[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for cell in row:
        if _local_name(cell.tag) != "c":
            continue
        reference = str(cell.attrib.get("r") or "")
        column = "".join(character for character in reference if character.isalpha()).upper()
        if column:
            result[column] = _xlsx_cell_text(cell, shared_strings)
    return result


def _xlsx_shared_strings(archive: ZipFile) -> list[str]:
    try:
        info = archive.getinfo(_SHARED_STRINGS_PATH)
    except KeyError:
        return []
    if info.file_size > MAX_RESPONSE_BYTES:
        raise ValueError("ChinaBond XLSX shared strings exceed size limit")
    try:
        root = ElementTree.fromstring(archive.read(info))
    except ElementTree.ParseError as exc:
        raise ValueError("ChinaBond XLSX shared strings are invalid") from exc
    return ["".join(item.itertext()).strip() for item in root if _local_name(item.tag) == "si"]


def parse_chinabond_history_xlsx(raw: bytes | bytearray | memoryview) -> list[dict[str, Any]]:
    """Normalize a ChinaBond ``historyDown`` XLSX download to 10Y/30Y rows."""

    if not isinstance(raw, (bytes, bytearray, memoryview)):
        raise ValueError("ChinaBond XLSX response must be bytes")
    payload = bytes(raw)
    if len(payload) > MAX_RESPONSE_BYTES:
        raise ValueError("ChinaBond XLSX response exceeds size limit")
    if not payload.startswith(b"PK"):
        raise ValueError("ChinaBond history download is not an XLSX file")
    try:
        with ZipFile(BytesIO(payload)) as archive:
            sheet_info = archive.getinfo(_HISTORY_SHEET_PATH)
            if sheet_info.file_size > MAX_RESPONSE_BYTES:
                raise ValueError("ChinaBond XLSX worksheet exceeds size limit")
            shared_strings = _xlsx_shared_strings(archive)
            sheet = archive.read(sheet_info)
    except (BadZipFile, KeyError, OSError) as exc:
        raise ValueError("ChinaBond history download is not a usable XLSX file") from exc
    try:
        root = ElementTree.fromstring(sheet)
    except ElementTree.ParseError as exc:
        raise ValueError("ChinaBond XLSX worksheet is invalid") from exc

    header_columns: dict[str, str] | None = None
    by_date: dict[str, dict[str, Any]] = {}
    for row in (element for element in root.iter() if _local_name(element.tag) == "row"):
        cells = _xlsx_row_cells(row, shared_strings)
        if header_columns is None:
            candidate = {value: column for column, value in cells.items() if value}
            if all(header in candidate for header in _HISTORY_REQUIRED_HEADERS):
                header_columns = candidate
            continue
        curve_name = cells.get(header_columns["曲线名称"], "").strip()
        date_text = _as_date(cells.get(header_columns["日期"]))
        ten_year = _yield(cells.get(header_columns["10年"]))
        thirty_year = _yield(cells.get(header_columns["30年"]))
        if curve_name != _GOVERNMENT_CURVE_NAME or date_text is None or ten_year is None or thirty_year is None:
            continue
        by_date[date_text] = {"date": date_text, "cn10y": ten_year, "cn30y": thirty_year}
    if not by_date:
        raise ValueError("ChinaBond XLSX has no valid 10Y/30Y government-yield rows")
    return [by_date[day] for day in sorted(by_date)]


def parse_chinabond_query_json(raw: str | bytes | Any) -> list[dict[str, Any]]:
    """Normalize the official ``queryXyz`` JSON to date/10Y/30Y observations."""

    if isinstance(raw, bytes):
        if len(raw) > MAX_RESPONSE_BYTES:
            raise ValueError("ChinaBond response exceeds size limit")
        try:
            raw = raw.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            raise ValueError("ChinaBond response is not UTF-8 JSON") from exc
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError("ChinaBond response is not valid JSON") from exc
    if not isinstance(raw, list):
        raise ValueError("ChinaBond response must be a JSON list")

    by_date: dict[str, dict[str, Any]] = {}
    for record in raw:
        if not isinstance(record, Mapping):
            continue
        date_text = _as_date(record.get("workTime"))
        tenors = record.get("bzqx")
        yields = record.get("syl")
        if date_text is None or not isinstance(tenors, list) or not isinstance(yields, list):
            continue
        if len(tenors) != len(yields):
            continue
        ten_index = _tenor_index(tenors, 10.0)
        thirty_index = _tenor_index(tenors, 30.0)
        if ten_index is None or thirty_index is None:
            continue
        ten_year = _yield(yields[ten_index])
        thirty_year = _yield(yields[thirty_index])
        if ten_year is None or thirty_year is None:
            continue
        by_date[date_text] = {
            "date": date_text,
            "cn10y": ten_year,
            "cn30y": thirty_year,
        }
    if not by_date:
        raise ValueError("ChinaBond response has no valid 10Y/30Y government-yield rows")
    return [by_date[day] for day in sorted(by_date)]


def _default_post(url: str, *, timeout: float) -> bytes:
    request = Request(
        url,
        data=b"",
        method="POST",
        headers={
            "Accept": "application/json",
            "User-Agent": "StockSignalDashboard/0.5.9",
        },
    )
    with urlopen(request, timeout=timeout) as response:  # nosec B310 -- fixed official source URL
        status = int(getattr(response, "status", 200) or 200)
        if status >= 400:
            raise RuntimeError(f"ChinaBond HTTP {status}")
        payload = response.read(MAX_RESPONSE_BYTES + 1)
    if len(payload) > MAX_RESPONSE_BYTES:
        raise ValueError("ChinaBond response exceeds size limit")
    return payload


def _default_get(url: str, *, timeout: float) -> bytes:
    request = Request(
        url,
        method="GET",
        headers={
            "Accept": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*;q=0.8",
            "User-Agent": "StockSignalDashboard/0.5.9",
        },
    )
    with urlopen(request, timeout=timeout) as response:  # nosec B310 -- fixed official source URL
        status = int(getattr(response, "status", 200) or 200)
        if status >= 400:
            raise RuntimeError(f"ChinaBond HTTP {status}")
        payload = response.read(MAX_RESPONSE_BYTES + 1)
    if len(payload) > MAX_RESPONSE_BYTES:
        raise ValueError("ChinaBond response exceeds size limit")
    return payload


def _query_xyz_url(start: date, end: date) -> str:
    query = urlencode(
        {
            "startTime": start.isoformat(),
            "endTime": end.isoformat(),
            "qxlx": "0,",
            "yqqxN": "N",
            "yqqxK": "K",
            "ycDefIds": f"{CHINABOND_GOVERNMENT_CURVE_ID},",
            "locale": "en_US",
        }
    )
    return f"{CHINABOND_QUERY_XYZ_URL}?{query}"


def _history_download_url(start: date, end: date) -> str:
    query = urlencode(
        {
            "startDate": start.isoformat(),
            "endDate": end.isoformat(),
            "gjqx": "0",
            "qxId": "hzsylqx",
            "locale": "cn_ZH",
        }
    )
    return f"{CHINABOND_HISTORY_DOWN_URL}?{query}"


def fetch_chinabond_yield_rows(
    start: date,
    end: date,
    *,
    http_get: HttpGet | None = None,
    http_post: HttpPost | None = None,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> list[dict[str, Any]]:
    """Fetch one <=365-day ChinaBond government-yield window.

    The smaller official XLSX download is the primary source.  ``queryXyz``
    remains a fallback for an unusable download.  Passing only ``http_post``
    keeps the prior injected-queryXyz test/client contract intact.
    """

    if not isinstance(start, date) or not isinstance(end, date):
        raise ValueError("start and end must be dates")
    if start > end:
        raise ValueError("start must not be after end")
    if (end - start).days > 365:
        raise ValueError("ChinaBond query range must not exceed 365 days")
    timeout = _checked_timeout(timeout)
    query_url = _query_xyz_url(start, end)

    # A caller that supplied only the old post injection explicitly selected
    # the legacy seam; avoid replacing its deterministic fixture with network.
    if http_get is None and http_post is not None:
        return parse_chinabond_query_json(http_post(query_url, timeout=timeout))

    get = _default_get if http_get is None else http_get
    try:
        return parse_chinabond_history_xlsx(get(_history_download_url(start, end), timeout=timeout))
    except Exception as history_error:
        post = _default_post if http_post is None else http_post
        try:
            return parse_chinabond_query_json(post(query_url, timeout=timeout))
        except Exception as fallback_error:
            raise RuntimeError(
                "ChinaBond history download and queryXyz fallback both failed: "
                f"{history_error}; {fallback_error}"
            ) from fallback_error
