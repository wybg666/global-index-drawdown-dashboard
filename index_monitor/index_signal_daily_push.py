"""Daily CN index-judgment snapshots and delivery state.

The 14:55 report is a forced, once-per-channel snapshot.  The 15:10 report is
sent only when its normalized judgment differs, or when the tail report never
reached the configured channel.  This module deliberately stays independent
from transition events: a daily report is not a fabricated lifecycle change.
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable


SCHEMA_VERSION = 1
SNAPSHOT_SCHEMA = "index_signal_daily_judgment/v1"
FINGERPRINT_VERSION = "index_signal_daily_judgment_fingerprint/v2"
MAX_RETAINED_MARKET_DATES = 30
TIMEFRAME_ORDER = ("short", "medium", "long")
VALID_STATUSES = {"none", "warning", "confirmed", "invalidated", "expired", "conflict"}
VALID_LIFECYCLES = {"none", "candidate", "resonant", "confirmed", "invalidated", "expired"}
ACTIVE_JUDGMENT_STATUSES = frozenset({"warning", "confirmed"})


def _copy(value: Any) -> Any:
    return copy.deepcopy(value)


def _text(value: Any) -> str:
    return str(value or "").strip()


def _number(value: Any) -> float | None:
    try:
        if value is None or isinstance(value, bool):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _rounded(value: Any, digits: int = 2) -> float | None:
    number = _number(value)
    return round(number, digits) if number is not None else None


def _score(value: Any) -> int:
    number = _number(value)
    return max(0, int(round(number))) if number is not None else 0


def _effective_status(frame: dict[str, Any]) -> tuple[str, str]:
    status = _text(frame.get("candidateStatus") or frame.get("status") or "none").lower()
    lifecycle = _text(frame.get("lifecycleState") or "").lower()
    if status not in VALID_STATUSES:
        status = "none"
    if lifecycle not in VALID_LIFECYCLES:
        lifecycle = {
            "warning": "candidate",
            "confirmed": "confirmed",
            "invalidated": "invalidated",
            "expired": "expired",
        }.get(status, "none")
    if lifecycle in {"invalidated", "expired"}:
        status = lifecycle
    return status, lifecycle


def _anchor_dates(frame: dict[str, Any]) -> list[str]:
    dates: list[str] = []
    anchors = frame.get("anchors") if isinstance(frame.get("anchors"), list) else []
    for anchor in anchors:
        if not isinstance(anchor, dict):
            continue
        date = _text(anchor.get("date"))
        if date and date not in dates:
            dates.append(date)
    if dates:
        return dates
    pivots = frame.get("pivotDates") if isinstance(frame.get("pivotDates"), list) else []
    for value in pivots:
        date = _text(value)
        if date and date not in dates:
            dates.append(date)
    return dates


def _frame_judgment(frame: dict[str, Any], timeframe: str) -> dict[str, Any]:
    status, lifecycle = _effective_status(frame)
    active = status != "none"
    if not active:
        lifecycle = "none"
    direction = _text(frame.get("direction")).lower()
    if direction not in {"top", "bottom"} or not active:
        direction = ""
    confirmation = frame.get("confirmation") if isinstance(frame.get("confirmation"), dict) else {}
    confluence = frame.get("confluence") if isinstance(frame.get("confluence"), dict) else {}
    quality = frame.get("dataQuality") if isinstance(frame.get("dataQuality"), dict) else {}
    raw_supporting = confluence.get("supportingTimeframes")
    supporting_values = raw_supporting if isinstance(raw_supporting, (list, tuple, set)) else []
    supporting = sorted({_text(value).lower() for value in supporting_values if _text(value)})

    decision = {
        "timeframe": timeframe,
        "status": status,
        "direction": direction,
        "lifecycleState": lifecycle,
        "score": _score(frame.get("score")) if active else 0,
        "patternFingerprint": _text(frame.get("patternFingerprint")) if active else "",
        "patternId": _text(frame.get("patternId") or frame.get("pattern")) if active else "",
        "anchorDates": _anchor_dates(frame) if active else [],
        "confluenceLevel": _text(confluence.get("level") or "none").lower() if active else "none",
        "supportingTimeframes": supporting if active else [],
        "keyCrossState": (
            _text(confirmation.get("keyCrossState") or frame.get("keyCrossState") or "").lower()
            if active
            else ""
        ),
        "keyLevel": _rounded(frame.get("keyLevel")) if active else None,
        "invalidationLevel": _rounded(frame.get("invalidationLevel")) if active else None,
        "targetRange": (
            [
                _rounded(value)
                for value in (frame.get("targetRange") if isinstance(frame.get("targetRange"), list) else [])[:2]
            ]
            if active
            else []
        ),
    }
    display = {
        **decision,
        "qualityStatus": _text(quality.get("status") or frame.get("availability") or "").lower(),
        "stateEligible": quality.get("stateEligible"),
        "qualityReason": _text(quality.get("reason")),
        "latestCompletedBarAt": _text(frame.get("latestCompletedBarAt")),
    }
    return display


def _enabled_timeframes(values: Iterable[str] | None) -> tuple[str, ...]:
    if values is None:
        return TIMEFRAME_ORDER
    selected = {_text(value).lower() for value in values}
    return tuple(timeframe for timeframe in TIMEFRAME_ORDER if timeframe in selected)


def build_index_signal_judgment_snapshot(
    items: list[dict[str, Any]],
    *,
    market: str,
    market_date: str,
    phase: str,
    generated_at: str,
    rule_version: str,
    enabled_timeframes: Iterable[str] | None = None,
    expected_item_count: int | None = None,
    expected_item_keys: Iterable[str] | None = None,
) -> dict[str, Any]:
    """Normalize one scan into a stable, operator-visible judgment snapshot."""

    timeframes = _enabled_timeframes(enabled_timeframes)
    normalized_items: list[dict[str, Any]] = []
    for raw in items if isinstance(items, list) else []:
        if not isinstance(raw, dict):
            continue
        index_key = _text(raw.get("key") or raw.get("indexKey"))
        if not index_key:
            continue
        frames = raw.get("timeframes") if isinstance(raw.get("timeframes"), dict) else {}
        normalized_items.append(
            {
                "indexKey": index_key,
                "indexName": _text(raw.get("name") or raw.get("indexName") or index_key),
                "timeframes": {
                    timeframe: _frame_judgment(
                        frames.get(timeframe) if isinstance(frames.get(timeframe), dict) else {},
                        timeframe,
                    )
                    for timeframe in timeframes
                },
            }
        )
    normalized_items.sort(key=lambda item: item["indexKey"])
    expected_key_values = (
        [expected_item_keys]
        if isinstance(expected_item_keys, str)
        else expected_item_keys
    )
    normalized_expected_keys = (
        sorted(_text(value) for value in expected_key_values if _text(value))
        if expected_key_values is not None
        else None
    )
    expected_count = (
        int(expected_item_count)
        if expected_item_count is not None
        else len(normalized_expected_keys)
        if normalized_expected_keys is not None
        else len(normalized_items)
    )
    snapshot = {
        "schema": SNAPSHOT_SCHEMA,
        "fingerprintVersion": FINGERPRINT_VERSION,
        "ruleVersion": _text(rule_version),
        "market": _text(market).upper(),
        "marketDate": _text(market_date),
        "phase": _text(phase).lower(),
        "generatedAt": _text(generated_at),
        "enabledTimeframes": list(timeframes),
        "expectedItemCount": expected_count,
        "items": normalized_items,
    }
    if normalized_expected_keys is not None:
        snapshot["expectedItemKeys"] = normalized_expected_keys
    snapshot["fingerprint"] = index_signal_judgment_fingerprint(snapshot)
    return snapshot


def _fingerprint_frame(frame: dict[str, Any], timeframe: str) -> dict[str, Any]:
    keys = (
        "timeframe",
        "status",
        "direction",
        "lifecycleState",
        "score",
        "patternFingerprint",
        "patternId",
        "anchorDates",
        "confluenceLevel",
        "supportingTimeframes",
        "keyCrossState",
        "keyLevel",
        "invalidationLevel",
        "targetRange",
    )
    normalized = {key: _copy(frame.get(key)) for key in keys}
    normalized["timeframe"] = timeframe
    if _text(normalized.get("status")).lower() in ACTIVE_JUDGMENT_STATUSES:
        return normalized
    # Expired/invalidated/conflict records remain in the stored snapshot for
    # audit, but are all equivalent to "no current signal" for tail/close
    # delivery.  This prevents inactive score drift from creating a useless
    # 15:10 correction.
    return {
        "timeframe": timeframe,
        "status": "none",
        "direction": "",
        "lifecycleState": "none",
        "score": 0,
        "patternFingerprint": "",
        "patternId": "",
        "anchorDates": [],
        "confluenceLevel": "none",
        "supportingTimeframes": [],
        "keyCrossState": "",
        "keyLevel": None,
        "invalidationLevel": None,
        "targetRange": [],
    }


def _fingerprint_payload(snapshot: dict[str, Any]) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    for raw in snapshot.get("items") if isinstance(snapshot.get("items"), list) else []:
        if not isinstance(raw, dict):
            continue
        frames = raw.get("timeframes") if isinstance(raw.get("timeframes"), dict) else {}
        normalized_frames = []
        for timeframe in TIMEFRAME_ORDER:
            frame = frames.get(timeframe)
            if not isinstance(frame, dict):
                continue
            normalized_frames.append(_fingerprint_frame(frame, timeframe))
        items.append({"indexKey": _text(raw.get("indexKey")), "timeframes": normalized_frames})
    items.sort(key=lambda item: item["indexKey"])
    return {
        "fingerprintVersion": _text(snapshot.get("fingerprintVersion") or FINGERPRINT_VERSION),
        "ruleVersion": _text(snapshot.get("ruleVersion")),
        "market": _text(snapshot.get("market")).upper(),
        "enabledTimeframes": list(snapshot.get("enabledTimeframes") or []),
        "items": items,
    }


def index_signal_judgment_fingerprint(snapshot: dict[str, Any]) -> str:
    payload = _fingerprint_payload(snapshot if isinstance(snapshot, dict) else {})
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def compare_index_signal_judgments(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    before_fingerprint = index_signal_judgment_fingerprint(before)
    after_fingerprint = index_signal_judgment_fingerprint(after)
    before_payload = _fingerprint_payload(before)
    after_payload = _fingerprint_payload(after)
    metadata_changes = [
        {
            "field": field,
            "before": _copy(before_payload.get(field)),
            "after": _copy(after_payload.get(field)),
        }
        for field in ("fingerprintVersion", "ruleVersion", "market", "enabledTimeframes")
        if before_payload.get(field) != after_payload.get(field)
    ]
    before_items = {
        _text(item.get("indexKey")): item
        for item in (before.get("items") if isinstance(before.get("items"), list) else [])
        if isinstance(item, dict) and _text(item.get("indexKey"))
    }
    after_items = {
        _text(item.get("indexKey")): item
        for item in (after.get("items") if isinstance(after.get("items"), list) else [])
        if isinstance(item, dict) and _text(item.get("indexKey"))
    }
    changes: list[dict[str, Any]] = []
    timeframes = tuple(
        dict.fromkeys(
            [*(before.get("enabledTimeframes") or []), *(after.get("enabledTimeframes") or [])]
        )
    )
    for index_key in sorted(set(before_items) | set(after_items)):
        old_item = before_items.get(index_key) or {}
        new_item = after_items.get(index_key) or {}
        old_frames = old_item.get("timeframes") if isinstance(old_item.get("timeframes"), dict) else {}
        new_frames = new_item.get("timeframes") if isinstance(new_item.get("timeframes"), dict) else {}
        for timeframe in timeframes:
            old_frame = old_frames.get(timeframe) if isinstance(old_frames.get(timeframe), dict) else {}
            new_frame = new_frames.get(timeframe) if isinstance(new_frames.get(timeframe), dict) else {}
            old_decision = _fingerprint_payload(
                {
                    "enabledTimeframes": [timeframe],
                    "items": [{"indexKey": index_key, "timeframes": {timeframe: old_frame}}],
                }
            )["items"][0]["timeframes"]
            new_decision = _fingerprint_payload(
                {
                    "enabledTimeframes": [timeframe],
                    "items": [{"indexKey": index_key, "timeframes": {timeframe: new_frame}}],
                }
            )["items"][0]["timeframes"]
            if old_decision == new_decision:
                continue
            changes.append(
                {
                    "indexKey": index_key,
                    "indexName": _text(new_item.get("indexName") or old_item.get("indexName") or index_key),
                    "timeframe": timeframe,
                    "before": _copy(old_frame),
                    "after": _copy(new_frame),
                }
            )
    return {
        "changed": before_fingerprint != after_fingerprint,
        "beforeFingerprint": before_fingerprint,
        "afterFingerprint": after_fingerprint,
        "metadataChanges": metadata_changes,
        "changes": changes,
    }


def close_snapshot_ready(snapshot: dict[str, Any]) -> bool:
    """Require every expected CN asset/frame to contain completed close data."""

    items = snapshot.get("items") if isinstance(snapshot.get("items"), list) else []
    try:
        expected = int(snapshot.get("expectedItemCount"))
    except (TypeError, ValueError):
        expected = len(items)
    if expected <= 0 or len(items) != expected:
        return False
    actual_keys: list[str] = []
    for item in items:
        if not isinstance(item, dict):
            return False
        index_key = _text(item.get("indexKey"))
        if not index_key:
            return False
        actual_keys.append(index_key)
    if len(set(actual_keys)) != len(actual_keys):
        return False
    if "expectedItemKeys" in snapshot:
        raw_expected_keys = snapshot.get("expectedItemKeys")
        if not isinstance(raw_expected_keys, list):
            return False
        expected_keys = [_text(value) for value in raw_expected_keys]
        if (
            not expected_keys
            or any(not value for value in expected_keys)
            or len(set(expected_keys)) != len(expected_keys)
            or set(actual_keys) != set(expected_keys)
        ):
            return False
    required_timeframes = _enabled_timeframes(snapshot.get("enabledTimeframes"))
    if not required_timeframes:
        return False
    for item in items:
        frames = item.get("timeframes") if isinstance(item.get("timeframes"), dict) else {}
        for timeframe in required_timeframes:
            frame = frames.get(timeframe)
            if not isinstance(frame, dict):
                return False
            quality = _text(frame.get("qualityStatus")).lower()
            if frame.get("stateEligible") is not True or quality not in {"available", "complete", "completed"}:
                return False
    return True


def channel_hash(webhook: Any) -> str:
    value = _text(webhook)
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:20] if value else "unconfigured"


def plan_index_signal_judgment_delivery(
    *,
    phase: str,
    market_date: str,
    snapshot: dict[str, Any],
    record: dict[str, Any] | None,
    close_ready: bool = True,
) -> dict[str, Any]:
    record = record if isinstance(record, dict) else {}
    normalized_phase = _text(phase).lower()
    if normalized_phase == "cn_tail":
        tail = record.get("tail") if isinstance(record.get("tail"), dict) else {}
        if tail.get("delivered") is True and _text(tail.get("marketDate")) == _text(market_date):
            return {"send": False, "reportType": "tail", "reason": "tail_already_delivered", "comparison": None}
        return {"send": True, "reportType": "tail", "reason": "forced_tail_judgment", "comparison": None}
    if normalized_phase != "cn_close":
        return {"send": False, "reportType": "", "reason": "phase_not_daily_judgment", "comparison": None}

    close = record.get("close") if isinstance(record.get("close"), dict) else {}
    if close.get("delivered") is True and _text(close.get("marketDate")) == _text(market_date):
        return {"send": False, "reportType": "close_changed", "reason": "close_already_delivered", "comparison": None}
    if close.get("verified") is True and _text(close.get("marketDate")) == _text(market_date):
        return {"send": False, "reportType": "close_same", "reason": "close_already_verified", "comparison": None}
    if not close_ready:
        return {"send": False, "reportType": "close_pending", "reason": "close_data_pending", "comparison": None}

    tail = record.get("tail") if isinstance(record.get("tail"), dict) else {}
    tail_snapshot = tail.get("snapshot") if isinstance(tail.get("snapshot"), dict) else None
    tail_delivered = tail.get("delivered") is True and _text(tail.get("marketDate")) == _text(market_date)
    if not tail_delivered or tail_snapshot is None:
        return {
            "send": True,
            "reportType": "close_fallback",
            "reason": "tail_not_delivered",
            "comparison": None,
        }
    comparison = compare_index_signal_judgments(tail_snapshot, snapshot)
    if comparison["changed"]:
        return {
            "send": True,
            "reportType": "close_changed",
            "reason": "judgment_changed_after_close",
            "comparison": comparison,
        }
    return {
        "send": False,
        "reportType": "close_same",
        "reason": "judgment_unchanged_after_close",
        "comparison": comparison,
    }


class IndexSignalDailyPushStore:
    """Small atomic store keyed by market date and redacted channel hash."""

    def __init__(self, path: str | Path, *, max_market_dates: int = MAX_RETAINED_MARKET_DATES) -> None:
        self.path = Path(path)
        self.max_market_dates = max(1, int(max_market_dates))
        self._lock = threading.RLock()

    def _empty(self) -> dict[str, Any]:
        return {"schemaVersion": SCHEMA_VERSION, "records": {}, "updatedAt": None}

    def _read_unlocked(self) -> dict[str, Any]:
        if not self.path.exists():
            return self._empty()
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8-sig"))
        except (OSError, ValueError, TypeError):
            return self._empty()
        if not isinstance(payload, dict) or not isinstance(payload.get("records"), dict):
            return self._empty()
        payload.setdefault("schemaVersion", SCHEMA_VERSION)
        payload.setdefault("updatedAt", None)
        return payload

    def _write_unlocked(self, payload: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self.path.with_name(f".{self.path.name}.{os.getpid()}.tmp")
        with temp_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, self.path)

    def _key(self, market: str, market_date: str, channel_key: str) -> str:
        return f"{_text(market).upper()}|{_text(market_date)}|{_text(channel_key) or 'unconfigured'}"

    def _trim_unlocked(self, payload: dict[str, Any]) -> None:
        records = payload.get("records") if isinstance(payload.get("records"), dict) else {}
        dates = sorted({_text(record.get("marketDate")) for record in records.values() if isinstance(record, dict)})
        keep = set(dates[-self.max_market_dates :])
        payload["records"] = {
            key: value
            for key, value in records.items()
            if isinstance(value, dict) and _text(value.get("marketDate")) in keep
        }

    def get(self, *, market: str, market_date: str, channel_key: str) -> dict[str, Any]:
        with self._lock:
            payload = self._read_unlocked()
            return _copy(payload.get("records", {}).get(self._key(market, market_date, channel_key), {}))

    def record_capture(
        self,
        *,
        market: str,
        market_date: str,
        channel_key: str,
        stage: str,
        snapshot: dict[str, Any],
        plan: dict[str, Any],
    ) -> dict[str, Any]:
        stage_key = "tail" if _text(stage).lower() == "tail" else "close"
        with self._lock:
            payload = self._read_unlocked()
            key = self._key(market, market_date, channel_key)
            records = payload.setdefault("records", {})
            record = records.get(key) if isinstance(records.get(key), dict) else {}
            record.update({"market": _text(market).upper(), "marketDate": _text(market_date), "channelHash": _text(channel_key)})
            previous_stage = record.get(stage_key) if isinstance(record.get(stage_key), dict) else {}
            # A successful delivery is the immutable comparison/audit point for
            # that market date and channel.  A duplicate scheduler/manual scan
            # must not replace the 14:55 snapshot that the recipient actually
            # saw (or the corresponding close snapshot).
            if previous_stage.get("delivered") is True or previous_stage.get("verified") is True:
                return _copy(record)
            record[stage_key] = {
                **previous_stage,
                "marketDate": _text(market_date),
                "capturedAt": datetime.now().isoformat(timespec="seconds"),
                "fingerprint": _text(snapshot.get("fingerprint")) or index_signal_judgment_fingerprint(snapshot),
                "snapshot": _copy(snapshot),
                "plan": _copy(plan),
            }
            records[key] = record
            payload["updatedAt"] = datetime.now().isoformat(timespec="seconds")
            self._trim_unlocked(payload)
            self._write_unlocked(payload)
            return _copy(record)

    def record_delivery_result(
        self,
        *,
        market: str,
        market_date: str,
        channel_key: str,
        stage: str,
        result: dict[str, Any],
    ) -> dict[str, Any]:
        stage_key = "tail" if _text(stage).lower() == "tail" else "close"
        with self._lock:
            payload = self._read_unlocked()
            key = self._key(market, market_date, channel_key)
            records = payload.setdefault("records", {})
            record = records.get(key) if isinstance(records.get(key), dict) else {
                "market": _text(market).upper(),
                "marketDate": _text(market_date),
                "channelHash": _text(channel_key),
            }
            stage_record = record.get(stage_key) if isinstance(record.get(stage_key), dict) else {}
            # Delivery state is monotonic.  Once the webhook accepted a report,
            # a later duplicate callback or retry failure cannot consume it or
            # erase its original timestamp/result.
            if stage_record.get("delivered") is True or stage_record.get("verified") is True:
                return _copy(record)
            delivered = bool(result.get("ok")) and not bool(result.get("skipped"))
            verified = bool(
                stage_key == "close"
                and (
                    delivered
                    or (
                        bool(result.get("ok"))
                        and bool(result.get("skipped"))
                        and _text(result.get("reportType")).lower() == "close_same"
                        and _text(result.get("reason")).lower() == "judgment_unchanged_after_close"
                    )
                )
            )
            stage_record.update(
                {
                    "delivered": delivered,
                    "deliveredAt": _text(result.get("notifiedAt")) if delivered else None,
                    "result": _copy(result),
                }
            )
            if stage_key == "close":
                stage_record.update(
                    {
                        "verified": verified,
                        "verifiedAt": (
                            _text(result.get("notifiedAt")) or datetime.now().isoformat(timespec="seconds")
                            if verified
                            else None
                        ),
                    }
                )
            record[stage_key] = stage_record
            records[key] = record
            payload["updatedAt"] = datetime.now().isoformat(timespec="seconds")
            self._trim_unlocked(payload)
            self._write_unlocked(payload)
            return _copy(record)

    def resolve_tail_by_close_fallback(
        self,
        *,
        market: str,
        market_date: str,
        channel_key: str,
        resolved_at: str,
    ) -> dict[str, Any]:
        """Close a failed tail attempt after its close fallback was delivered."""

        with self._lock:
            payload = self._read_unlocked()
            key = self._key(market, market_date, channel_key)
            records = payload.setdefault("records", {})
            record = records.get(key) if isinstance(records.get(key), dict) else {
                "market": _text(market).upper(),
                "marketDate": _text(market_date),
                "channelHash": _text(channel_key),
            }
            tail = record.get("tail") if isinstance(record.get("tail"), dict) else {}
            tail.update(
                {
                    "resolved": True,
                    "resolvedAt": _text(resolved_at),
                    "resolution": "resolved_by_close_fallback",
                }
            )
            record["tail"] = tail
            records[key] = record
            payload["updatedAt"] = datetime.now().isoformat(timespec="seconds")
            self._trim_unlocked(payload)
            self._write_unlocked(payload)
            return _copy(record)

    def latest_delivery(self) -> dict[str, Any] | None:
        with self._lock:
            records = self._read_unlocked().get("records", {})
            deliveries: list[dict[str, Any]] = []
            for record in records.values() if isinstance(records, dict) else []:
                if not isinstance(record, dict):
                    continue
                for stage in ("tail", "close"):
                    stage_record = record.get(stage) if isinstance(record.get(stage), dict) else {}
                    if stage_record.get("delivered") is not True:
                        continue
                    result = stage_record.get("result") if isinstance(stage_record.get("result"), dict) else {}
                    deliveries.append(
                        {
                            **_copy(result),
                            "reportType": _text((stage_record.get("plan") or {}).get("reportType")) or stage,
                            "marketDate": record.get("marketDate"),
                            "channelHash": record.get("channelHash"),
                        }
                    )
            if not deliveries:
                return None
            deliveries.sort(key=lambda row: _text(row.get("notifiedAt")))
            return deliveries[-1]
