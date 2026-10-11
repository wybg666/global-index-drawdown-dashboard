from __future__ import annotations

import copy
import json
import os
import threading
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 2
MAX_EVENTS = 200
VALID_TIMEFRAMES = {"short", "medium", "long"}
VALID_DIRECTIONS = {"top", "bottom"}
VALID_CANDIDATE_STATUSES = {"none", "warning", "confirmed", "invalidated", "expired", "conflict"}
VALID_LIFECYCLE_STATES = {"none", "candidate", "resonant", "confirmed", "invalidated", "expired"}
MAX_RETIRED_FINGERPRINTS = 16
NOTIFIABLE_TRANSITIONS = {
    ("none", "warning"),
    ("warning", "confirmed"),
    ("warning", "invalidated"),
    ("confirmed", "invalidated"),
}


_PATH_LOCKS: dict[str, threading.RLock] = {}
_PATH_LOCKS_GUARD = threading.Lock()


def _lock_for(path: Path) -> threading.RLock:
    key = os.path.normcase(str(path.expanduser().resolve()))
    with _PATH_LOCKS_GUARD:
        lock = _PATH_LOCKS.get(key)
        if lock is None:
            lock = threading.RLock()
            _PATH_LOCKS[key] = lock
        return lock


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _today() -> str:
    return date.today().isoformat()


def _empty_store(*, suppressed_before: str | None = None) -> dict[str, Any]:
    return {
        "schemaVersion": SCHEMA_VERSION,
        "notificationSuppressedBefore": suppressed_before,
        "updatedAt": None,
        "states": {},
        "observations": {},
        "events": [],
    }


def _state_key(index_key: str, timeframe: str, direction: str) -> str:
    return f"{index_key}|{timeframe}|{direction}"


def _copy(value: Any) -> Any:
    return copy.deepcopy(value)


def _is_close_phase(scan_phase: str) -> bool:
    return "close" in scan_phase.strip().lower()


def _normalize_market_date(value: Any) -> str:
    text = str(value or "").strip()
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError as exc:
        raise ValueError("market_date must be an ISO date (YYYY-MM-DD)") from exc


def _candidate_is_stale(candidate: dict[str, Any]) -> bool:
    return any(candidate.get(name) is True for name in ("stale", "isStale", "dataStale")) or candidate.get(
        "fresh"
    ) is False


def _candidate_status(candidate: dict[str, Any]) -> str:
    if any(
        candidate.get(name) is True
        for name in ("invalidated", "isInvalidated", "invalidationTriggered")
    ):
        return "invalidated"
    status = str(candidate.get("candidateStatus") or candidate.get("status") or "none").strip().lower()
    if status not in VALID_CANDIDATE_STATUSES:
        raise ValueError(f"unsupported candidate status: {status}")
    return status


def _candidate_lifecycle_state(candidate: dict[str, Any], status: str) -> str:
    """Return the evaluator lifecycle with a compatibility status fallback."""

    lifecycle = str(candidate.get("lifecycleState") or "").strip().lower()
    if lifecycle in VALID_LIFECYCLE_STATES:
        return lifecycle
    if status == "confirmed":
        return "confirmed"
    if status == "invalidated":
        return "invalidated"
    if status == "expired":
        return "expired"
    if status == "warning":
        return "candidate"
    return "none"


def _candidate_fingerprint(candidate: dict[str, Any]) -> str | None:
    value = str(candidate.get("patternFingerprint") or "").strip()
    return value or None


def _as_nonnegative_int(value: Any, default: int = 0) -> int:
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return default


def _later_timestamp(current: Any, incoming: Any) -> Any:
    """Keep a monotonic completed-bar marker without assuming a timezone."""

    current_text = str(current or "").strip()
    incoming_text = str(incoming or "").strip()
    if not current_text:
        return incoming if incoming_text else current
    if not incoming_text:
        return current
    return incoming if incoming_text > current_text else current


def _candidate_invalidation_close(candidate: dict[str, Any]) -> float | None:
    evidence = candidate.get("evidence") if isinstance(candidate.get("evidence"), list) else []
    for item in evidence:
        if not isinstance(item, dict) or str(item.get("label") or "") != "invalidation_break":
            continue
        value = item.get("value")
        if not isinstance(value, dict):
            continue
        try:
            return float(value.get("close"))
        except (TypeError, ValueError):
            return None
    return None


def _pivot_dates(candidate: dict[str, Any]) -> list[str]:
    direct = candidate.get("pivotDates")
    if isinstance(direct, (list, tuple)):
        return [str(item) for item in direct if item not in (None, "")]

    pivots = candidate.get("pivots")
    if isinstance(pivots, (list, tuple)):
        dates: list[str] = []
        for pivot in pivots:
            if isinstance(pivot, dict):
                value = pivot.get("date") or pivot.get("marketDate")
            else:
                value = pivot
            if value not in (None, ""):
                dates.append(str(value))
        if dates:
            return dates

    anchors = candidate.get("anchors")
    if isinstance(anchors, (list, tuple)):
        dates = [
            str(anchor.get("date"))
            for anchor in anchors
            if isinstance(anchor, dict) and anchor.get("date") not in (None, "")
        ]
        if dates:
            return dates

    return [
        str(candidate[name])
        for name in ("firstPivotDate", "secondPivotDate", "pivotDate")
        if candidate.get(name) not in (None, "")
    ]


def _new_signal_id(
    *, index_key: str, timeframe: str, direction: str, candidate: dict[str, Any]
) -> str:
    supplied = str(candidate.get("signalId") or "").strip()
    if supplied:
        return supplied
    return f"sig-{index_key}-{timeframe}-{direction}-{uuid.uuid4().hex}"


def _notification_status(result: dict[str, Any]) -> str:
    raw = str(result.get("status") or "").strip().lower()
    if result.get("skipped") is True:
        return raw or "skipped"
    if result.get("ok") is True or result.get("success") is True or raw in {
        "ok",
        "success",
        "sent",
        "delivered",
    }:
        return "success"
    if result.get("ok") is False or result.get("success") is False or result.get("error") or raw in {
        "failed",
        "failure",
        "error",
    }:
        return "failed"
    return raw or "unknown"


class IndexSignalStore:
    """Atomic JSON storage for index-signal states and transition events."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self._lock = _lock_for(self.path)
        self._data: dict[str, Any] | None = None

    def load(self) -> dict[str, Any]:
        with self._lock:
            self._data = self._load_unlocked()
            return _copy(self._data)

    def snapshot(self) -> dict[str, Any]:
        return self.load()

    def record_observations(self, observations: list[dict[str, Any]]) -> None:
        """Persist the latest evaluated result for every index/timeframe in one write."""

        if not isinstance(observations, list):
            raise ValueError("observations must be a list")
        with self._lock:
            data = self._load_unlocked()
            stored = data.setdefault("observations", {})
            if not isinstance(stored, dict):
                stored = {}
                data["observations"] = stored
            changed_at = _now()
            changed = False
            for entry in observations:
                if not isinstance(entry, dict):
                    continue
                index_key = str(entry.get("indexKey") or "").strip()
                timeframe = str(entry.get("timeframe") or "").strip().lower()
                candidate = entry.get("candidate") if isinstance(entry.get("candidate"), dict) else None
                if not index_key or timeframe not in VALID_TIMEFRAMES or candidate is None:
                    continue
                status = _candidate_status(candidate)
                lifecycle_state = _candidate_lifecycle_state(candidate, status)
                if lifecycle_state in {"invalidated", "expired"}:
                    status = lifecycle_state
                direction = str(candidate.get("direction") or "").strip().lower()
                if direction not in VALID_DIRECTIONS:
                    direction = None
                value = {
                    "indexKey": index_key,
                    "timeframe": timeframe,
                    "direction": direction,
                    "status": status,
                    "candidateStatus": status,
                    "score": candidate.get("score", 0),
                    "pattern": candidate.get("pattern") or "",
                    "patternId": candidate.get("patternId") or candidate.get("pattern") or "",
                    "anchors": _copy(candidate.get("anchors")) if isinstance(candidate.get("anchors"), list) else [],
                    "sourceInterval": candidate.get("sourceInterval"),
                    "barCount": candidate.get("barCount"),
                    "availability": candidate.get("availability") or "available",
                    "dataQuality": _copy(candidate.get("dataQuality")) if isinstance(candidate.get("dataQuality"), dict) else {},
                    "confluence": _copy(candidate.get("confluence")) if isinstance(candidate.get("confluence"), dict) else {},
                    "keyLevel": candidate.get("keyLevel"),
                    "invalidationLevel": candidate.get("invalidationLevel"),
                    "targetRange": _copy(candidate.get("targetRange")),
                    "evidence": _copy(candidate.get("evidence")) if isinstance(candidate.get("evidence"), list) else [],
                    "metrics": _copy(candidate.get("metrics")) if isinstance(candidate.get("metrics"), dict) else {},
                    "patternFingerprint": _candidate_fingerprint(candidate),
                    "patternStartAt": candidate.get("patternStartAt"),
                    "patternEndAt": candidate.get("patternEndAt"),
                    "patternWidthBars": candidate.get("patternWidthBars") or 0,
                    "expiresAfterBars": candidate.get("expiresAfterBars"),
                    "signalAgeBars": candidate.get("signalAgeBars") or 0,
                    "latestCompletedBarAt": candidate.get("latestCompletedBarAt"),
                    "invalidationBuffer": candidate.get("invalidationBuffer"),
                    "lifecycleState": lifecycle_state,
                    "confirmation": (
                        _copy(candidate.get("confirmation"))
                        if isinstance(candidate.get("confirmation"), dict)
                        else {}
                    ),
                    "marketDate": _normalize_market_date(entry.get("marketDate")),
                    "scanPhase": str(entry.get("scanPhase") or ""),
                    "changedAt": changed_at,
                }
                key = f"{index_key}|{timeframe}"
                if stored.get(key) != value:
                    stored[key] = value
                    changed = True
            if changed:
                data["updatedAt"] = changed_at
                self._write_unlocked(data)
            self._data = data

    def apply_scan(
        self,
        *,
        index_key: str,
        timeframe: str,
        direction: str,
        candidate: dict[str, Any],
        market_date: str,
        scan_phase: str,
    ) -> dict[str, Any]:
        index_key = str(index_key or "").strip()
        timeframe = str(timeframe or "").strip().lower()
        direction = str(direction or "").strip().lower()
        scan_phase = str(scan_phase or "").strip()
        market_date = _normalize_market_date(market_date)
        if not index_key:
            raise ValueError("index_key is required")
        if timeframe not in VALID_TIMEFRAMES:
            raise ValueError(f"unsupported timeframe: {timeframe}")
        if direction not in VALID_DIRECTIONS:
            raise ValueError(f"unsupported direction: {direction}")
        if not isinstance(candidate, dict):
            raise ValueError("candidate must be a dict")
        if not scan_phase:
            raise ValueError("scan_phase is required")

        key = _state_key(index_key, timeframe, direction)
        with self._lock:
            data = self._load_unlocked()
            existing = data["states"].get(key)
            current = _copy(existing) if isinstance(existing, dict) else self._new_empty_state(
                index_key=index_key,
                timeframe=timeframe,
                direction=direction,
            )

            stored_market_date = str(current.get("marketDate") or "")
            if _candidate_is_stale(candidate) or (stored_market_date and market_date < stored_market_date):
                self._data = data
                return {
                    "state": _copy(current),
                    "event": None,
                    "shouldNotify": False,
                    "changed": False,
                    "stale": True,
                }

            raw_status = _candidate_status(candidate)
            incoming_lifecycle = _candidate_lifecycle_state(candidate, raw_status)
            desired_status = raw_status
            # Evaluator lifecycle is authoritative for terminal observations:
            # an old warning-shaped structure must not refresh a signal after
            # its structural expiry has elapsed.
            if incoming_lifecycle in {"invalidated", "expired"}:
                desired_status = incoming_lifecycle
            if not _is_close_phase(scan_phase):
                if desired_status == "confirmed":
                    desired_status = "warning"

            current_status = str(current.get("status") or "none")
            incoming_fingerprint = _candidate_fingerprint(candidate)
            current_fingerprint = str(current.get("patternFingerprint") or "").strip() or None
            same_identity = bool(
                incoming_fingerprint
                and current_fingerprint
                and incoming_fingerprint == current_fingerprint
            )
            retired_identity = self._is_retired_fingerprint(current, incoming_fingerprint)

            # A detector may use rolling channel/wedge boundaries while the
            # stored signal is governed by its original invalidation line.
            # Only a completed close crossing that frozen boundary can end an
            # explicitly identified active formation.
            if (
                same_identity
                and current_status in {"warning", "confirmed"}
                and desired_status == "invalidated"
            ):
                close = _candidate_invalidation_close(candidate)
                try:
                    frozen_level = float(current.get("invalidationLevel"))
                    frozen_buffer = float(current.get("invalidationBuffer"))
                except (TypeError, ValueError):
                    frozen_level = None
                    frozen_buffer = None
                crossed = (
                    close is not None
                    and frozen_level is not None
                    and frozen_buffer is not None
                    and (
                        close >= frozen_level + frozen_buffer
                        if direction == "top"
                        else close <= frozen_level - frozen_buffer
                    )
                )
                if not crossed:
                    self._data = data
                    return {
                        "state": _copy(current),
                        "event": None,
                        "shouldNotify": False,
                        "changed": False,
                        "stale": False,
                    }

            # A retired formation can never be made current again merely
            # because a rolling detector rediscovers the same anchors.  If a
            # different formation is active under this state key, leave it
            # untouched as well.
            if incoming_fingerprint and retired_identity and not (
                same_identity and current_status in {"invalidated", "expired"}
            ):
                self._data = data
                return {
                    "state": _copy(current),
                    "event": None,
                    "shouldNotify": False,
                    "changed": False,
                    "stale": False,
                }

            # A terminal formation is a tombstone, not a pending rearm.  Keep
            # it terminal until an actually new structural fingerprint exists.
            if (
                same_identity
                and current_status in {"invalidated", "expired"}
                and incoming_lifecycle in {"candidate", "resonant", "confirmed"}
            ):
                proposed = _copy(current)
                proposed["marketDate"] = market_date
                proposed["scanPhase"] = scan_phase
                proposed["latestCompletedBarAt"] = _later_timestamp(
                    current.get("latestCompletedBarAt"), candidate.get("latestCompletedBarAt")
                )
                state_changed = proposed != existing
                if state_changed:
                    now = _now()
                    proposed["updatedAt"] = now
                    data["states"][key] = proposed
                    data["updatedAt"] = now
                    self._write_unlocked(data)
                self._data = data
                return {
                    "state": _copy(proposed),
                    "event": None,
                    "shouldNotify": False,
                    "changed": state_changed,
                    "stale": False,
                }

            new_identity = bool(incoming_fingerprint and not same_identity and not retired_identity)
            if new_identity:
                # A fresh formation gets a new signal identity.  Preserve
                # terminal fingerprints so a future stale rediscovery cannot
                # resurrect them.
                proposed = self._new_empty_state(
                    index_key=index_key,
                    timeframe=timeframe,
                    direction=direction,
                )
                proposed["retiredFingerprints"] = self._retired_fingerprints(current)
                if current_fingerprint:
                    self._retire_fingerprint(
                        proposed,
                        fingerprint=current_fingerprint,
                        reason="superseded_by_new_fingerprint",
                        market_date=market_date,
                    )
                current_status = "none"
            else:
                proposed = _copy(current)
            self._merge_candidate(
                proposed,
                candidate=candidate,
                candidate_status=desired_status,
                market_date=market_date,
                scan_phase=scan_phase,
            )
            if same_identity:
                self._freeze_same_identity_lifecycle(proposed, current)
                # Daily confirmation is a durable state, not a score snapshot.
                # A later weaker warning for the same structural formation may
                # refresh display metrics, but cannot silently relabel a
                # confirmed signal as a candidate.
                if current_status == "confirmed" and desired_status == "warning":
                    proposed["candidateStatus"] = "confirmed"
                    proposed["lifecycleState"] = "confirmed"

            if existing is None and desired_status in {"invalidated", "expired"}:
                self._data = data
                return {
                    "state": _copy(current),
                    "event": None,
                    "shouldNotify": False,
                    "changed": False,
                    "stale": False,
                }

            event_from: str | None = None
            event_to: str | None = None
            silent_reset = False
            starts_new_signal = False
            changed_at: str | None = None
            rearm_after = str(proposed.get("rearmAfterDate") or proposed.get("lastInvalidatedDate") or "")

            if current_status == "invalidated":
                if desired_status in {"warning", "confirmed"}:
                    starts_new_signal = True
                    if rearm_after and market_date <= rearm_after:
                        proposed["status"] = desired_status
                        proposed["rearmPending"] = True
                        proposed["rearmAfterDate"] = rearm_after
                    else:
                        proposed["status"] = desired_status
                        proposed["rearmPending"] = False
                        proposed["rearmAfterDate"] = None
                        event_from = "none"
                        event_to = desired_status
                elif desired_status in {"none", "conflict"} and _is_close_phase(scan_phase):
                    invalidated_date = str(proposed.get("lastInvalidatedDate") or current.get("marketDate") or "")
                    if invalidated_date and market_date > invalidated_date:
                        proposed["status"] = "none"
                        proposed["rearmPending"] = False
                        proposed["rearmAfterDate"] = None
                        silent_reset = True
            elif current.get("rearmPending"):
                if desired_status == "warning" and rearm_after and market_date > rearm_after:
                    proposed["status"] = "warning"
                    proposed["rearmPending"] = False
                    proposed["rearmAfterDate"] = None
                    event_from = "none"
                    event_to = "warning"
                elif desired_status == "confirmed":
                    proposed["status"] = "confirmed"
                    event_from = "warning"
                    event_to = "confirmed"
                    proposed["rearmPending"] = False
                    proposed["rearmAfterDate"] = None
                elif desired_status in {"none", "conflict"} and _is_close_phase(scan_phase):
                    if rearm_after and market_date > rearm_after:
                        proposed["status"] = "none"
                        proposed["rearmPending"] = False
                        proposed["rearmAfterDate"] = None
                        silent_reset = True
                elif desired_status == "invalidated":
                    proposed["status"] = "invalidated"
                    event_from = "warning"
                    event_to = "invalidated"
                    proposed["rearmPending"] = False
            elif current_status == "none":
                # Terminal observations only close an already-active
                # formation.  A newly discovered shape that is already
                # invalid/expired must never manufacture a dashboard state.
                if desired_status in {"warning", "confirmed"}:
                    proposed["status"] = desired_status
                    event_from = "none"
                    event_to = desired_status
                    starts_new_signal = desired_status in {"warning", "confirmed"}
            elif current_status == "warning":
                if desired_status in {"confirmed", "invalidated", "expired"}:
                    proposed["status"] = desired_status
                    event_from = "warning"
                    event_to = desired_status
                elif desired_status == "none" and bool(candidate.get("clearWhenMissing")):
                    # 15m/60m candidates are session-local observations.  The
                    # scanner explicitly sets this flag only after it has a
                    # fresh, conflict-free frame with no remaining candidate
                    # for this direction, so removing it is not a signal
                    # invalidation and must not create a notification event.
                    proposed["status"] = "none"
                    proposed["signalId"] = None
                    proposed["firstDetectedAt"] = None
                    proposed["confirmedAt"] = None
                    proposed["invalidatedAt"] = None
                    silent_reset = True
            elif current_status == "confirmed":
                if desired_status in {"invalidated", "expired"}:
                    proposed["status"] = desired_status
                    event_from = "confirmed"
                    event_to = desired_status
                elif desired_status == "none" and bool(candidate.get("clearWhenMissing")):
                    proposed["status"] = "none"
                    proposed["signalId"] = None
                    proposed["firstDetectedAt"] = None
                    proposed["confirmedAt"] = None
                    proposed["invalidatedAt"] = None
                    silent_reset = True

            if starts_new_signal:
                proposed["signalId"] = _new_signal_id(
                    index_key=index_key,
                    timeframe=timeframe,
                    direction=direction,
                    candidate=candidate,
                )
                proposed["firstDetectedAt"] = _now()
                proposed["confirmedAt"] = None
                proposed["invalidatedAt"] = None

            if event_to is not None or silent_reset or proposed.get("status") != current_status:
                changed_at = _now()
                proposed["changedAt"] = changed_at

            if event_to == "confirmed":
                proposed["confirmedAt"] = changed_at or _now()
            if event_to == "invalidated":
                invalidated_at = changed_at or _now()
                proposed["invalidatedAt"] = invalidated_at
                proposed["lastInvalidatedDate"] = market_date
                proposed["rearmAfterDate"] = market_date
                proposed["rearmPending"] = False
            if event_to in {"invalidated", "expired"}:
                self._retire_fingerprint(
                    proposed,
                    fingerprint=str(proposed.get("patternFingerprint") or "").strip() or None,
                    reason=event_to,
                    market_date=market_date,
                )

            event: dict[str, Any] | None = None
            should_notify = False
            if event_from is not None and event_to is not None:
                should_notify, suppression_reason = self._should_notify(
                    data=data,
                    state=proposed,
                    from_status=event_from,
                    to_status=event_to,
                    market_date=market_date,
                )
                event = self._make_event(
                    state_key=key,
                    state=proposed,
                    from_status=event_from,
                    to_status=event_to,
                    market_date=market_date,
                    scan_phase=scan_phase,
                    changed_at=changed_at or _now(),
                    should_notify=should_notify,
                    suppression_reason=suppression_reason,
                )
                data["events"].append(event)
                data["events"] = data["events"][-MAX_EVENTS:]
                if should_notify:
                    dedupe = proposed.get("notificationDedupe")
                    if not isinstance(dedupe, dict):
                        dedupe = {}
                    dedupe[event_to] = market_date
                    proposed["notificationDedupe"] = dedupe

            state_changed = existing is None or proposed != existing
            changed = state_changed or event is not None
            if changed:
                now = _now()
                if state_changed:
                    proposed["updatedAt"] = now
                    data["states"][key] = proposed
                data["updatedAt"] = now
                self._write_unlocked(data)

            self._data = data
            return {
                "state": _copy(proposed),
                "event": _copy(event),
                "shouldNotify": should_notify,
                "changed": changed,
                "stale": False,
            }

    def reconcile_missing(
        self,
        *,
        index_key: str,
        timeframe: str,
        direction: str,
        market_date: str,
        scan_phase: str,
        completed_bar_timestamps: list[str],
        latest_close: float | None,
        latest_atr: float | None,
        availability: str,
        data_quality: dict[str, Any],
        observation_status: str = "none",
    ) -> dict[str, Any]:
        """Age one missing directional candidate against completed source bars.

        This is deliberately separate from ``apply_scan``: a no-pattern
        observation is not proof that a prior structure vanished.  It can only
        age/retire a stored formation when the current frame is complete,
        state-eligible, conflict-free, and contains a new completed bar.
        """

        del latest_atr  # The lifecycle freezes its original invalidation buffer.
        index_key = str(index_key or "").strip()
        timeframe = str(timeframe or "").strip().lower()
        direction = str(direction or "").strip().lower()
        market_date = _normalize_market_date(market_date)
        scan_phase = str(scan_phase or "").strip()
        if not index_key:
            raise ValueError("index_key is required")
        if timeframe not in VALID_TIMEFRAMES:
            raise ValueError(f"unsupported timeframe: {timeframe}")
        if direction not in VALID_DIRECTIONS:
            raise ValueError(f"unsupported direction: {direction}")
        if not scan_phase:
            raise ValueError("scan_phase is required")

        timestamps = sorted(
            {
                str(value).strip()
                for value in (completed_bar_timestamps or [])
                if str(value or "").strip()
            }
        )
        quality = data_quality if isinstance(data_quality, dict) else {}
        normalized_availability = str(availability or "").strip().lower()
        normalized_observation_status = str(observation_status or "none").strip().lower()
        key = _state_key(index_key, timeframe, direction)

        with self._lock:
            data = self._load_unlocked()
            existing = data["states"].get(key)
            current = _copy(existing) if isinstance(existing, dict) else self._new_empty_state(
                index_key=index_key,
                timeframe=timeframe,
                direction=direction,
            )

            def unchanged(reason: str) -> dict[str, Any]:
                self._data = data
                return {
                    "state": _copy(current),
                    "event": None,
                    "shouldNotify": False,
                    "changed": False,
                    "stale": False,
                    "skippedReason": reason,
                }

            if not isinstance(existing, dict):
                return unchanged("no_stored_state")
            if quality.get("stateEligible") is not True:
                return unchanged("data_quality_ineligible")
            if normalized_availability != "available":
                return unchanged("frame_unavailable")
            if normalized_observation_status == "conflict":
                return unchanged("cross_timeframe_or_direction_conflict")
            if not timestamps:
                return unchanged("completed_bars_missing")

            latest_bar_at = timestamps[-1]
            previous_latest = str(current.get("latestCompletedBarAt") or "").strip()
            if previous_latest and latest_bar_at <= previous_latest:
                return unchanged("no_new_completed_bar")

            current_status = str(current.get("status") or "none").strip().lower()
            if current_status in {"none", "conflict"}:
                return unchanged("state_not_active")

            # Terminal states remain visible for exactly one subsequently
            # completed, qualified bar, then clear silently while their
            # fingerprint tombstone remains in the durable state.
            if current_status in {"invalidated", "expired"}:
                proposed = _copy(current)
                proposed.update(
                    {
                        "status": "none",
                        "candidateStatus": "none",
                        "lifecycleState": "none",
                        "lastTerminalSignalId": current.get("signalId"),
                        "signalId": None,
                        "firstDetectedAt": None,
                        "confirmedAt": None,
                        "invalidatedAt": None,
                        "latestCompletedBarAt": latest_bar_at,
                        "marketDate": market_date,
                        "scanPhase": scan_phase,
                        "changedAt": _now(),
                    }
                )
                now = _now()
                proposed["updatedAt"] = now
                data["states"][key] = proposed
                data["updatedAt"] = now
                self._write_unlocked(data)
                self._data = data
                return {
                    "state": _copy(proposed),
                    "event": None,
                    "shouldNotify": False,
                    "changed": True,
                    "stale": False,
                }

            if current_status not in {"warning", "confirmed"}:
                return unchanged("state_not_active")

            proposed = _copy(current)
            proposed["latestCompletedBarAt"] = latest_bar_at
            proposed["marketDate"] = market_date
            proposed["scanPhase"] = scan_phase
            fingerprint = str(current.get("patternFingerprint") or "").strip() or None
            pattern_end_at = str(current.get("patternEndAt") or "").strip()
            expires_after = _as_nonnegative_int(current.get("expiresAfterBars"))
            end_index = timestamps.index(pattern_end_at) if pattern_end_at in timestamps else None
            lifecycle_reason: str | None = None
            target_status: str | None = None

            try:
                close = float(latest_close) if latest_close is not None else None
            except (TypeError, ValueError):
                close = None
            try:
                invalidation_level = float(current.get("invalidationLevel"))
            except (TypeError, ValueError):
                invalidation_level = None
            try:
                invalidation_buffer = float(current.get("invalidationBuffer"))
            except (TypeError, ValueError):
                invalidation_buffer = None
            if close is not None and invalidation_level is not None and invalidation_buffer is not None:
                crossed = (
                    close >= invalidation_level + invalidation_buffer
                    if direction == "top"
                    else close <= invalidation_level - invalidation_buffer
                )
                if crossed:
                    target_status = "invalidated"
                    lifecycle_reason = "frozen_invalidation_break"

            if target_status is None:
                if not fingerprint or not pattern_end_at or expires_after <= 0 or end_index is None:
                    target_status = "expired"
                    lifecycle_reason = "legacy_lifecycle_metadata_missing"
                else:
                    age = max(0, len(timestamps) - 1 - end_index)
                    proposed["signalAgeBars"] = max(_as_nonnegative_int(current.get("signalAgeBars")), age)
                    if age >= expires_after:
                        target_status = "expired"
                        lifecycle_reason = "width_based_expiry"

            if target_status is None:
                now = _now()
                proposed["updatedAt"] = now
                data["states"][key] = proposed
                data["updatedAt"] = now
                self._write_unlocked(data)
                self._data = data
                return {
                    "state": _copy(proposed),
                    "event": None,
                    "shouldNotify": False,
                    "changed": proposed != existing,
                    "stale": False,
                }

            proposed["status"] = target_status
            proposed["candidateStatus"] = target_status
            proposed["lifecycleState"] = target_status
            proposed["lifecycleReason"] = lifecycle_reason
            changed_at = _now()
            proposed["changedAt"] = changed_at
            if target_status == "invalidated":
                proposed["invalidatedAt"] = changed_at
                proposed["lastInvalidatedDate"] = market_date
            self._retire_fingerprint(
                proposed,
                fingerprint=fingerprint,
                reason=target_status,
                market_date=market_date,
            )
            should_notify, suppression_reason = self._should_notify(
                data=data,
                state=proposed,
                from_status=current_status,
                to_status=target_status,
                market_date=market_date,
            )
            event = self._make_event(
                state_key=key,
                state=proposed,
                from_status=current_status,
                to_status=target_status,
                market_date=market_date,
                scan_phase=scan_phase,
                changed_at=changed_at,
                should_notify=should_notify,
                suppression_reason=suppression_reason,
            )
            data["events"].append(event)
            data["events"] = data["events"][-MAX_EVENTS:]
            if should_notify:
                dedupe = proposed.get("notificationDedupe")
                if not isinstance(dedupe, dict):
                    dedupe = {}
                dedupe[target_status] = market_date
                proposed["notificationDedupe"] = dedupe
            now = _now()
            proposed["updatedAt"] = now
            data["states"][key] = proposed
            data["updatedAt"] = now
            self._write_unlocked(data)
            self._data = data
            return {
                "state": _copy(proposed),
                "event": _copy(event),
                "shouldNotify": should_notify,
                "changed": True,
                "stale": False,
            }

    def mark_notification(self, event_id: str, result: dict[str, Any]) -> None:
        event_id = str(event_id or "").strip()
        if not event_id:
            raise ValueError("event_id is required")
        if not isinstance(result, dict):
            raise ValueError("result must be a dict")

        with self._lock:
            data = self._load_unlocked()
            event = next((item for item in data["events"] if str(item.get("id")) == event_id), None)
            if event is None:
                raise KeyError(f"unknown event id: {event_id}")

            # Store-suppressed lifecycle transitions (notably expiry) are not
            # delivery attempts.  A defensive caller must not overwrite that
            # audit fact with a synthetic skipped/success response.
            if event.get("shouldNotify") is False and event.get("notificationStatus") == "suppressed":
                self._data = data
                return

            normalized_result = _copy(result)
            status = _notification_status(normalized_result)
            if event.get("notificationResult") == normalized_result and event.get("notificationStatus") == status:
                self._data = data
                return

            now = _now()
            event["notificationResult"] = normalized_result
            event["notificationStatus"] = status
            event["notifiedAt"] = now

            state_key = str(event.get("stateKey") or "")
            state = data["states"].get(state_key)
            if isinstance(state, dict):
                state["lastNotificationDate"] = event.get("marketDate")
                state["lastNotificationStatus"] = status
                state["lastNotificationResult"] = normalized_result
                state["lastNotificationEventId"] = event_id
                state["lastNotificationAt"] = now
                state["updatedAt"] = now

            data["updatedAt"] = now
            self._write_unlocked(data)
            self._data = data

    @staticmethod
    def _new_empty_state(*, index_key: str, timeframe: str, direction: str) -> dict[str, Any]:
        return {
            "indexKey": index_key,
            "timeframe": timeframe,
            "direction": direction,
            "status": "none",
            "candidateStatus": "none",
            "signalId": None,
            "pivotDates": [],
            "firstDetectedAt": None,
            "confirmedAt": None,
            "invalidatedAt": None,
            "changedAt": None,
            "updatedAt": None,
            "marketDate": None,
            "scanPhase": None,
            "score": 0,
            "pattern": "",
            "patternId": "",
            "anchors": [],
            "sourceInterval": None,
            "barCount": None,
            "availability": "available",
            "dataQuality": {},
            "confluence": {},
            "keyLevel": None,
            "invalidationLevel": None,
            "targetRange": None,
            "evidence": [],
            "metrics": {},
            "lastNotificationDate": None,
            "lastNotificationStatus": None,
            "lastNotificationResult": None,
            "lastNotificationEventId": None,
            "notificationDedupe": {},
            "lastInvalidatedDate": None,
            "rearmAfterDate": None,
            "rearmPending": False,
            "patternFingerprint": None,
            "patternStartAt": None,
            "patternEndAt": None,
            "patternWidthBars": 0,
            "expiresAfterBars": None,
            "signalAgeBars": 0,
            "latestCompletedBarAt": None,
            "invalidationBuffer": None,
            "lifecycleState": "none",
            "confirmation": {},
            "retiredFingerprints": [],
        }

    @staticmethod
    def _retired_fingerprints(state: dict[str, Any]) -> list[dict[str, Any]]:
        raw = state.get("retiredFingerprints")
        if not isinstance(raw, list):
            return []
        items: list[dict[str, Any]] = []
        for item in raw:
            if isinstance(item, dict):
                fingerprint = str(item.get("fingerprint") or "").strip()
                if fingerprint:
                    items.append(dict(item, fingerprint=fingerprint))
            elif isinstance(item, str) and item.strip():
                items.append({"fingerprint": item.strip()})
        return items[-MAX_RETIRED_FINGERPRINTS:]

    @classmethod
    def _is_retired_fingerprint(cls, state: dict[str, Any], fingerprint: str | None) -> bool:
        if not fingerprint:
            return False
        return any(item.get("fingerprint") == fingerprint for item in cls._retired_fingerprints(state))

    @classmethod
    def _retire_fingerprint(
        cls,
        state: dict[str, Any],
        *,
        fingerprint: str | None,
        reason: str,
        market_date: str,
    ) -> None:
        if not fingerprint:
            return
        retained = [
            item
            for item in cls._retired_fingerprints(state)
            if item.get("fingerprint") != fingerprint
        ]
        retained.append(
            {
                "fingerprint": fingerprint,
                "reason": reason,
                "marketDate": market_date,
                "latestCompletedBarAt": state.get("latestCompletedBarAt"),
            }
        )
        state["retiredFingerprints"] = retained[-MAX_RETIRED_FINGERPRINTS:]

    @staticmethod
    def _freeze_same_identity_lifecycle(state: dict[str, Any], current: dict[str, Any]) -> None:
        """Restore immutable lifecycle and invalidation boundaries for one formation."""

        for name in (
            "patternFingerprint",
            "patternStartAt",
            "patternEndAt",
            "patternWidthBars",
            "expiresAfterBars",
            "invalidationLevel",
            "invalidationBuffer",
        ):
            if current.get(name) is not None:
                state[name] = _copy(current.get(name))
        state["signalAgeBars"] = max(
            _as_nonnegative_int(current.get("signalAgeBars")),
            _as_nonnegative_int(state.get("signalAgeBars")),
        )
        state["latestCompletedBarAt"] = _later_timestamp(
            current.get("latestCompletedBarAt"), state.get("latestCompletedBarAt")
        )

    @staticmethod
    def _merge_candidate(
        state: dict[str, Any],
        *,
        candidate: dict[str, Any],
        candidate_status: str,
        market_date: str,
        scan_phase: str,
    ) -> None:
        state["candidateStatus"] = candidate_status
        state["marketDate"] = market_date
        state["scanPhase"] = scan_phase
        state["score"] = candidate.get("score", 0)
        state["pattern"] = candidate.get("pattern") or ""
        state["patternId"] = candidate.get("patternId") or candidate.get("pattern") or ""
        state["anchors"] = _copy(candidate.get("anchors")) if isinstance(candidate.get("anchors"), list) else []
        state["sourceInterval"] = candidate.get("sourceInterval")
        state["barCount"] = candidate.get("barCount")
        state["availability"] = candidate.get("availability") or "available"
        state["dataQuality"] = _copy(candidate.get("dataQuality")) if isinstance(candidate.get("dataQuality"), dict) else {}
        state["confluence"] = _copy(candidate.get("confluence")) if isinstance(candidate.get("confluence"), dict) else {}
        state["keyLevel"] = candidate.get("keyLevel")
        state["invalidationLevel"] = candidate.get("invalidationLevel")
        state["targetRange"] = _copy(candidate.get("targetRange"))
        state["evidence"] = _copy(candidate.get("evidence")) if isinstance(candidate.get("evidence"), list) else []
        state["metrics"] = _copy(candidate.get("metrics")) if isinstance(candidate.get("metrics"), dict) else {}
        state["pivotDates"] = _pivot_dates(candidate)
        state["patternFingerprint"] = _candidate_fingerprint(candidate)
        state["patternStartAt"] = candidate.get("patternStartAt")
        state["patternEndAt"] = candidate.get("patternEndAt")
        state["patternWidthBars"] = candidate.get("patternWidthBars") or 0
        state["expiresAfterBars"] = candidate.get("expiresAfterBars")
        state["signalAgeBars"] = candidate.get("signalAgeBars") or 0
        state["latestCompletedBarAt"] = candidate.get("latestCompletedBarAt")
        state["invalidationBuffer"] = candidate.get("invalidationBuffer")
        state["lifecycleState"] = _candidate_lifecycle_state(candidate, candidate_status)
        state["confirmation"] = (
            _copy(candidate.get("confirmation"))
            if isinstance(candidate.get("confirmation"), dict)
            else {}
        )

    @staticmethod
    def _should_notify(
        *,
        data: dict[str, Any],
        state: dict[str, Any],
        from_status: str,
        to_status: str,
        market_date: str,
    ) -> tuple[bool, str | None]:
        if to_status == "expired":
            return False, "expiry_non_notifiable"
        if (from_status, to_status) not in NOTIFIABLE_TRANSITIONS:
            return False, "transition_not_notifiable"

        suppressed_before = str(data.get("notificationSuppressedBefore") or "")
        if suppressed_before and market_date <= suppressed_before:
            return False, "history_rebuild"

        dedupe = state.get("notificationDedupe")
        if isinstance(dedupe, dict) and dedupe.get(to_status) == market_date:
            return False, "daily_duplicate"
        return True, None

    @staticmethod
    def _make_event(
        *,
        state_key: str,
        state: dict[str, Any],
        from_status: str,
        to_status: str,
        market_date: str,
        scan_phase: str,
        changed_at: str,
        should_notify: bool,
        suppression_reason: str | None,
    ) -> dict[str, Any]:
        event = {
            "id": f"evt-{uuid.uuid4().hex}",
            "stateKey": state_key,
            "indexKey": state.get("indexKey"),
            "timeframe": state.get("timeframe"),
            "direction": state.get("direction"),
            "fromStatus": from_status,
            "toStatus": to_status,
            "marketDate": market_date,
            "scanPhase": scan_phase,
            "changedAt": changed_at,
            "signalId": state.get("signalId"),
            "score": state.get("score"),
            "pattern": state.get("pattern"),
            "patternId": state.get("patternId"),
            "anchors": _copy(state.get("anchors")),
            "sourceInterval": state.get("sourceInterval"),
            "barCount": state.get("barCount"),
            "dataQuality": _copy(state.get("dataQuality")),
            "confluence": _copy(state.get("confluence")),
            "keyLevel": state.get("keyLevel"),
            "invalidationLevel": state.get("invalidationLevel"),
            "targetRange": _copy(state.get("targetRange")),
            "pivotDates": _copy(state.get("pivotDates")),
            "evidence": _copy(state.get("evidence")),
            "metrics": _copy(state.get("metrics")),
            "patternFingerprint": state.get("patternFingerprint"),
            "patternStartAt": state.get("patternStartAt"),
            "patternEndAt": state.get("patternEndAt"),
            "patternWidthBars": state.get("patternWidthBars") or 0,
            "expiresAfterBars": state.get("expiresAfterBars"),
            "signalAgeBars": state.get("signalAgeBars") or 0,
            "latestCompletedBarAt": state.get("latestCompletedBarAt"),
            "invalidationBuffer": state.get("invalidationBuffer"),
            "lifecycleState": state.get("lifecycleState") or "none",
            "shouldNotify": should_notify,
            "notificationStatus": "pending" if should_notify else "suppressed",
            "notificationResult": None,
        }
        if suppression_reason:
            event["notificationSuppressionReason"] = suppression_reason
        return event

    def _load_unlocked(self) -> dict[str, Any]:
        if not self.path.exists():
            return _empty_store()

        try:
            with self.path.open("r", encoding="utf-8-sig") as handle:
                data = json.load(handle)
            if not isinstance(data, dict):
                raise ValueError("state root must be an object")
            if not isinstance(data.get("states", {}), dict):
                raise ValueError("states must be an object")
            if not isinstance(data.get("observations", {}), dict):
                raise ValueError("observations must be an object")
            if not isinstance(data.get("events", []), list):
                raise ValueError("events must be an array")
        except Exception:
            return self._recover_corrupt_unlocked()

        data.setdefault("schemaVersion", SCHEMA_VERSION)
        data.setdefault("notificationSuppressedBefore", None)
        data.setdefault("updatedAt", None)
        data.setdefault("states", {})
        data.setdefault("observations", {})
        data.setdefault("events", [])
        data["events"] = data["events"][-MAX_EVENTS:]
        return data

    def _recover_corrupt_unlocked(self) -> dict[str, Any]:
        timestamp = datetime.now().strftime("%Y%m%dT%H%M%S%f")
        backup = self.path.with_name(f"{self.path.name}.corrupt-{timestamp}")
        if self.path.exists():
            self.path.replace(backup)
        data = _empty_store(suppressed_before=_today())
        data["corruptionRecoveredAt"] = _now()
        self._write_unlocked(data)
        return data

    def _write_unlocked(self, data: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.path.with_suffix(self.path.suffix + ".tmp")
        try:
            with tmp_path.open("w", encoding="utf-8", newline="\n") as handle:
                json.dump(data, handle, ensure_ascii=False, indent=2, sort_keys=True)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            tmp_path.replace(self.path)
        finally:
            if tmp_path.exists():
                try:
                    tmp_path.unlink()
                except OSError:
                    pass
