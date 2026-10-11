"""Pure, offline outcome labelling for index-pattern research.

This module deliberately has no network, storage, scheduler, notification, or
live-market dependencies.  It has two separable jobs:

* replay an evaluator over chronological prefixes and emit a signal only on
  the *first bar where that signal is visible*; and
* attach post-event labels (never input to a live evaluator) for a fixed number
  of completed future bars, then aggregate those labels by score bucket.

The prefix contract is important: callbacks receive a deep-copied prefix only,
never the full future series.  Consequently an event's ``eventIndex`` is the
observation bar, not a pivot or a detector's retrospective pattern date.
"""

from __future__ import annotations

from copy import deepcopy
from math import isfinite
from typing import Any, Callable, Iterable, Mapping, Sequence


DEFAULT_HORIZONS: tuple[int, ...] = (5, 10, 20)
ACTIVE_CANDIDATE_STATUSES = frozenset({"candidate", "warning", "resonant", "confirmed"})

# (lower inclusive, upper inclusive or None, display label).  The bands match
# the provisional public score policy while keeping sub-threshold observations
# visible for later calibration instead of silently discarding them.
DEFAULT_SCORE_BUCKETS: tuple[tuple[float | None, float | None, str], ...] = (
    (None, 39.999999, "<40"),
    (40.0, 49.999999, "40-49"),
    (50.0, 59.999999, "50-59"),
    (60.0, 69.999999, "60-69"),
    (70.0, None, "70+"),
)


Number = float | int
Evaluator = Callable[[list[dict[str, Any]]], Any]


def _number(value: Any) -> float | None:
    """Return a finite numeric value, rejecting booleans and NaN/inf."""

    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if isfinite(parsed) else None


def _rows_copy(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Copy chronological OHLC rows without accepting malformed row objects."""

    result: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            raise ValueError(f"row {index} must be a mapping")
        result.append(deepcopy(dict(row)))
    return result


def _normalize_horizons(horizons: Iterable[int] | None) -> tuple[int, ...]:
    values = DEFAULT_HORIZONS if horizons is None else tuple(horizons)
    normalized: list[int] = []
    for value in values:
        if isinstance(value, bool):
            raise ValueError("horizons must contain positive integer bar counts")
        try:
            number = int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError("horizons must contain positive integer bar counts") from exc
        if number <= 0 or number != value:
            raise ValueError("horizons must contain positive integer bar counts")
        if number not in normalized:
            normalized.append(number)
    if not normalized:
        raise ValueError("at least one horizon is required")
    return tuple(sorted(normalized))


def _row_time(row: Mapping[str, Any]) -> str | None:
    for name in ("date", "timestamp", "barEnd", "barEndAt"):
        raw = row.get(name)
        if raw not in (None, ""):
            return str(raw)
    return None


def _canonical_direction(value: Any) -> str | None:
    raw = str(value or "").strip().lower()
    if raw in {"bottom", "bull", "bullish", "long", "up"}:
        return "bottom"
    if raw in {"top", "bear", "bearish", "short", "down"}:
        return "top"
    return None


def _candidate_status(candidate: Mapping[str, Any]) -> str:
    return str(candidate.get("candidateStatus") or candidate.get("status") or "").strip().lower()


def _candidate_is_active(candidate: Mapping[str, Any]) -> bool:
    """Return whether an evaluator result represents a live first-seen signal."""

    lifecycle = str(candidate.get("lifecycleState") or "").strip().lower()
    if lifecycle in {"invalidated", "expired", "none"}:
        return False
    if _candidate_status(candidate) not in ACTIVE_CANDIDATE_STATUSES:
        return False
    return _canonical_direction(candidate.get("direction")) is not None


def _looks_like_candidate(value: Mapping[str, Any]) -> bool:
    candidate_keys = {
        "candidateStatus",
        "status",
        "pattern",
        "patternId",
        "patternFingerprint",
        "direction",
        "lifecycleState",
    }
    return bool(candidate_keys.intersection(value))


def _flatten_evaluator_result(value: Any, *, timeframe_hint: str | None = None) -> list[dict[str, Any]]:
    """Normalise direct, per-timeframe, or list evaluator results into candidates.

    The validation core stays independent from the production evaluator: a
    callback may return one candidate, ``{timeframe: candidate}``, a list, or a
    wrapper containing ``candidates``.  Non-candidate metadata is ignored.
    """

    if isinstance(value, Mapping):
        if _looks_like_candidate(value):
            candidate = deepcopy(dict(value))
            if timeframe_hint and not candidate.get("timeframe"):
                candidate["timeframe"] = timeframe_hint
            return [candidate]

        result: list[dict[str, Any]] = []
        wrapped = value.get("candidates")
        if isinstance(wrapped, (list, tuple)):
            for item in wrapped:
                result.extend(_flatten_evaluator_result(item, timeframe_hint=timeframe_hint))
        for key, item in value.items():
            if key == "candidates" or not isinstance(item, (Mapping, list, tuple)):
                continue
            hint = str(key) if isinstance(item, Mapping) else timeframe_hint
            result.extend(_flatten_evaluator_result(item, timeframe_hint=hint))
        return result
    if isinstance(value, (list, tuple)):
        result = []
        for item in value:
            result.extend(_flatten_evaluator_result(item, timeframe_hint=timeframe_hint))
        return result
    return []


def _stable_identity_part(value: Any) -> str:
    """Create a deterministic representation suitable only for in-memory replay."""

    if isinstance(value, Mapping):
        return "{" + ",".join(
            f"{str(key)}:{_stable_identity_part(child)}"
            for key, child in sorted(value.items(), key=lambda item: str(item[0]))
        ) + "}"
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(_stable_identity_part(item) for item in value) + "]"
    return repr(value)


def _event_identity(candidate: Mapping[str, Any]) -> str:
    """Return a conservative lifecycle identity without using moving latest bars.

    A detector-provided fingerprint is authoritative.  When it is unavailable,
    only structural dates/anchors are used.  The final conservative fallback
    intentionally emits at most one event for a timeframe/direction/pattern
    during a replay rather than creating a false new event on every new bar.
    """

    timeframe = str(candidate.get("timeframe") or "unknown").strip().lower() or "unknown"
    direction = _canonical_direction(candidate.get("direction")) or "unknown"
    pattern = str(candidate.get("patternId") or candidate.get("pattern") or "unknown").strip() or "unknown"
    base = (timeframe, direction, pattern)

    for name in ("patternFingerprint", "signalFingerprint", "fingerprint", "signalId"):
        supplied = str(candidate.get(name) or "").strip()
        if supplied:
            return _stable_identity_part(("fingerprint", supplied))

    structural: list[tuple[str, Any]] = []
    for name in (
        "patternStartAt",
        "patternEndAt",
        "patternStartIndex",
        "patternEndIndex",
        "firstPivotDate",
        "secondPivotDate",
    ):
        value = candidate.get(name)
        if value not in (None, ""):
            structural.append((name, value))
    anchors = candidate.get("identityAnchors")
    if isinstance(anchors, (list, tuple)) and anchors:
        structural.append(("identityAnchors", anchors))
    pivot_dates = candidate.get("pivotDates")
    if isinstance(pivot_dates, (list, tuple)) and pivot_dates:
        structural.append(("pivotDates", pivot_dates))

    return _stable_identity_part(("structural", base, structural)) if structural else _stable_identity_part(base)


def replay_signal_events(
    rows: Iterable[Mapping[str, Any]],
    evaluator: Evaluator,
    *,
    start_index: int = 0,
) -> list[dict[str, Any]]:
    """Replay *evaluator* in chronological prefixes and emit first-visible signals.

    ``evaluator`` receives exactly one argument: a private deep copy of the
    rows from index ``0`` through the current observation bar (inclusive).  It
    must not receive the full series.  The returned event is stamped with that
    current index/date/close, even when the detected pattern refers to older
    pivots.  A stable fingerprint is emitted only once per replay.
    """

    if not callable(evaluator):
        raise ValueError("evaluator must be callable")
    source_rows = _rows_copy(rows)
    if isinstance(start_index, bool) or not isinstance(start_index, int) or start_index < 0:
        raise ValueError("start_index must be a non-negative integer")

    seen: set[str] = set()
    events: list[dict[str, Any]] = []
    for current_index in range(start_index, len(source_rows)):
        # Each invocation gets a newly copied prefix.  This protects the raw
        # series even if an experimental evaluator mutates an input row.
        prefix = deepcopy(source_rows[: current_index + 1])
        evaluated = evaluator(prefix)
        for candidate in _flatten_evaluator_result(evaluated):
            if not _candidate_is_active(candidate):
                continue
            identity = _event_identity(candidate)
            if identity in seen:
                continue
            seen.add(identity)
            event = deepcopy(candidate)
            direction = _canonical_direction(event.get("direction"))
            event["direction"] = direction
            event["timeframe"] = str(event.get("timeframe") or "unknown").strip().lower() or "unknown"
            event["patternId"] = str(event.get("patternId") or event.get("pattern") or "unknown")
            event["eventKey"] = identity
            # Authoritative event clock: the evaluator saw the signal only on
            # this completed prefix endpoint, never on a retrospective pivot.
            event["eventIndex"] = current_index
            event["eventAt"] = _row_time(source_rows[current_index])
            event["entryClose"] = _number(source_rows[current_index].get("close"))
            events.append(event)
    return events


def sequential_prefix_replay(
    rows: Iterable[Mapping[str, Any]],
    evaluator: Evaluator,
    *,
    start_index: int = 0,
) -> list[dict[str, Any]]:
    """Named alias for :func:`replay_signal_events` used by research callers."""

    return replay_signal_events(rows, evaluator, start_index=start_index)


# A readable synonym for callers that use "prefix events" in their notebooks.
replay_prefix_events = replay_signal_events


def _target_level(event: Mapping[str, Any], direction: str) -> float | None:
    direct = _number(event.get("targetLevel"))
    if direct is not None:
        return direct
    values = event.get("targetRange")
    if not isinstance(values, (list, tuple)):
        return None
    normalized = [_number(value) for value in values]
    valid = [value for value in normalized if value is not None]
    if not valid:
        return None
    # Shape targets are ranges.  Label the conservative edge that a price must
    # encounter first: lower bound for bullish/bottom, upper bound for bearish/top.
    return min(valid) if direction == "bottom" else max(valid)


def _invalidation_level(event: Mapping[str, Any]) -> tuple[float | None, float]:
    level = _number(event.get("invalidationLevel"))
    if level is None:
        level = _number(event.get("invalidation"))
    buffer = _number(event.get("invalidationBuffer"))
    return level, abs(buffer) if buffer is not None else 0.0


def _unavailable_outcome(
    *,
    horizon: int,
    bars_observed: int,
    reason: str,
    entry_close: float | None,
) -> dict[str, Any]:
    return {
        "horizon": horizon,
        "available": False,
        "sampleCount": 0,
        "barsObserved": bars_observed,
        "reason": reason,
        "entryClose": entry_close,
        "exitClose": None,
        "directionalReturnPct": None,
        "mfePct": None,
        "maePct": None,
        "targetLevel": None,
        "invalidationLevel": None,
        "targetBeforeInvalidation": None,
        "targetOutcome": "unavailable",
        "targetHitIndex": None,
        "invalidationHitIndex": None,
    }


def _target_outcome(
    future_rows: Sequence[Mapping[str, Any]],
    *,
    event_index: int,
    direction: str,
    target_level: float | None,
    invalidation_level: float | None,
    invalidation_buffer: float,
) -> tuple[bool | None, str, int | None, int | None]:
    """Resolve endpoint order without inventing an intrabar sequence."""

    if target_level is None:
        return None, "target_unavailable", None, None

    target_hit: int | None = None
    invalidation_hit: int | None = None
    for offset, row in enumerate(future_rows, start=1):
        close = _number(row.get("close"))
        high = _number(row.get("high"))
        low = _number(row.get("low"))
        high = high if high is not None else close
        low = low if low is not None else close
        if high is None or low is None:
            continue
        absolute_index = event_index + offset
        hit_target = high >= target_level if direction == "bottom" else low <= target_level
        if invalidation_level is None:
            hit_invalidation = False
        elif direction == "bottom":
            hit_invalidation = low <= invalidation_level - invalidation_buffer
        else:
            hit_invalidation = high >= invalidation_level + invalidation_buffer
        if hit_target and target_hit is None:
            target_hit = absolute_index
        if hit_invalidation and invalidation_hit is None:
            invalidation_hit = absolute_index

    if target_hit is not None and invalidation_hit is not None:
        if target_hit < invalidation_hit:
            return True, "target_before_invalidation", target_hit, invalidation_hit
        if invalidation_hit < target_hit:
            return False, "invalidation_before_target", target_hit, invalidation_hit
        # OHLC has no path ordering inside a bar.  Treat this as unknown rather
        # than biasing target success or failure in a backtest.
        return None, "same_bar_ambiguous", target_hit, invalidation_hit
    if target_hit is not None:
        return True, "target_before_invalidation", target_hit, None
    if invalidation_hit is not None:
        return False, "invalidation_before_target", None, invalidation_hit
    return None, "not_reached", None, None


def _complete_outcome(
    future_rows: Sequence[Mapping[str, Any]],
    *,
    horizon: int,
    event_index: int,
    entry_close: float,
    direction: str,
    target_level: float | None,
    invalidation_level: float | None,
    invalidation_buffer: float,
) -> dict[str, Any]:
    exit_close = _number(future_rows[-1].get("close"))
    if exit_close is None:
        return _unavailable_outcome(
            horizon=horizon,
            bars_observed=len(future_rows),
            reason="missing_exit_close",
            entry_close=entry_close,
        )

    highs: list[float] = []
    lows: list[float] = []
    for row in future_rows:
        close = _number(row.get("close"))
        high = _number(row.get("high"))
        low = _number(row.get("low"))
        high = high if high is not None else close
        low = low if low is not None else close
        if high is None or low is None:
            return _unavailable_outcome(
                horizon=horizon,
                bars_observed=len(future_rows),
                reason="missing_price_data",
                entry_close=entry_close,
            )
        highs.append(high)
        lows.append(low)

    sign = 1.0 if direction == "bottom" else -1.0
    directional_return = sign * (exit_close - entry_close) / entry_close * 100.0
    if direction == "bottom":
        mfe = max((high - entry_close) / entry_close * 100.0 for high in highs)
        mae = min((low - entry_close) / entry_close * 100.0 for low in lows)
    else:
        mfe = max((entry_close - low) / entry_close * 100.0 for low in lows)
        mae = min((entry_close - high) / entry_close * 100.0 for high in highs)

    target_before, target_outcome, target_hit, invalidation_hit = _target_outcome(
        future_rows,
        event_index=event_index,
        direction=direction,
        target_level=target_level,
        invalidation_level=invalidation_level,
        invalidation_buffer=invalidation_buffer,
    )
    return {
        "horizon": horizon,
        "available": True,
        "sampleCount": 1,
        "barsObserved": len(future_rows),
        "reason": None,
        "entryClose": entry_close,
        "exitClose": exit_close,
        "directionalReturnPct": directional_return,
        "mfePct": mfe,
        "maePct": mae,
        "targetLevel": target_level,
        "invalidationLevel": invalidation_level,
        "targetBeforeInvalidation": target_before,
        "targetOutcome": target_outcome,
        "targetHitIndex": target_hit,
        "invalidationHitIndex": invalidation_hit,
    }


def label_signal_outcomes(
    rows: Iterable[Mapping[str, Any]],
    events: Iterable[Mapping[str, Any]],
    horizons: Iterable[int] = DEFAULT_HORIZONS,
) -> list[dict[str, Any]]:
    """Attach future-bar labels to first-visible signal events.

    Each full horizon uses bars ``eventIndex + 1`` through
    ``eventIndex + horizon``.  A partial tail is deliberately not counted as a
    sample, preventing a newer event from being scored on fewer bars merely
    because the dataset ended.  Direction-normalized return/MFE/MAE are based
    on the event-bar close; ``bottom`` is bullish and ``top`` is bearish.

    These labels are strictly for offline calibration and outcome reporting.
    They must never be fed back into a live signal evaluation at the event
    timestamp.
    """

    source_rows = _rows_copy(rows)
    normalized_horizons = _normalize_horizons(horizons)
    labeled: list[dict[str, Any]] = []
    for event_number, raw_event in enumerate(events):
        if not isinstance(raw_event, Mapping):
            raise ValueError(f"event {event_number} must be a mapping")
        event = deepcopy(dict(raw_event))
        raw_index = event.get("eventIndex", event.get("index"))
        if isinstance(raw_index, bool):
            raise ValueError(f"event {event_number} has an invalid eventIndex")
        try:
            event_index = int(raw_index)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"event {event_number} has an invalid eventIndex") from exc
        if event_index < 0 or event_index >= len(source_rows) or event_index != raw_index:
            raise ValueError(f"event {event_number} eventIndex is outside supplied rows")

        direction = _canonical_direction(event.get("direction"))
        entry_close = _number(source_rows[event_index].get("close"))
        event["eventIndex"] = event_index
        event["eventAt"] = event.get("eventAt") or _row_time(source_rows[event_index])
        # An arbitrary candidate-provided entry price cannot replace the
        # observation bar's close, which is the only fill proxy available
        # without looking ahead.
        event["entryClose"] = entry_close
        outcomes: dict[int, dict[str, Any]] = {}

        for horizon in normalized_horizons:
            future_rows = source_rows[event_index + 1 : event_index + horizon + 1]
            if len(future_rows) < horizon:
                outcomes[horizon] = _unavailable_outcome(
                    horizon=horizon,
                    bars_observed=len(future_rows),
                    reason="insufficient_complete_bars",
                    entry_close=entry_close,
                )
                continue
            if direction is None:
                outcomes[horizon] = _unavailable_outcome(
                    horizon=horizon,
                    bars_observed=len(future_rows),
                    reason="unsupported_direction",
                    entry_close=entry_close,
                )
                continue
            if entry_close is None or entry_close <= 0:
                outcomes[horizon] = _unavailable_outcome(
                    horizon=horizon,
                    bars_observed=len(future_rows),
                    reason="invalid_entry_close",
                    entry_close=entry_close,
                )
                continue
            target_level = _target_level(event, direction)
            invalidation_level, invalidation_buffer = _invalidation_level(event)
            outcomes[horizon] = _complete_outcome(
                future_rows,
                horizon=horizon,
                event_index=event_index,
                entry_close=entry_close,
                direction=direction,
                target_level=target_level,
                invalidation_level=invalidation_level,
                invalidation_buffer=invalidation_buffer,
            )
        event["outcomes"] = outcomes
        labeled.append(event)
    return labeled


def score_bucket_for(
    score: Number,
    score_buckets: Sequence[tuple[float | None, float | None, str]] = DEFAULT_SCORE_BUCKETS,
) -> str | None:
    """Return the deterministic display band for a finite numeric score."""

    numeric = _number(score)
    if numeric is None:
        return None
    for minimum, maximum, label in score_buckets:
        if (minimum is None or numeric >= minimum) and (maximum is None or numeric <= maximum):
            return str(label)
    return None


def _outcome_for_horizon(event: Mapping[str, Any], horizon: int) -> Mapping[str, Any] | None:
    outcomes = event.get("outcomes")
    if not isinstance(outcomes, Mapping):
        return None
    direct = outcomes.get(horizon)
    if isinstance(direct, Mapping):
        return direct
    string_key = outcomes.get(str(horizon))
    return string_key if isinstance(string_key, Mapping) else None


def summarize_score_buckets(
    labeled_events: Iterable[Mapping[str, Any]],
    *,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    score_buckets: Sequence[tuple[float | None, float | None, str]] = DEFAULT_SCORE_BUCKETS,
) -> list[dict[str, Any]]:
    """Summarize complete labels by timeframe, pattern, score band and horizon.

    Every represented group receives one row for every requested horizon.  When
    a newer event has no complete future bars, its group's ``n`` is zero and
    ``winRate``/averages are ``None`` rather than a misleading zero.  ``n`` is
    the count of full, finite direction-normalized return labels.
    """

    normalized_horizons = _normalize_horizons(horizons)
    groups: dict[tuple[str, str, str, int], list[Mapping[str, Any]]] = {}
    for raw_event in labeled_events:
        if not isinstance(raw_event, Mapping):
            continue
        bucket = score_bucket_for(raw_event.get("score"), score_buckets)
        if bucket is None:
            continue
        timeframe = str(raw_event.get("timeframe") or "unknown").strip().lower() or "unknown"
        pattern_id = str(raw_event.get("patternId") or raw_event.get("pattern") or "unknown").strip() or "unknown"
        for horizon in normalized_horizons:
            groups.setdefault((timeframe, pattern_id, bucket, horizon), []).append(raw_event)

    summaries: list[dict[str, Any]] = []
    for (timeframe, pattern_id, bucket, horizon), members in sorted(groups.items()):
        usable: list[Mapping[str, Any]] = []
        for event in members:
            outcome = _outcome_for_horizon(event, horizon)
            if not isinstance(outcome, Mapping) or outcome.get("sampleCount") != 1:
                continue
            if outcome.get("available") is not True or _number(outcome.get("directionalReturnPct")) is None:
                continue
            usable.append(outcome)

        returns = [_number(item.get("directionalReturnPct")) for item in usable]
        mfes = [_number(item.get("mfePct")) for item in usable]
        maes = [_number(item.get("maePct")) for item in usable]
        return_values = [value for value in returns if value is not None]
        mfe_values = [value for value in mfes if value is not None]
        mae_values = [value for value in maes if value is not None]
        n = len(return_values)
        resolved_target_order = [
            item.get("targetBeforeInvalidation")
            for item in usable
            if item.get("targetBeforeInvalidation") in {True, False}
        ]
        summaries.append(
            {
                "timeframe": timeframe,
                "patternId": pattern_id,
                "scoreBucket": bucket,
                "horizon": horizon,
                "n": n,
                "winRate": (sum(value > 0 for value in return_values) / n) if n else None,
                "averageDirectionalReturnPct": (sum(return_values) / n) if n else None,
                "averageMfePct": (sum(mfe_values) / len(mfe_values)) if mfe_values else None,
                "averageMaePct": (sum(mae_values) / len(mae_values)) if mae_values else None,
                "targetBeforeInvalidationRate": (
                    sum(value is True for value in resolved_target_order) / len(resolved_target_order)
                    if resolved_target_order
                    else None
                ),
            }
        )
    return summaries


__all__ = [
    "ACTIVE_CANDIDATE_STATUSES",
    "DEFAULT_HORIZONS",
    "DEFAULT_SCORE_BUCKETS",
    "label_signal_outcomes",
    "replay_prefix_events",
    "replay_signal_events",
    "score_bucket_for",
    "sequential_prefix_replay",
    "summarize_score_buckets",
]
