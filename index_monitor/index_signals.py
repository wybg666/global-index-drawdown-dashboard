"""Pure indicator and top/bottom candidate calculations for index telemetry.

The module deliberately has no storage, network, scheduling, or notification
dependencies.  Callers can therefore evaluate the same immutable market-data
snapshot repeatedly and receive deterministic short/medium candidates.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from math import ceil, isfinite
from typing import Any, Iterable


Number = float | int


@dataclass(frozen=True)
class _TimeframeConfig:
    id: str
    label: str
    interval: str
    bar_count: int
    role: str
    patterns: tuple[str, ...]
    lookback: int
    pivot_side: int
    min_separation: int
    min_rows: int
    warning: int
    confirm: int
    confirmation_policy: str


_TIMEFRAMES = {
    # Never map these labels to different windows of the same daily series.
    # ``short`` and ``medium`` consume actual 15-minute and 60-minute bars
    # supplied by the market-data layer; only ``long`` consumes daily bars.
    "short": _TimeframeConfig(
        "short",
        "短期 · 15分钟",
        "15m",
        # Retain enough independent 15m history to compute a 20-session
        # same-clock-slot volume baseline.  Pattern detection still consumes
        # only the most recent ``lookback`` bars below.
        600,
        "候选发现",
        (
            "double_top",
            "double_bottom",
            "bear_flag",
            "bull_flag",
            "rising_wedge",
            "falling_wedge",
        ),
        200,
        3,
        5,
        60,
        40,
        55,
        "requires_higher_timeframe",
    ),
    "medium": _TimeframeConfig(
        "medium",
        "中期 · 60分钟",
        "60m",
        # 400 completed 60m bars likewise retains far more than the 200-bar
        # detection window, so same-slot RVOL does not need to borrow midday
        # volume for an opening bar.
        400,
        "方向过滤 / 共振",
        (
            "head_shoulders_top",
            "head_shoulders_bottom",
            "triple_top",
            "triple_bottom",
            "bear_channel_break",
            "bull_channel_break",
        ),
        200,
        5,
        10,
        80,
        45,
        60,
        "requires_daily_alignment",
    ),
    "long": _TimeframeConfig(
        "long",
        "长期 · 日线",
        "1d",
        320,
        "趋势背景与收盘确认",
        (
            "double_top",
            "double_bottom",
            "head_shoulders_top",
            "head_shoulders_bottom",
            "rounded_bottom",
            "v_top",
            "v_bottom",
        ),
        120,
        5,
        10,
        80,
        50,
        65,
        "market_close_only",
    ),
}

TIMEFRAME_IDS = tuple(_TIMEFRAMES)
TIMEFRAME_MIN_ROWS = {timeframe: config.min_rows for timeframe, config in _TIMEFRAMES.items()}
# Number of provider bars retained for each source frame.  This is deliberately
# not the 200-bar pattern window and is imported by the fetch layer so its
# request plan cannot drift away from the public rule catalog.
TIMEFRAME_FETCH_BARS = {timeframe: config.bar_count for timeframe, config in _TIMEFRAMES.items()}

# The longest score-critical indicator preparation is MACD(12, 26, 9).  MA60
# is a display metric only; pattern qualification and score evidence use at
# most the 20-bar MA/volume series, RSI14, ATR14, and MACD.
INDICATOR_WARMUP_BARS = 35
RVOL_HISTORY_DAYS = 20
SCORE_MAXIMUM = 80
TIMEFRAME_REQUIRED_BARS = {
    timeframe: config.lookback + INDICATOR_WARMUP_BARS + config.pivot_side
    for timeframe, config in _TIMEFRAMES.items()
}
CONFIRMATION_CATEGORIES = frozenset({"trend", "momentum", "volume", "relative_strength"})
CORRELATED_PRICE_CATEGORIES = frozenset({"trend", "momentum", "relative_strength"})
CORRELATED_PRICE_CONFIRMATION_CAP = 35


def _validate_timeframe_contract() -> None:
    for timeframe, config in _TIMEFRAMES.items():
        required = TIMEFRAME_REQUIRED_BARS[timeframe]
        if config.bar_count < required:
            raise ValueError(
                f"{timeframe} bar_count={config.bar_count} is below required "
                f"lookback+indicator_warmup+pivot_side={required}"
            )


_validate_timeframe_contract()
RULE_VERSION = "2026.08-decision-1"

_TIMEFRAME_DETECTION_RULES = {
    "short": (
        "双顶/底按两次同区枢轴与颈线回撤识别；旗形要求强旗杆、反向整理并向旗杆方向脱离；"
        "楔形要求高低枢轴边界收敛且价格接近或突破边界。"
    ),
    "medium": (
        "头肩形态要求肩部近似、头部更高/更低与颈线；三重形态要求三次同区触碰；"
        "通道突破要求上下边界各至少三次枢轴触碰、边界近似平行且收盘有效越界。"
    ),
    "long": (
        "双顶/底、头肩与V形按日线枢轴和颈线识别；圆弧底要求低点位于中段、"
        "底部停留区足够宽且进出底部斜率呈弧形，直线V形不标为圆弧底。"
    ),
}


def index_signal_rules() -> dict[str, Any]:
    """Return the public, versioned rules rendered by the index-signal pages.

    Keeping the page copy in this module prevents the dashboard from claiming
    old daily-window labels after the evaluator has moved to real bar periods.
    """

    return {
        "version": RULE_VERSION,
        "timeframes": [
            {
                "id": config.id,
                "label": config.label,
                "interval": config.interval,
                "barCount": config.bar_count,
                "role": config.role,
                "patterns": list(config.patterns),
                "detectionRule": _TIMEFRAME_DETECTION_RULES[config.id],
                "lookback": config.lookback,
                "indicatorWarmupBars": INDICATOR_WARMUP_BARS,
                "requiredBarCount": TIMEFRAME_REQUIRED_BARS[config.id],
                # ``config.min_rows`` is an internal detector heuristic.  The
                # public API must instead state the full operational contract:
                # detection window + indicator warm-up + pivot confirmation.
                "minimumOperationalBars": TIMEFRAME_REQUIRED_BARS[config.id],
                "minRows": TIMEFRAME_REQUIRED_BARS[config.id],
                "pivotSide": config.pivot_side,
                "minSeparation": config.min_separation,
                "warningThreshold": config.warning,
                # 15m/60m may become candidates or resonant with a higher
                # frame, but never independently confirmed.  Publishing their
                # internal score cutoffs as confirmation thresholds would
                # falsely imply a trade-grade single-period trigger.
                "confirmThreshold": (
                    config.confirm
                    if config.confirmation_policy == "market_close_only"
                    else None
                ),
                "confirmThresholdApplicability": (
                    "market_close_only"
                    if config.confirmation_policy == "market_close_only"
                    else "not_applicable"
                ),
                "confirmationPolicy": config.confirmation_policy,
                # ``barCount`` is source retention.  It is deliberately
                # separate from the detector's ``lookback`` and its indicator
                # warm-up contract above.
                "fetchBarCount": config.bar_count,
                "rvolHistoryDays": RVOL_HISTORY_DAYS if config.interval != "1d" else None,
                "volumePolicy": "same_slot_rvol20" if config.interval != "1d" else "volume_ma20",
            }
            for config in _TIMEFRAMES.values()
        ],
        "shared": {
            # Structure can contribute at most 35; all price-derived
            # confirmations share one 35-point correlation cap; volume adds at
            # most 10.  The public score scale is therefore 80, not the old
            # additive 100-point wording.
            "scoreMaximum": SCORE_MAXIMUM,
            "scoreScale": {
                "structureMaximum": 35,
                "correlatedPriceConfirmationMaximum": CORRELATED_PRICE_CONFIRMATION_CAP,
                "volumeMaximum": 10,
                "maximum": SCORE_MAXIMUM,
            },
            "scoreSummary": "结构最高35｜趋势/动量/相对强弱合计最高35（相关性封顶）｜成交量最高10｜总分最高80",
            "minEvidenceCategories": 2,
            "correlatedPriceConfirmationCap": CORRELATED_PRICE_CONFIRMATION_CAP,
            "missingVolumePolicy": "zero_contribution_no_score_renormalization",
            "intradayVolumePolicy": "same_slot_rvol20_prior_distinct_local_trading_days",
            "rvolHistoryDays": RVOL_HISTORY_DAYS,
            "thresholdCalibration": {
                # Compatibility surface retained for existing API consumers.
                "status": "provisional",
                "requirement": "pending_out_of_sample_calibration",
                # Calibration is a research artifact, never an online learning
                # loop: each event is emitted only when first visible on a
                # completed prefix, not retrospectively at a pivot.
                "strictOutOfSample": True,
                "evaluationMethod": "sequential_prefix_replay",
                "eventTiming": "first_visible_completed_bar",
                "sampleEligibility": "full_horizon_completed_bars_only",
                "groupBy": ["timeframe", "patternId", "scoreBucket"],
                "scoreBuckets": ["<40", "40-49", "50-59", "60-69", "70+"],
                "horizonsBars": [5, 10, 20],
                "outcomeMetrics": [
                    "directionalReturnPct",
                    "mfePct",
                    "maePct",
                    "targetBeforeInvalidation",
                ],
                # OHLC cannot establish intrabar ordering if both boundaries
                # are touched in one bar.  Such cases must not manufacture a
                # target win or a stop loss in calibration.
                "sameBarTargetInvalidation": "ambiguous_not_success_or_failure",
                "sameTimeSnapshotDefinition": "same_time_trading_day_snapshot",
                "minimumSameTimeSnapshots": 40,
                "minimumQualifiedSamples": 40,
                "statusBeforeMinimum": "provisional",
                "promotionCriteria": [
                    "strict_out_of_sample",
                    "same_time_snapshots_gte_40",
                    "qualified_samples_gte_40",
                    "manual_review_required",
                ],
                "automaticThresholdUpdates": False,
                "automaticTrading": False,
                "manualReviewRequired": True,
            },
            "decisionState": {
                "mode": "observe_manual_review",
                "automaticTrading": False,
                "description": "仅观察 / 人工复核；确认不是交易，也不触发自动下单。",
            },
            "confirmationBuffer": "max(关键位×0.5%, ATR14×0.25)",
            "intradayPolicy": "15分钟与60分钟只产生候选/预警，不以盘中未完成K线单独确认。",
            "confluencePolicy": "15分钟仅在60分钟同向有效时标注共振；60分钟仅在日线同向有效时标注共振；15分钟与日线跨越60分钟的同向仅作非相邻对齐，不升级。任一周期出现反向有效信号即标记跨周期冲突并阻断共振升级；日线候选/收盘确认不被低周期升级或降级。",
            "confluenceContract": {
                "shortPromotion": "same_direction_medium_active",
                "mediumPromotion": "same_direction_daily_active",
                "nonAdjacentAlignment": "nonadjacent_alignment_only",
                "oppositeDirection": "cross_timeframe_conflict_blocks_promotion",
                "dailyRole": "terminal_no_intraday_promotion_or_demotion",
            },
        },
    }


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if isfinite(parsed) else None


def _rounded(value: float | None, digits: int = 4) -> float | None:
    return None if value is None else round(float(value), digits)


def sma(values: list[float | None], period: int) -> list[float | None]:
    """Return a simple moving-average series aligned to *values*."""

    result: list[float | None] = [None] * len(values)
    if period <= 0:
        return result
    for index in range(period - 1, len(values)):
        window = values[index - period + 1 : index + 1]
        normalized = [_number(value) for value in window]
        if any(value is None for value in normalized):
            continue
        result[index] = sum(value for value in normalized if value is not None) / period
    return result


def ema(values: list[float | None], period: int) -> list[float | None]:
    """Return an EMA seeded by the first available value, resetting on gaps."""

    result: list[float | None] = [None] * len(values)
    if period <= 0:
        return result
    multiplier = 2.0 / (period + 1.0)
    previous: float | None = None
    for index, raw_value in enumerate(values):
        value = _number(raw_value)
        if value is None:
            previous = None
            continue
        previous = value if previous is None else previous + multiplier * (value - previous)
        result[index] = previous
    return result


def rsi(values: list[float | None], period: int = 14) -> list[float | None]:
    """Return Wilder RSI with an arithmetic seed over the first full period."""

    result: list[float | None] = [None] * len(values)
    if period <= 0:
        return result

    average_gain: float | None = None
    average_loss: float | None = None
    for index in range(1, len(values)):
        current = _number(values[index])
        previous = _number(values[index - 1])
        if current is None or previous is None:
            average_gain = None
            average_loss = None
            continue

        change = current - previous
        gain = max(change, 0.0)
        loss = max(-change, 0.0)
        if average_gain is None or average_loss is None:
            if index < period:
                continue
            seed = [_number(value) for value in values[index - period : index + 1]]
            if any(value is None for value in seed):
                continue
            changes = [
                float(seed[position]) - float(seed[position - 1])
                for position in range(1, len(seed))
            ]
            average_gain = sum(max(item, 0.0) for item in changes) / period
            average_loss = sum(max(-item, 0.0) for item in changes) / period
        else:
            average_gain = (average_gain * (period - 1) + gain) / period
            average_loss = (average_loss * (period - 1) + loss) / period

        if average_loss == 0:
            result[index] = 50.0 if average_gain == 0 else 100.0
        else:
            relative_strength = average_gain / average_loss
            result[index] = 100.0 - 100.0 / (1.0 + relative_strength)
    return result


def macd(values: list[float | None]) -> dict[str, list[float | None]]:
    """Return aligned MACD(12, 26, 9) line, signal, and histogram series."""

    fast = ema(values, 12)
    slow = ema(values, 26)
    line = [
        None if fast_value is None or slow_value is None else fast_value - slow_value
        for fast_value, slow_value in zip(fast, slow)
    ]
    signal = ema(line, 9)
    histogram = [
        None if line_value is None or signal_value is None else line_value - signal_value
        for line_value, signal_value in zip(line, signal)
    ]
    return {"line": line, "signal": signal, "histogram": histogram}


def atr(rows: list[dict], period: int = 14) -> list[float | None]:
    """Return Wilder ATR aligned to the supplied OHLC rows."""

    result: list[float | None] = [None] * len(rows)
    if period <= 0:
        return result

    true_ranges: list[float | None] = []
    previous_close: float | None = None
    for row in rows:
        high = _number(row.get("high"))
        low = _number(row.get("low"))
        close = _number(row.get("close"))
        if high is None or low is None:
            true_range = None
        elif previous_close is None:
            true_range = high - low
        else:
            true_range = max(high - low, abs(high - previous_close), abs(low - previous_close))
        true_ranges.append(true_range)
        previous_close = close

    previous_average: float | None = None
    for index, true_range in enumerate(true_ranges):
        if true_range is None:
            previous_average = None
            continue
        if previous_average is None:
            if index < period - 1:
                continue
            seed = true_ranges[index - period + 1 : index + 1]
            if any(value is None for value in seed):
                continue
            previous_average = sum(float(value) for value in seed if value is not None) / period
        else:
            previous_average = (previous_average * (period - 1) + true_range) / period
        result[index] = previous_average
    return result


def find_pivots(rows: list[dict], side: int) -> dict[str, list[int]]:
    """Return strict local high/low indexes with *side* bars on both sides."""

    pivots = {"highs": [], "lows": []}
    if side <= 0 or len(rows) < side * 2 + 1:
        return pivots

    for index in range(side, len(rows) - side):
        high = _number(rows[index].get("high"))
        low = _number(rows[index].get("low"))
        neighbors = rows[index - side : index] + rows[index + 1 : index + side + 1]
        neighbor_highs = [_number(row.get("high")) for row in neighbors]
        neighbor_lows = [_number(row.get("low")) for row in neighbors]
        if high is not None and all(value is not None and high > value for value in neighbor_highs):
            pivots["highs"].append(index)
        if low is not None and all(value is not None and low < value for value in neighbor_lows):
            pivots["lows"].append(index)
    return pivots


def _last(series: list[float | None]) -> float | None:
    return series[-1] if series else None


def _metrics(indicators: dict[str, Any]) -> dict[str, float | None]:
    return {
        "ma20": _rounded(_last(indicators["ma20"])),
        "ma60": _rounded(_last(indicators["ma60"])),
        "rsi14": _rounded(_last(indicators["rsi14"])),
        "atr14": _rounded(_last(indicators["atr14"])),
    }


_LIFECYCLE_STATES = frozenset(
    {"candidate", "resonant", "confirmed", "invalidated", "expired", "none"}
)


def _row_timestamp(rows: list[dict], index: int) -> str | None:
    """Return a completed provider bar timestamp without normalising its timezone."""

    if not 0 <= index < len(rows):
        return None
    value = str(rows[index].get("date") or "").strip()
    return value or None


def _structural_lifecycle_anchors(
    candidate: dict[str, Any] | None,
    rows: list[dict],
) -> list[dict[str, Any]]:
    """Return detector-declared immutable identity anchors for a lifecycle.

    ``anchors`` is a display/scoring surface and may legitimately include the
    newest close or a rolling-window edge.  It must never be silently promoted
    into lifecycle identity: doing so remints a signal every completed bar.
    Every pattern detector therefore declares ``identityAnchors`` separately.
    Older/simple structural candidates may use the strictly bounded
    ``firstIndex``/``secondIndex`` fallback, but an observation-bar index is
    deliberately rejected there as well.
    """

    if not isinstance(candidate, dict):
        return []
    def normalise(raw_anchors: Any) -> list[dict[str, Any]]:
        anchors: list[dict[str, Any]] = []
        if not isinstance(raw_anchors, (list, tuple)):
            return anchors
        for raw in raw_anchors:
            if not isinstance(raw, dict):
                continue
            kind = str(raw.get("kind") or "").strip()
            try:
                index = int(raw.get("index"))
            except (TypeError, ValueError):
                continue
            # Identity must not be the observation bar.  A detector may show
            # that bar in display anchors, but it is not immutable yet.
            if not kind or not 0 <= index < max(0, len(rows) - 1):
                continue
            timestamp = str(raw.get("date") or _row_timestamp(rows, index) or "").strip()
            if not timestamp:
                continue
            anchors.append({"kind": kind, "index": index, "date": timestamp})
        unique = {(item["kind"], item["index"], item["date"]): item for item in anchors}
        return sorted(unique.values(), key=lambda item: (item["index"], item["kind"], item["date"]))

    # A present declaration is authoritative.  Do not fall back to display
    # anchors if a detector accidentally declares no valid immutable anchor;
    # that failure must remain observable instead of creating a moving ID.
    if "identityAnchors" in candidate:
        return normalise(candidate.get("identityAnchors"))

    # Safe structural fallback for legacy/simple candidates.  These names are
    # part of the candidate's structural contract, unlike arbitrary display
    # anchors.  Both must precede the current observation bar.
    anchors: list[dict[str, Any]] = []
    fallback_indexes = (
        ("structural_start", candidate.get("firstIndex")),
        ("structural_end", candidate.get("secondIndex")),
    )
    for kind, raw_index in fallback_indexes:
        try:
            index = int(raw_index)
        except (TypeError, ValueError):
            continue
        if not 0 <= index < max(0, len(rows) - 1):
            continue
        timestamp = _row_timestamp(rows, index)
        if timestamp is not None:
            anchors.append({"kind": kind, "index": index, "date": timestamp})

    return normalise(anchors)


def _lifecycle_metadata(
    *,
    candidate: dict[str, Any] | None,
    rows: list[dict],
    config: _TimeframeConfig | None,
    direction: str | None,
    status: str,
    latest_atr: float | None,
    lifecycle_state: str | None = None,
) -> dict[str, Any]:
    """Publish structural signal identity, elapsed completed bars, and state.

    This evaluator only observes a completed-row snapshot.  It exposes expiry
    metadata for the persistence layer to reconcile later; it deliberately
    keeps the legacy ``candidateStatus`` untouched in this phase.
    """

    latest_completed_at = _row_timestamp(rows, len(rows) - 1) if rows else None
    metadata: dict[str, Any] = {
        "patternFingerprint": None,
        "patternStartAt": None,
        "patternEndAt": None,
        "patternWidthBars": 0,
        "expiresAfterBars": None,
        "signalAgeBars": 0,
        "latestCompletedBarAt": latest_completed_at,
        "invalidationBuffer": None,
        "lifecycleState": "none",
    }
    if not isinstance(candidate, dict):
        return metadata

    anchors = _structural_lifecycle_anchors(candidate, rows)
    if anchors:
        start_anchor, end_anchor = anchors[0], anchors[-1]
        width = max(1, int(end_anchor["index"]) - int(start_anchor["index"]) + 1)
        expires_after = max(1, int(ceil(width * 0.75)))
        age = max(0, len(rows) - 1 - int(end_anchor["index"]))
        pattern_id = str(candidate.get("patternId") or candidate.get("pattern") or "none").strip() or "none"
        safe_direction = str(direction or "none").strip().lower() or "none"
        safe_timeframe = config.id if config is not None else "none"
        anchor_identity = "|".join(f"{item['kind']}@{item['date']}" for item in anchors)
        metadata.update(
            {
                "patternFingerprint": f"{safe_timeframe}|{safe_direction}|{pattern_id}|{anchor_identity}",
                "patternStartAt": start_anchor["date"],
                "patternEndAt": end_anchor["date"],
                "patternWidthBars": width,
                "expiresAfterBars": expires_after,
                "signalAgeBars": age,
            }
        )

    invalidation_level = _number(candidate.get("invalidation"))
    if invalidation_level is not None:
        metadata["invalidationBuffer"] = _rounded(
            max(abs(invalidation_level) * 0.005, (latest_atr or 0.0) * 0.25)
        )

    override = str(lifecycle_state or "").strip().lower()
    if override in _LIFECYCLE_STATES:
        metadata["lifecycleState"] = override
    elif status == "confirmed":
        metadata["lifecycleState"] = (
            "expired"
            if metadata["expiresAfterBars"] is not None
            and metadata["signalAgeBars"] >= metadata["expiresAfterBars"]
            else "confirmed"
        )
    elif status == "warning":
        metadata["lifecycleState"] = (
            "expired"
            if metadata["expiresAfterBars"] is not None
            and metadata["signalAgeBars"] >= metadata["expiresAfterBars"]
            else "candidate"
        )
    return metadata


def _empty_result(
    indicators: dict[str, Any],
    *,
    config: _TimeframeConfig | None = None,
    row_count: int = 0,
    availability: str = "available",
    rows: list[dict] | None = None,
) -> dict[str, Any]:
    latest_close = _last(indicators.get("close") or [])
    latest_atr = _last(indicators.get("atr14") or [])
    result = {
        "timeframe": config.id if config is not None else None,
        "sourceInterval": config.interval if config is not None else None,
        "barCount": int(row_count),
        "availability": availability,
        "direction": None,
        "candidateStatus": "none",
        "score": 0,
        "pattern": "none",
        "patternId": "none",
        "anchors": [],
        "keyLevel": None,
        "invalidationLevel": None,
        "targetRange": None,
        "evidence": [],
        "metrics": _metrics(indicators),
        "confluence": {"level": "none", "supportingTimeframes": []},
        "keyCrossState": "unavailable",
        "keyCrossEvent": None,
        "confirmation": _confirmation_contract(
            config=config,
            candidate=None,
            direction=None,
            status="none",
            lifecycle_state=None,
            score=0,
            latest_close=latest_close,
            latest_atr=latest_atr,
            key_cross_state="unavailable",
        ),
    }
    result.update(
        _lifecycle_metadata(
            candidate=None,
            rows=rows or [],
            config=config,
            direction=None,
            status="none",
            latest_atr=latest_atr,
        )
    )
    return result


def _local_trading_date_and_slot(row: dict[str, Any]) -> tuple[str, str] | None:
    """Return the provider-rendered local date and clock slot for an intraday bar.

    Providers normalize rows into the market's own timezone before they enter
    this pure evaluator (Yahoo rows include an ISO offset; domestic sources
    carry a local wall-clock string).  Do not convert that wall clock again:
    RVOL must compare 09:30 with prior 09:30 bars in the same market, not with
    another timezone's midday session.
    """

    raw = str(row.get("date") or "").strip()
    if not raw or ("T" not in raw and " " not in raw):
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return parsed.date().isoformat(), parsed.strftime("%H:%M")


def _same_slot_rvol20(rows: list[dict], volumes: list[float | None]) -> list[float | None]:
    """Compute intraday RVOL against *prior* 20 same-clock local sessions.

    The result for a bar is ``volume / mean(prior same slot volumes)``.  Each
    baseline contains one valid observation per preceding local trading date;
    invalid/zero volume never becomes a synthetic baseline.  A row is scored
    only after 20 valid *prior* dates, so the current bar cannot contaminate
    its own reference sample.
    """

    # Provider delivery order is not a market-data contract.  Build the
    # baseline in local chronological order, but write every result back to the
    # matching caller index so all other aligned indicator series stay intact.
    rvol: list[float | None] = [None] * len(rows)
    observations: list[tuple[str, str, int, float]] = []
    for index, row in enumerate(rows):
        identity = _local_trading_date_and_slot(row)
        volume = volumes[index] if index < len(volumes) else None
        if identity is None or volume is None or volume <= 0:
            continue
        trading_day, slot = identity
        observations.append((trading_day, slot, index, volume))

    history_by_slot: dict[str, dict[str, float]] = {}
    for trading_day, slot, index, volume in sorted(observations, key=lambda item: (item[0], item[1], item[2])):
        slot_history = history_by_slot.setdefault(slot, {})
        # A baseline must contain exactly the preceding *distinct* local dates.
        # Explicit date filtering avoids both provider-order dependence and
        # same-day revisions contaminating a bar's own reference set.
        baseline = [
            prior_volume
            for prior_day, prior_volume in sorted(slot_history.items())
            if prior_day < trading_day
        ][-RVOL_HISTORY_DAYS:]
        if len(baseline) == RVOL_HISTORY_DAYS:
            average = sum(baseline) / RVOL_HISTORY_DAYS
            if average > 0:
                rvol[index] = volume / average
        # Providers should not duplicate a slot/date.  If they do, retain the
        # first completed observation rather than allowing intra-bar revisions
        # to alter a prior-day baseline inside one scan.
        slot_history.setdefault(trading_day, volume)
    return rvol


def _indicator_bundle(rows: list[dict], *, interval: str = "1d") -> dict[str, Any]:
    closes = [_number(row.get("close")) for row in rows]
    volumes = [_number(row.get("volume")) for row in rows]
    ma20 = sma(closes, 20)
    intraday = str(interval or "").strip().lower() in {"15m", "60m"}
    return {
        "close": closes,
        "volume": volumes,
        "ma5": sma(closes, 5),
        "ma10": sma(closes, 10),
        "ma20": ma20,
        "ma60": sma(closes, 60),
        "ma20Slope": [
            None
            if index < 5 or ma20[index] is None or ma20[index - 5] is None
            else (ma20[index] - ma20[index - 5]) / 5.0
            for index in range(len(rows))
        ],
        "rsi14": rsi(closes, 14),
        "macd": macd(closes),
        "atr14": atr(rows, 14),
        "volumeMa20": sma(volumes, 20),
        # A daily bar's volume has no intraday seasonal slot, so retain its
        # established MA20 semantics.  Intraday volume is comparable only to
        # the same market-local clock slot across prior trading dates.
        "volumeMode": "same_slot_rvol20" if intraday else "volume_ma20",
        "rvol20SameSlot": _same_slot_rvol20(rows, volumes) if intraday else [None] * len(rows),
    }


def latest_indicator_metrics(*, daily_rows: list[dict], current_price: float | None) -> dict[str, float | None]:
    """Return the latest dashboard metrics without evaluating a signal transition."""

    rows = [dict(row) for row in (daily_rows or [])]
    live_price = _number(current_price)
    if rows and live_price is not None:
        rows[-1]["close"] = live_price
    indicators = _indicator_bundle(rows)

    def latest(name: str) -> float | None:
        values = indicators.get(name)
        if not isinstance(values, list) or not values:
            return None
        return _rounded(_number(values[-1]))

    return {
        "ma20": latest("ma20"),
        "ma60": latest("ma60"),
        "rsi14": latest("rsi14"),
        "atr14": latest("atr14"),
    }


def _candidate_pair(
    *,
    rows: list[dict],
    pivot_indexes: list[int],
    direction: str,
    min_separation: int,
    latest_atr: float | None,
    current_price: float,
) -> dict[str, Any] | None:
    price_field = "high" if direction == "top" else "low"
    neck_field = "low" if direction == "top" else "high"
    tolerance_ratio = max(0.03, 1.5 * (latest_atr or 0.0) / max(abs(current_price), 1e-12))
    depth_ratio = max(0.03, (latest_atr or 0.0) / max(abs(current_price), 1e-12))

    for second_position in range(len(pivot_indexes) - 1, 0, -1):
        second_index = pivot_indexes[second_position]
        second_price = _number(rows[second_index].get(price_field))
        if second_price is None:
            continue
        for first_position in range(second_position - 1, -1, -1):
            first_index = pivot_indexes[first_position]
            if second_index - first_index < min_separation:
                continue
            first_price = _number(rows[first_index].get(price_field))
            if first_price is None:
                continue
            reference_price = (abs(first_price) + abs(second_price)) / 2.0
            if reference_price <= 0:
                continue
            if abs(second_price - first_price) / reference_price > tolerance_ratio:
                continue

            between = rows[first_index + 1 : second_index]
            neck_values = [_number(row.get(neck_field)) for row in between]
            valid_necks = [value for value in neck_values if value is not None]
            if not valid_necks:
                continue
            key_level = min(valid_necks) if direction == "top" else max(valid_necks)
            if direction == "top":
                depth = min(first_price, second_price) - key_level
            else:
                depth = key_level - max(first_price, second_price)
            if depth / reference_price < depth_ratio:
                continue

            latest_close = _number(rows[-1].get("close"))
            if direction == "top":
                second_test_failed = second_price <= first_price or (
                    second_price > first_price and latest_close is not None and latest_close < first_price
                )
                targets = sorted([key_level - depth, key_level - depth * 0.618])
                invalidation = max(first_price, second_price)
            else:
                second_test_failed = second_price >= first_price or (
                    second_price < first_price and latest_close is not None and latest_close > first_price
                )
                targets = sorted([key_level + depth * 0.618, key_level + depth])
                invalidation = min(first_price, second_price)
            first_anchor = {
                "kind": "first_peak" if direction == "top" else "first_trough",
                "index": first_index,
                "date": str(rows[first_index].get("date") or ""),
                "price": _rounded(first_price),
            }
            second_anchor = {
                "kind": "second_peak" if direction == "top" else "second_trough",
                "index": second_index,
                "date": str(rows[second_index].get("date") or ""),
                "price": _rounded(second_price),
            }
            return {
                "pattern": f"double_{direction}",
                "patternId": f"double_{direction}",
                "key": key_level,
                "invalidation": invalidation,
                "target": targets,
                "firstIndex": first_index,
                "secondIndex": second_index,
                "firstPrice": first_price,
                "secondPrice": second_price,
                "depth": depth,
                "double": True,
                "secondTestFailed": second_test_failed,
                "structuralChecks": {
                    "paired_pivots": True,
                    "neckline_depth": True,
                    "repeat_test_failed": second_test_failed,
                },
                "comparisonIndexes": {
                    "kind": "repeat_test",
                    "first": first_index,
                    "second": second_index,
                },
                "anchors": [first_anchor, second_anchor],
                "identityAnchors": [dict(first_anchor), dict(second_anchor)],
            }
    return None


def _shape_tolerance_ratio(latest_atr: float | None, current_price: float) -> float:
    return max(0.03, 1.5 * (latest_atr or 0.0) / max(abs(current_price), 1e-12))


def _shape_depth_ratio(latest_atr: float | None, current_price: float) -> float:
    return max(0.03, (latest_atr or 0.0) / max(abs(current_price), 1e-12))


def _between_level(
    rows: list[dict],
    start_index: int,
    end_index: int,
    *,
    direction: str,
) -> float | None:
    field = "low" if direction == "top" else "high"
    values = [
        _number(row.get(field))
        for row in rows[start_index + 1 : end_index]
    ]
    valid = [value for value in values if value is not None]
    if not valid:
        return None
    return min(valid) if direction == "top" else max(valid)


def _shape_target(key_level: float, depth: float, direction: str) -> list[float]:
    if direction == "top":
        return sorted([key_level - depth, key_level - depth * 0.618])
    return sorted([key_level + depth * 0.618, key_level + depth])


def _anchor(rows: list[dict], index: int, kind: str, price: float) -> dict[str, Any]:
    return {
        "kind": kind,
        "index": index,
        "date": str(rows[index].get("date") or ""),
        "price": _rounded(price),
    }


def _head_shoulders_candidate(
    *,
    rows: list[dict],
    pivot_indexes: list[int],
    direction: str,
    min_separation: int,
    latest_atr: float | None,
    current_price: float,
) -> dict[str, Any] | None:
    """Return the newest valid head-and-shoulders candidate for one direction."""

    price_field = "high" if direction == "top" else "low"
    tolerance = _shape_tolerance_ratio(latest_atr, current_price)
    min_depth = _shape_depth_ratio(latest_atr, current_price)
    for position in range(len(pivot_indexes) - 1, 1, -1):
        left_index, head_index, right_index = pivot_indexes[position - 2 : position + 1]
        if min(head_index - left_index, right_index - head_index) < min_separation:
            continue
        left = _number(rows[left_index].get(price_field))
        head = _number(rows[head_index].get(price_field))
        right = _number(rows[right_index].get(price_field))
        if None in (left, head, right):
            continue
        left, head, right = float(left), float(head), float(right)
        reference = (abs(left) + abs(right)) / 2.0
        if reference <= 0 or abs(left - right) / reference > tolerance:
            continue
        head_is_distinct = (
            head >= max(left, right) * (1.0 + max(0.01, tolerance / 3.0))
            if direction == "top"
            else head <= min(left, right) * (1.0 - max(0.01, tolerance / 3.0))
        )
        if not head_is_distinct:
            continue
        first_neck = _between_level(rows, left_index, head_index, direction=direction)
        second_neck = _between_level(rows, head_index, right_index, direction=direction)
        if first_neck is None or second_neck is None:
            continue
        key_level = (first_neck + second_neck) / 2.0
        depth = head - key_level if direction == "top" else key_level - head
        if depth <= 0 or depth / max(abs(head), 1e-12) < min_depth:
            continue
        anchors = [
            _anchor(rows, left_index, "left_shoulder", left),
            _anchor(rows, head_index, "head", head),
            _anchor(rows, right_index, "right_shoulder", right),
        ]
        return {
            "pattern": f"head_shoulders_{direction}",
            "patternId": f"head_shoulders_{direction}",
            "key": key_level,
            "invalidation": head,
            "target": _shape_target(key_level, depth, direction),
            "firstIndex": left_index,
            "secondIndex": right_index,
            "firstPrice": left,
            "secondPrice": right,
            "depth": depth,
            "double": False,
            "structuralChecks": {
                "shoulder_balance": True,
                "head_distinct": True,
                "neckline_depth": True,
            },
            # Shoulder prices are structural anchors, not interchangeable
            # repeat tests.  Do not use them for generic divergence/volume
            # comparisons unless a future detector explicitly establishes one.
            "comparisonIndexes": {
                "kind": "shoulder_balance",
                "first": left_index,
                "second": right_index,
            },
            "anchors": anchors,
            "identityAnchors": [dict(anchor) for anchor in anchors],
        }
    return None


def _triple_candidate(
    *,
    rows: list[dict],
    pivot_indexes: list[int],
    direction: str,
    min_separation: int,
    latest_atr: float | None,
    current_price: float,
) -> dict[str, Any] | None:
    """Return the newest three-touch resistance/support candidate."""

    price_field = "high" if direction == "top" else "low"
    tolerance = _shape_tolerance_ratio(latest_atr, current_price)
    min_depth = _shape_depth_ratio(latest_atr, current_price)
    for position in range(len(pivot_indexes) - 1, 1, -1):
        first_index, second_index, third_index = pivot_indexes[position - 2 : position + 1]
        if min(second_index - first_index, third_index - second_index) < min_separation:
            continue
        values = [_number(rows[index].get(price_field)) for index in (first_index, second_index, third_index)]
        if any(value is None for value in values):
            continue
        first, second, third = (float(value) for value in values if value is not None)
        reference = (abs(first) + abs(second) + abs(third)) / 3.0
        if reference <= 0 or (max(first, second, third) - min(first, second, third)) / reference > tolerance:
            continue
        necks = [
            _between_level(rows, first_index, second_index, direction=direction),
            _between_level(rows, second_index, third_index, direction=direction),
        ]
        if any(value is None for value in necks):
            continue
        key_level = sum(float(value) for value in necks if value is not None) / len(necks)
        cluster = min(first, second, third) if direction == "top" else max(first, second, third)
        depth = cluster - key_level if direction == "top" else key_level - cluster
        if depth <= 0 or depth / max(abs(cluster), 1e-12) < min_depth:
            continue
        anchors = [
            _anchor(rows, first_index, "first_touch", first),
            _anchor(rows, second_index, "second_touch", second),
            _anchor(rows, third_index, "third_touch", third),
        ]
        return {
            "pattern": f"triple_{direction}",
            "patternId": f"triple_{direction}",
            "key": key_level,
            "invalidation": max(first, second, third) if direction == "top" else min(first, second, third),
            "target": _shape_target(key_level, depth, direction),
            "firstIndex": first_index,
            "secondIndex": third_index,
            "firstPrice": first,
            "secondPrice": third,
            "depth": depth,
            "double": False,
            "secondTestFailed": third <= max(first, second) if direction == "top" else third >= min(first, second),
            "structuralChecks": {
                "three_touches": True,
                "neckline_depth": True,
                "repeat_test_failed": third <= max(first, second) if direction == "top" else third >= min(first, second),
            },
            "comparisonIndexes": {
                "kind": "repeat_test",
                "first": first_index,
                "second": third_index,
            },
            "anchors": anchors,
            "identityAnchors": [dict(anchor) for anchor in anchors],
        }
    return None


def _v_reversal_candidate(
    *,
    rows: list[dict],
    pivot_indexes: list[int],
    direction: str,
    min_separation: int,
    lookback: int,
) -> dict[str, Any] | None:
    """Detect a sharp reversal that has retraced at least 61.8 percent."""

    if not pivot_indexes or len(rows) < min_separation * 2 + 1:
        return None
    start = max(0, len(rows) - lookback)
    extreme_field = "high" if direction == "top" else "low"
    base_field = "low" if direction == "top" else "high"
    latest_close = _number(rows[-1].get("close"))
    if latest_close is None:
        return None

    for extreme_index in reversed(pivot_indexes):
        if extreme_index < start + min_separation or len(rows) - 1 - extreme_index < min_separation:
            continue
        extreme = _number(rows[extreme_index].get(extreme_field))
        prior = [
            (_number(row.get(base_field)), index)
            for index, row in enumerate(rows[start:extreme_index], start=start)
        ]
        valid_prior = [(float(value), index) for value, index in prior if value is not None]
        if extreme is None or not valid_prior:
            continue
        base, base_index = (
            min(valid_prior, key=lambda item: item[0])
            if direction == "top"
            else max(valid_prior, key=lambda item: item[0])
        )
        extreme = float(extreme)
        movement = extreme - base if direction == "top" else base - extreme
        retracement = extreme - latest_close if direction == "top" else latest_close - extreme
        if movement <= 0 or retracement / movement < 0.618:
            continue
        key_level = extreme - movement * 0.618 if direction == "top" else extreme + movement * 0.618
        target = sorted([base, key_level]) if direction == "top" else sorted([key_level, base])
        anchors = [
            _anchor(rows, base_index, "impulse_start", base),
            _anchor(rows, extreme_index, "v_extreme", extreme),
            _anchor(rows, len(rows) - 1, "latest_close", float(latest_close)),
        ]
        return {
            "pattern": f"v_{direction}",
            "patternId": f"v_{direction}",
            "key": key_level,
            "invalidation": extreme,
            "target": target,
            "firstIndex": base_index,
            "secondIndex": extreme_index,
            "firstPrice": base,
            "secondPrice": extreme,
            "depth": movement,
            "double": False,
            "structuralChecks": {"impulse_retracement": True},
            "anchors": anchors,
            "identityAnchors": [dict(anchors[0]), dict(anchors[1])],
        }
    return None


def _linear_slope(points: list[tuple[int, float]]) -> float | None:
    """Return an ordinary-least-squares slope for indexed price points."""

    if len(points) < 2:
        return None
    count = float(len(points))
    mean_x = sum(index for index, _value in points) / count
    mean_y = sum(value for _index, value in points) / count
    denominator = sum((index - mean_x) ** 2 for index, _value in points)
    if denominator <= 0:
        return None
    return sum((index - mean_x) * (value - mean_y) for index, value in points) / denominator


def _pivot_trendline(
    rows: list[dict],
    indexes: list[int],
    field: str,
) -> tuple[int, float, float, int, float] | None:
    """Fit a line through the last two-or-three usable extrema."""

    points = [
        (index, value)
        for index in indexes[-3:]
        if (value := _number(rows[index].get(field))) is not None
    ]
    if len(points) < 2:
        return None
    slope = _linear_slope([(index, float(value)) for index, value in points])
    if slope is None:
        return None
    first_index, first_value = points[0]
    last_index, last_value = points[-1]
    return first_index, float(first_value), slope, last_index, float(last_value)


def _line_at(line: tuple[int, float, float, int, float], index: int) -> float:
    return line[1] + line[2] * (index - line[0])


def _flag_candidate(
    *,
    rows: list[dict],
    direction: str,
    latest_atr: float | None,
    current_price: float,
) -> dict[str, Any] | None:
    """Detect a compact counter-trend flag after a directional flagpole.

    A flag is a continuation setup, so ``bear_flag`` is represented as a top
    (downside) signal and ``bull_flag`` as a bottom (upside) signal.  The scan
    uses only the newest 120 bars and requires the pole, consolidation and
    current bar to be separated in time; it therefore cannot turn a single
    impulse candle into a flag.
    """

    closes = [_number(row.get("close")) for row in rows]
    if len(closes) < 32 or any(value is None for value in closes[-32:]):
        return None
    start = max(0, len(rows) - 120)
    max_tail = min(48, len(rows) - start - 18)
    pole_lengths = (8, 12, 16, 20, 24, 32)
    flag_lengths = (12, 16, 20, 24, 32, 40, 48, 56)
    minimum_move = max(abs(current_price) * 0.04, (latest_atr or 0.0) * 3.0)

    for breakout_tail in range(max_tail, -1, -1):
        flag_end = len(rows) - 1 - breakout_tail
        for flag_length in reversed(flag_lengths):
            flag_start = flag_end - flag_length + 1
            if flag_start - max(pole_lengths) < start:
                continue
            flag_points = [
                (index, float(closes[index]))
                for index in range(flag_start, flag_end + 1)
                if closes[index] is not None
            ]
            if len(flag_points) != flag_length:
                continue
            flag_slope = _linear_slope(flag_points)
            if flag_slope is None:
                continue
            flag_high = max(_number(row.get("high")) or float("-inf") for row in rows[flag_start : flag_end + 1])
            flag_low = min(_number(row.get("low")) or float("inf") for row in rows[flag_start : flag_end + 1])
            if not isfinite(flag_high) or not isfinite(flag_low) or flag_high <= flag_low:
                continue
            for pole_length in reversed(pole_lengths):
                pole_start = flag_start - pole_length
                pole_end = flag_start
                if pole_start < start:
                    continue
                first_price = closes[pole_start]
                pole_price = closes[pole_end]
                flag_finish = closes[flag_end]
                latest_close = closes[-1]
                if None in (first_price, pole_price, flag_finish, latest_close):
                    continue
                first_price = float(first_price)
                pole_price = float(pole_price)
                flag_finish = float(flag_finish)
                latest_close = float(latest_close)
                pole_move = pole_price - first_price
                if abs(pole_move) < minimum_move:
                    continue
                if direction == "top":
                    retracement = flag_high - pole_price
                    qualifies = (
                        pole_move < 0
                        and flag_slope > 0
                        and retracement >= abs(pole_move) * 0.08
                        and retracement <= abs(pole_move) * 0.78
                        and latest_close <= flag_low + max(abs(current_price) * 0.01, (latest_atr or 0.0))
                    )
                    pattern = "bear_flag"
                    key_level = flag_low
                    invalidation = flag_high
                    target = _shape_target(key_level, abs(pole_move), "top")
                else:
                    retracement = pole_price - flag_low
                    qualifies = (
                        pole_move > 0
                        and flag_slope < 0
                        and retracement >= abs(pole_move) * 0.08
                        and retracement <= abs(pole_move) * 0.78
                        and latest_close >= flag_high - max(abs(current_price) * 0.01, (latest_atr or 0.0))
                    )
                    pattern = "bull_flag"
                    key_level = flag_high
                    invalidation = flag_low
                    target = _shape_target(key_level, abs(pole_move), "bottom")
                if not qualifies:
                    continue
                anchors = [
                    _anchor(rows, pole_start, "flagpole_start", first_price),
                    _anchor(rows, pole_end, "flagpole_end", pole_price),
                    _anchor(rows, flag_end, "flag_end", flag_finish),
                    _anchor(rows, len(rows) - 1, "latest_close", latest_close),
                ]
                return {
                    "pattern": pattern,
                    "patternId": pattern,
                    "key": key_level,
                    "invalidation": invalidation,
                    "target": target,
                    "firstIndex": pole_start,
                    "secondIndex": flag_end,
                    "firstPrice": first_price,
                    "secondPrice": flag_finish,
                    "depth": abs(pole_move),
                    "double": False,
                    "structuralChecks": {
                        "flagpole": True,
                        "countertrend_consolidation": True,
                        "boundary_state": True,
                    },
                    "anchors": anchors,
                    # The newest flag endpoint can still be an observation bar.
                    # The immutable pole pivots are therefore the lifecycle ID.
                    "identityAnchors": [dict(anchors[0]), dict(anchors[1])],
                }
    return None


def _wedge_candidate(
    *,
    rows: list[dict],
    pivots: dict[str, list[int]],
    direction: str,
    min_separation: int,
    latest_atr: float | None,
    current_price: float,
) -> dict[str, Any] | None:
    """Detect converging rising/falling wedge boundaries from swing pivots."""

    high_line = _pivot_trendline(rows, pivots.get("highs") or [], "high")
    low_line = _pivot_trendline(rows, pivots.get("lows") or [], "low")
    if high_line is None or low_line is None:
        return None
    start = max(high_line[0], low_line[0])
    end = len(rows) - 1
    if end - start < max(min_separation * 2, 12):
        return None
    upper_start, lower_start = _line_at(high_line, start), _line_at(low_line, start)
    upper_end, lower_end = _line_at(high_line, end), _line_at(low_line, end)
    start_width = upper_start - lower_start
    end_width = upper_end - lower_end
    if start_width <= 0 or end_width <= 0 or end_width > start_width * 0.82:
        return None
    latest_close = _number(rows[-1].get("close"))
    if latest_close is None:
        return None
    buffer = max(abs(current_price) * 0.005, (latest_atr or 0.0) * 0.25)
    high_slope, low_slope = high_line[2], low_line[2]
    if direction == "top":
        qualifies = high_slope > 0 and low_slope > high_slope and float(latest_close) <= lower_end + buffer
        pattern = "rising_wedge"
        key_level, invalidation = lower_end, upper_end
    else:
        qualifies = high_slope < 0 and low_slope < 0 and high_slope < low_slope and float(latest_close) >= upper_end - buffer
        pattern = "falling_wedge"
        key_level, invalidation = upper_end, lower_end
    if not qualifies:
        return None
    depth = start_width
    anchors = [
        _anchor(rows, high_line[3], "upper_boundary_pivot", high_line[4]),
        _anchor(rows, low_line[3], "lower_boundary_pivot", low_line[4]),
        _anchor(rows, len(rows) - 1, "latest_close", float(latest_close)),
    ]
    return {
        "pattern": pattern,
        "patternId": pattern,
        "key": key_level,
        "invalidation": invalidation,
        "target": _shape_target(key_level, depth, direction),
        "firstIndex": start,
        "secondIndex": end,
        "firstPrice": upper_start if direction == "top" else lower_start,
        "secondPrice": upper_end if direction == "top" else lower_end,
        "depth": depth,
        "double": False,
        "structuralChecks": {"converging_boundaries": True, "boundary_state": True},
        "anchors": anchors,
        "identityAnchors": [dict(anchors[0]), dict(anchors[1])],
    }


def _channel_break_candidate(
    *,
    rows: list[dict],
    pivots: dict[str, list[int]],
    direction: str,
    min_separation: int,
    latest_atr: float | None,
    current_price: float,
) -> dict[str, Any] | None:
    """Detect a close outside a parallel swing channel on the 60-minute frame."""

    high_indexes = pivots.get("highs") or []
    low_indexes = pivots.get("lows") or []
    # Two opposing swings describe a triangle just as easily as a channel;
    # require three touches on each boundary before calling it a channel.
    if len(high_indexes) < 3 or len(low_indexes) < 3:
        return None
    high_line = _pivot_trendline(rows, high_indexes, "high")
    low_line = _pivot_trendline(rows, low_indexes, "low")
    if high_line is None or low_line is None:
        return None
    start = max(high_line[0], low_line[0])
    end = len(rows) - 1
    if end - start < max(min_separation * 2, 20):
        return None
    upper_start, lower_start = _line_at(high_line, start), _line_at(low_line, start)
    upper_end, lower_end = _line_at(high_line, end), _line_at(low_line, end)
    start_width = upper_start - lower_start
    end_width = upper_end - lower_end
    if start_width <= 0 or end_width <= 0:
        return None
    width_ratio = end_width / start_width
    slope_scale = max(abs(high_line[2]), abs(low_line[2]), abs(current_price) * 0.0001)
    if not 0.60 <= width_ratio <= 1.40 or abs(high_line[2] - low_line[2]) / slope_scale > 0.55:
        return None
    latest_close = _number(rows[-1].get("close"))
    if latest_close is None:
        return None
    buffer = max(abs(current_price) * 0.005, (latest_atr or 0.0) * 0.25)
    if direction == "top":
        qualifies = float(latest_close) <= lower_end - buffer
        pattern = "bear_channel_break"
        key_level, invalidation = lower_end, upper_end
    else:
        qualifies = float(latest_close) >= upper_end + buffer
        pattern = "bull_channel_break"
        key_level, invalidation = upper_end, lower_end
    if not qualifies:
        return None
    depth = max(start_width, end_width)
    anchors = [
        _anchor(rows, high_line[0], "channel_upper_start", high_line[1]),
        _anchor(rows, high_line[3], "channel_upper_end", high_line[4]),
        _anchor(rows, low_line[0], "channel_lower_start", low_line[1]),
        _anchor(rows, low_line[3], "channel_lower_end", low_line[4]),
        _anchor(rows, len(rows) - 1, "breakout_close", float(latest_close)),
    ]
    return {
        "pattern": pattern,
        "patternId": pattern,
        "key": key_level,
        "invalidation": invalidation,
        "target": _shape_target(key_level, depth, direction),
        "firstIndex": start,
        "secondIndex": end,
        "firstPrice": upper_start if direction == "top" else lower_start,
        "secondPrice": float(latest_close),
        "depth": depth,
        "double": False,
        "structuralChecks": {"parallel_boundaries": True, "close_break": True},
        "anchors": anchors,
        # A close beyond the channel is an event/confirmation input, never a
        # formation identity.  The four confirmed boundary pivots are stable.
        "identityAnchors": [dict(anchor) for anchor in anchors[:4]],
    }


def _rounded_bottom_candidate(
    *,
    rows: list[dict],
    lookback: int,
    latest_atr: float | None,
    current_price: float,
) -> dict[str, Any] | None:
    """Detect a broad daily U-shaped base rather than a two-leg V reversal."""

    if len(rows) < 60:
        return None
    start = max(0, len(rows) - lookback)
    end = len(rows) - 1
    span = end - start
    # Keep the trough near the middle of the base.  A trough in the first/last
    # third is a sharp V leg, which is handled by the dedicated V detector.
    lower_bound = start + max(12, int(span * 0.35))
    upper_bound = end - max(12, int(span * 0.35))
    if lower_bound >= upper_bound:
        return None
    lows = [(_number(rows[index].get("low")), index) for index in range(lower_bound, upper_bound + 1)]
    valid_lows = [(float(value), index) for value, index in lows if value is not None]
    if not valid_lows:
        return None
    trough, trough_index = min(valid_lows, key=lambda item: item[0])

    # Lifecycle identity must be derived from the actual formation, not from
    # ``start``/``end``.  Those are only the rolling detector window and shift
    # as new completed bars arrive.  Choose a pre-trough rim from a fixed
    # trough-relative span, then the first durable recovery crossing of that
    # rim-to-trough range.  Neither identity anchor may be the observation bar.
    identity_left_start = max(0, trough_index - max(20, lookback // 2))
    left_rim_values = [
        (_number(rows[index].get("high")), index)
        for index in range(identity_left_start, trough_index)
    ]
    valid_left_rims = [(float(value), index) for value, index in left_rim_values if value is not None]
    if not valid_left_rims:
        return None
    # Prefer the earliest matching rim, which is deterministic across later
    # observations and represents the beginning of the durable formation.
    left_rim, left_rim_index = max(valid_left_rims, key=lambda item: (item[0], -item[1]))
    formation_depth = left_rim - trough
    if formation_depth <= 0:
        return None
    recovery_level = trough + formation_depth * 0.25
    recovery_index: int | None = None
    recovery_price: float | None = None
    for index in range(trough_index + 1, end):
        value = _number(rows[index].get("close"))
        if value is not None and float(value) >= recovery_level:
            recovery_index, recovery_price = index, float(value)
            break
    if recovery_index is None or recovery_price is None:
        return None

    start_close = _number(rows[start].get("close"))
    latest_close = _number(rows[end].get("close"))
    if start_close is None or latest_close is None:
        return None
    start_close, latest_close = float(start_close), float(latest_close)
    depth = min(start_close, latest_close) - trough
    minimum_depth = max(abs(current_price) * 0.06, (latest_atr or 0.0) * 3.0)
    if depth < minimum_depth:
        return None
    left = [_number(rows[index].get("close")) for index in range(start, trough_index + 1)]
    right = [_number(rows[index].get("close")) for index in range(trough_index, end + 1)]
    left_points = [(index, float(value)) for index, value in enumerate(left, start=start) if value is not None]
    right_points = [(index, float(value)) for index, value in enumerate(right, start=trough_index) if value is not None]
    left_slope, right_slope = _linear_slope(left_points), _linear_slope(right_points)
    if left_slope is None or right_slope is None or left_slope >= 0 or right_slope <= 0:
        return None
    # A rounded base decelerates into its trough and accelerates out of it.
    # Two straight legs (a V) have almost equal outer/inner slopes and belong
    # to the V-reversal detector instead.
    left_width = max(5, (trough_index - start) // 3)
    right_width = max(5, (end - trough_index) // 3)
    left_outer_start = _number(rows[start].get("close"))
    left_outer_end = _number(rows[start + left_width].get("close"))
    left_inner_start = _number(rows[trough_index - left_width].get("close"))
    right_inner_end = _number(rows[trough_index + right_width].get("close"))
    right_outer_start = _number(rows[end - right_width].get("close"))
    right_outer_end = _number(rows[end].get("close"))
    if None in (
        left_outer_start,
        left_outer_end,
        left_inner_start,
        right_inner_end,
        right_outer_start,
        right_outer_end,
    ):
        return None
    left_outer_slope = (float(left_outer_end) - float(left_outer_start)) / left_width
    left_inner_slope = (trough - float(left_inner_start)) / left_width
    right_inner_slope = (float(right_inner_end) - trough) / right_width
    right_outer_slope = (float(right_outer_end) - float(right_outer_start)) / right_width
    if (
        abs(left_inner_slope) >= abs(left_outer_slope) * 0.88
        or abs(right_inner_slope) >= abs(right_outer_slope) * 0.88
    ):
        return None
    # A broad base takes at least a fifth of the inspected range on each side.
    if trough_index - start < span * 0.30 or end - trough_index < span * 0.30:
        return None
    # A U base stays near its trough for a material stretch.  Without this
    # plateau check, two long straight legs are indistinguishable from a V.
    near_floor = recovery_level
    near_start = trough_index
    near_end = trough_index
    while near_start > start:
        value = _number(rows[near_start - 1].get("low"))
        if value is None or value > near_floor:
            break
        near_start -= 1
    while near_end < end:
        value = _number(rows[near_end + 1].get("low"))
        if value is None or value > near_floor:
            break
        near_end += 1
    if near_end - near_start + 1 < max(10, int(span * 0.20)):
        return None
    neckline_values = [_number(rows[index].get("high")) for index in range(start, trough_index + 1)]
    neckline = max(value for value in neckline_values if value is not None) if any(value is not None for value in neckline_values) else None
    if neckline is None:
        return None
    anchors = [
        _anchor(rows, left_rim_index, "rounded_left_rim", left_rim),
        _anchor(rows, trough_index, "rounded_trough", trough),
        _anchor(rows, recovery_index, "rounded_recovery_cross", recovery_price),
    ]
    return {
        "pattern": "rounded_bottom",
        "patternId": "rounded_bottom",
        "key": float(neckline),
        "invalidation": trough,
        "target": _shape_target(float(neckline), depth, "bottom"),
        "firstIndex": left_rim_index,
        "secondIndex": recovery_index,
        "firstPrice": left_rim,
        "secondPrice": recovery_price,
        "depth": depth,
        "double": False,
        "structuralChecks": {"curved_base": True, "broad_floor": True},
        "anchors": anchors,
        "identityAnchors": [dict(anchor) for anchor in anchors],
    }


def _pattern_candidates(
    *,
    rows: list[dict],
    pivots: dict[str, list[int]],
    direction: str,
    config: _TimeframeConfig,
    latest_atr: float | None,
    current_price: float,
) -> list[dict[str, Any]]:
    """Return only configured, price-shape candidates for one direction."""

    candidates: list[dict[str, Any]] = []
    pivot_indexes = pivots["highs"] if direction == "top" else pivots["lows"]
    allowed = set(config.patterns)

    head_shoulders = f"head_shoulders_{direction}"
    if head_shoulders in allowed:
        candidate = _head_shoulders_candidate(
            rows=rows,
            pivot_indexes=pivot_indexes,
            direction=direction,
            min_separation=config.min_separation,
            latest_atr=latest_atr,
            current_price=current_price,
        )
        if candidate is not None:
            candidates.append(candidate)

    triple = f"triple_{direction}"
    if triple in allowed:
        candidate = _triple_candidate(
            rows=rows,
            pivot_indexes=pivot_indexes,
            direction=direction,
            min_separation=config.min_separation,
            latest_atr=latest_atr,
            current_price=current_price,
        )
        if candidate is not None:
            candidates.append(candidate)

    double = f"double_{direction}"
    if double in allowed:
        candidate = _candidate_pair(
            rows=rows,
            pivot_indexes=pivot_indexes,
            direction=direction,
            min_separation=config.min_separation,
            latest_atr=latest_atr,
            current_price=current_price,
        )
        if candidate is not None:
            candidates.append(candidate)

    flag = "bear_flag" if direction == "top" else "bull_flag"
    if flag in allowed:
        candidate = _flag_candidate(
            rows=rows,
            direction=direction,
            latest_atr=latest_atr,
            current_price=current_price,
        )
        if candidate is not None:
            candidates.append(candidate)

    wedge = "rising_wedge" if direction == "top" else "falling_wedge"
    if wedge in allowed:
        candidate = _wedge_candidate(
            rows=rows,
            pivots=pivots,
            direction=direction,
            min_separation=config.min_separation,
            latest_atr=latest_atr,
            current_price=current_price,
        )
        if candidate is not None:
            candidates.append(candidate)

    channel = "bear_channel_break" if direction == "top" else "bull_channel_break"
    if channel in allowed:
        candidate = _channel_break_candidate(
            rows=rows,
            pivots=pivots,
            direction=direction,
            min_separation=config.min_separation,
            latest_atr=latest_atr,
            current_price=current_price,
        )
        if candidate is not None:
            candidates.append(candidate)

    if direction == "bottom" and "rounded_bottom" in allowed:
        candidate = _rounded_bottom_candidate(
            rows=rows,
            lookback=config.lookback,
            latest_atr=latest_atr,
            current_price=current_price,
        )
        if candidate is not None:
            candidates.append(candidate)

    reversal = f"v_{direction}"
    if reversal in allowed:
        candidate = _v_reversal_candidate(
            rows=rows,
            pivot_indexes=pivot_indexes,
            direction=direction,
            min_separation=config.min_separation,
            lookback=config.lookback,
        )
        if candidate is not None:
            candidates.append(candidate)
    return candidates


def _trend_candidate(
    *, rows: list[dict], pivots: dict[str, list[int]], direction: str
) -> dict[str, Any] | None:
    key_indexes = pivots["lows"] if direction == "top" else pivots["highs"]
    invalidation_indexes = pivots["highs"] if direction == "top" else pivots["lows"]
    if not key_indexes:
        return None
    key_index = key_indexes[-1]
    key_field = "low" if direction == "top" else "high"
    invalidation_field = "high" if direction == "top" else "low"
    key_level = _number(rows[key_index].get(key_field))
    if key_level is None:
        return None
    invalidation_index = invalidation_indexes[-1] if invalidation_indexes else None
    invalidation = (
        _number(rows[invalidation_index].get(invalidation_field))
        if invalidation_index is not None
        else None
    )

    comparison_indexes = pivots["highs"] if direction == "top" else pivots["lows"]

    return {
        "pattern": f"trend_{direction}_candidate",
        "key": key_level,
        "invalidation": invalidation,
        "target": None,
        "firstIndex": comparison_indexes[-2] if len(comparison_indexes) >= 2 else None,
        "secondIndex": comparison_indexes[-1] if comparison_indexes else None,
        "firstPrice": None,
        "secondPrice": None,
        "depth": None,
        "double": False,
        "structuralChecks": {"trend_key": True},
    }


def _aligned_relative_series(
    rows: list[dict], benchmark_rows: list[dict] | None, period: int
) -> list[float | None]:
    result: list[float | None] = [None] * len(rows)
    if not benchmark_rows:
        return result
    benchmark_by_date = {
        str(row.get("date")): _number(row.get("close"))
        for row in benchmark_rows
        if row.get("date") is not None
    }
    closes = [_number(row.get("close")) for row in rows]
    benchmark_closes = [benchmark_by_date.get(str(row.get("date"))) for row in rows]
    for index in range(period, len(rows)):
        current = closes[index]
        base = closes[index - period]
        benchmark_current = benchmark_closes[index]
        benchmark_base = benchmark_closes[index - period]
        if None in (current, base, benchmark_current, benchmark_base) or not base or not benchmark_base:
            continue
        own_return = current / base - 1.0
        benchmark_return = benchmark_current / benchmark_base - 1.0
        result[index] = own_return - benchmark_return
    return result


def _same_close_series(rows: list[dict], benchmark_rows: list[dict] | None) -> bool:
    if not benchmark_rows or len(rows) != len(benchmark_rows):
        return False
    return all(
        str(row.get("date")) == str(benchmark.get("date"))
        and _number(row.get("close")) == _number(benchmark.get("close"))
        for row, benchmark in zip(rows, benchmark_rows)
    )


def _supporting_categories(evidence: Iterable[dict[str, Any]]) -> set[str]:
    return {str(item["category"]) for item in evidence if item.get("points", 0) > 0}


def _semantic_comparison(candidate: dict[str, Any] | None, *, length: int) -> tuple[str, int, int] | None:
    """Return a detector-declared, semantically valid anchor comparison only."""

    if not isinstance(candidate, dict):
        return None
    raw = candidate.get("comparisonIndexes")
    if not isinstance(raw, dict):
        return None
    kind = str(raw.get("kind") or "").strip()
    try:
        first_index = int(raw.get("first"))
        second_index = int(raw.get("second"))
    except (TypeError, ValueError):
        return None
    if kind != "repeat_test" or not (0 <= first_index < second_index < length):
        return None
    return kind, first_index, second_index


def _score_direction(
    *,
    rows: list[dict],
    indicators: dict[str, Any],
    candidate: dict[str, Any] | None,
    direction: str,
    relative10: list[float | None],
    relative20: list[float | None],
) -> dict[str, Any]:
    evidence: list[dict[str, Any]] = []
    raw_score = 0
    correlated_points = 0

    def add(category: str, label: str, value: Any, points: int) -> None:
        nonlocal raw_score, correlated_points
        if points <= 0:
            return
        accepted_points = points
        if category in CORRELATED_PRICE_CATEGORIES:
            accepted_points = min(points, max(0, CORRELATED_PRICE_CONFIRMATION_CAP - correlated_points))
            correlated_points += accepted_points
        if accepted_points <= 0:
            return
        raw_score += accepted_points
        evidence.append({"category": category, "label": label, "value": value, "points": accepted_points})

    close = _last(indicators["close"])
    latest_atr = _last(indicators["atr14"])
    comparison = _semantic_comparison(candidate, length=len(indicators["close"]))
    if candidate is not None:
        pattern = str(candidate.get("pattern") or f"shape_{direction}")
        add("structure", f"valid_{pattern}", True, 20)
        structural_checks = candidate.get("structuralChecks")
        repeat_test_failed = (
            bool(structural_checks.get("repeat_test_failed"))
            if isinstance(structural_checks, dict)
            else False
        )
        if comparison is not None and repeat_test_failed:
            add("structure", "second_test_failed", True, 10)
        elif comparison is None and isinstance(structural_checks, dict):
            verified_checks = [
                name
                for name, passed in structural_checks.items()
                if name != "repeat_test_failed" and bool(passed)
            ]
            if verified_checks:
                add("structure", "semantic_pattern_constraints", verified_checks, 10)
        if (
            close is not None
            and latest_atr is not None
            and latest_atr > 0
            and abs(close - candidate["key"]) <= latest_atr
        ):
            add("structure", "within_one_atr_of_key_level", _rounded(abs(close - candidate["key"])), 5)

    ma5 = _last(indicators["ma5"])
    ma10 = _last(indicators["ma10"])
    ma20 = _last(indicators["ma20"])
    ma20_slope = _last(indicators["ma20Slope"])
    if direction == "top":
        if close is not None and ma5 is not None and close < ma5:
            add("trend", "close_below_ma5", _rounded(ma5), 5)
        if ma5 is not None and ma10 is not None and ma5 < ma10:
            add("trend", "ma5_below_ma10", {"ma5": _rounded(ma5), "ma10": _rounded(ma10)}, 5)
        if close is not None and ma20 is not None and close < ma20:
            add("trend", "close_below_ma20", _rounded(ma20), 8)
        if ma20_slope is not None and ma20_slope <= 0:
            add("trend", "ma20_flat_or_falling", _rounded(ma20_slope), 7)
    else:
        if close is not None and ma5 is not None and close > ma5:
            add("trend", "close_above_ma5", _rounded(ma5), 5)
        if ma5 is not None and ma10 is not None and ma5 > ma10:
            add("trend", "ma5_above_ma10", {"ma5": _rounded(ma5), "ma10": _rounded(ma10)}, 5)
        if close is not None and ma20 is not None and close > ma20:
            add("trend", "close_above_ma20", _rounded(ma20), 8)
        if ma20_slope is not None and ma20_slope >= 0:
            add("trend", "ma20_flat_or_rising", _rounded(ma20_slope), 7)

    if candidate is not None:
        macd_line = indicators["macd"]["line"]
        macd_signal = indicators["macd"]["signal"]
        macd_divergent = False
        if comparison is not None:
            _kind, first_index, second_index = comparison
            first_rsi = indicators["rsi14"][first_index]
            second_rsi = indicators["rsi14"][second_index]
            first_price = candidate.get("firstPrice")
            second_price = candidate.get("secondPrice")
            if None not in (first_rsi, second_rsi, first_price, second_price):
                divergent = (
                    second_price >= first_price * 0.99 and second_rsi < first_rsi
                    if direction == "top"
                    else second_price <= first_price * 1.01 and second_rsi > first_rsi
                )
                if divergent:
                    add(
                        "momentum",
                        f"rsi_{direction}_divergence",
                        {"first": _rounded(first_rsi), "second": _rounded(second_rsi)},
                        10,
                    )

            first_macd = macd_line[first_index]
            second_macd = macd_line[second_index]
            if None not in (first_macd, second_macd, first_price, second_price):
                macd_divergent = (
                    second_price >= first_price * 0.99 and second_macd < first_macd
                    if direction == "top"
                    else second_price <= first_price * 1.01 and second_macd > first_macd
                )

        directional_cross = False
        if len(macd_line) >= 2 and None not in (
            macd_line[-2],
            macd_signal[-2],
            macd_line[-1],
            macd_signal[-1],
        ):
            directional_cross = (
                macd_line[-2] > macd_signal[-2] and macd_line[-1] <= macd_signal[-1]
                if direction == "top"
                else macd_line[-2] < macd_signal[-2] and macd_line[-1] >= macd_signal[-1]
            )
        if macd_divergent:
            add("momentum", f"macd_{direction}_divergence", True, 10)
        elif directional_cross:
            add("momentum", "macd_directional_cross", True, 10)

    volume_mode = str(indicators.get("volumeMode") or "volume_ma20")
    if volume_mode == "same_slot_rvol20":
        same_slot_rvol = indicators.get("rvol20SameSlot")
        rvol_series = same_slot_rvol if isinstance(same_slot_rvol, list) else []
        volume_evidence_available = False
        if candidate is not None and comparison is not None:
            _kind, first_index, second_index = comparison
            first_rvol = rvol_series[first_index] if first_index < len(rvol_series) else None
            second_rvol = rvol_series[second_index] if second_index < len(rvol_series) else None
            if (
                first_rvol is not None
                and second_rvol is not None
                and first_rvol > 0
                and second_rvol > 0
            ):
                volume_evidence_available = True
                if second_rvol < first_rvol:
                    add(
                        "volume",
                        "repeat_test_same_slot_rvol_weaker",
                        {
                            "firstRvol20SameSlot": _rounded(first_rvol),
                            "secondRvol20SameSlot": _rounded(second_rvol),
                            "historyTradingDays": RVOL_HISTORY_DAYS,
                        },
                        5,
                    )
        latest_rvol = _last(rvol_series)
        if latest_rvol is not None and latest_rvol > 0:
            volume_evidence_available = True
        crossing_key = candidate is not None and close is not None and (
            close <= candidate["key"] if direction == "top" else close >= candidate["key"]
        )
        if crossing_key and latest_rvol is not None and latest_rvol > 1.0:
            add(
                "volume",
                "key_break_same_slot_rvol_above_20d",
                {
                    "rvol20SameSlot": _rounded(latest_rvol),
                    "historyTradingDays": RVOL_HISTORY_DAYS,
                },
                5,
            )
        if not volume_evidence_available:
            evidence.append(
                {
                    "category": "volume",
                    "label": "volume_same_slot_rvol_unavailable",
                    "value": {"historyTradingDays": RVOL_HISTORY_DAYS},
                    "points": 0,
                }
            )
    else:
        recent_volumes = indicators["volume"][-20:]
        volume_available = (
            len(recent_volumes) == 20
            and all(value is not None and value > 0 for value in recent_volumes)
            and _last(indicators["volumeMa20"]) is not None
        )
        if volume_available and candidate is not None:
            if comparison is not None:
                _kind, first_index, second_index = comparison
                first_volume = indicators["volume"][first_index]
                second_volume = indicators["volume"][second_index]
                if first_volume is not None and second_volume is not None and second_volume < first_volume:
                    add(
                        "volume",
                        "second_test_volume_weaker",
                        {"first": _rounded(first_volume), "second": _rounded(second_volume)},
                        5,
                    )
            volume_ma20 = _last(indicators["volumeMa20"])
            latest_volume = _last(indicators["volume"])
            crossing_key = close is not None and (
                close <= candidate["key"] if direction == "top" else close >= candidate["key"]
            )
            if crossing_key and latest_volume is not None and volume_ma20 is not None and latest_volume > volume_ma20:
                add(
                    "volume",
                    "key_break_volume_above_ma20",
                    {"volume": _rounded(latest_volume), "ma20": _rounded(volume_ma20)},
                    5,
                )
        elif not volume_available:
            evidence.append({"category": "volume", "label": "volume_unavailable", "value": None, "points": 0})

    relative10_now = _last(relative10)
    relative10_prior = relative10[-6] if len(relative10) >= 6 else None
    relative20_now = _last(relative20)
    relative20_prior = relative20[-6] if len(relative20) >= 6 else None
    if relative10_now is not None and relative10_prior is not None:
        improved = relative10_now > relative10_prior
        if (direction == "bottom" and improved) or (direction == "top" and not improved and relative10_now < relative10_prior):
            add(
                "relative_strength",
                f"relative_10d_{'improving' if direction == 'bottom' else 'deteriorating'}",
                _rounded(relative10_now * 100.0),
                5,
            )
    if relative20_now is not None and relative20_prior is not None:
        qualifies = (
            relative20_now > 0 and relative20_now > relative20_prior
            if direction == "bottom"
            else relative20_now < 0 and relative20_now < relative20_prior
        )
        if qualifies:
            add(
                "relative_strength",
                f"relative_20d_{'positive_improving' if direction == 'bottom' else 'negative_deteriorating'}",
                _rounded(relative20_now * 100.0),
                5,
            )

    score = min(SCORE_MAXIMUM, int(raw_score))
    public_evidence = [
        {"category": item["category"], "label": item["label"], "value": item["value"]}
        for item in evidence
    ]
    return {
        "score": score,
        "evidence": public_evidence,
        "supportingCategories": _supporting_categories(evidence),
        "candidate": candidate,
    }


def _public_result(
    *,
    direction: str,
    status: str,
    scored: dict[str, Any],
    indicators: dict[str, Any],
    config: _TimeframeConfig,
    row_count: int,
    rows: list[dict],
    lifecycle_state: str | None = None,
) -> dict[str, Any]:
    candidate = scored["candidate"]
    latest_close = _last(indicators["close"])
    latest_atr = _last(indicators["atr14"])
    cross = _key_cross_details(
        candidate=candidate,
        direction=direction,
        close=latest_close,
        previous_close=indicators["close"][-2] if len(indicators["close"]) >= 2 else None,
        latest_atr=latest_atr,
    )
    result = {
        "timeframe": config.id,
        "sourceInterval": config.interval,
        "barCount": int(row_count),
        "availability": "available",
        "direction": direction,
        "candidateStatus": status,
        "score": scored["score"],
        "pattern": candidate["pattern"],
        "patternId": candidate.get("patternId") or candidate["pattern"],
        "anchors": [dict(anchor) for anchor in candidate.get("anchors") or []],
        "keyLevel": _rounded(candidate["key"]),
        "invalidationLevel": _rounded(candidate["invalidation"]),
        "targetRange": (
            [_rounded(value) for value in candidate["target"]]
            if candidate["target"] is not None
            else None
        ),
        "evidence": scored["evidence"],
        "metrics": _metrics(indicators),
        "confluence": {"level": "none", "supportingTimeframes": []},
        "keyCrossState": cross["state"],
        "keyCrossEvent": cross["event"],
        "confirmation": _confirmation_contract(
            config=config,
            candidate=candidate,
            direction=direction,
            status=status,
            lifecycle_state=lifecycle_state,
            score=scored["score"],
            latest_close=latest_close,
            latest_atr=latest_atr,
            key_cross_state=cross["state"],
        ),
    }
    result.update(
        _lifecycle_metadata(
            candidate=candidate,
            rows=rows,
            config=config,
            direction=direction,
            status=status,
            latest_atr=_last(indicators["atr14"]),
            lifecycle_state=lifecycle_state,
        )
    )
    return result


def _is_close_phase(scan_phase: str) -> bool:
    phase = str(scan_phase or "").strip().lower()
    return phase == "close" or phase.endswith("_close") or phase.endswith("close")


def _key_cross_details(
    *,
    candidate: dict[str, Any] | None,
    direction: str,
    close: float | None,
    previous_close: float | None,
    latest_atr: float | None,
) -> dict[str, str | None]:
    """Describe the current key-side state and the one-bar entry event.

    ``key_side`` is below the key for a top candidate and above it for a bottom
    candidate.  Confirmation deliberately consumes this state rather than the
    transient event, so a close that crossed on the prior completed bar remains
    confirmable on a later completed bar.
    """

    key_level = _number(candidate.get("key")) if isinstance(candidate, dict) else None
    if close is None or key_level is None or direction not in {"top", "bottom"}:
        return {"state": "unavailable", "event": None}
    buffer = max(abs(key_level) * 0.005, (latest_atr or 0.0) * 0.25)

    def state_for(value: float | None) -> str:
        if value is None:
            return "unavailable"
        if direction == "top":
            if value <= key_level - buffer:
                return "key_side"
            if value >= key_level + buffer:
                return "opposite_side"
        else:
            if value >= key_level + buffer:
                return "key_side"
            if value <= key_level - buffer:
                return "opposite_side"
        return "within_buffer"

    state = state_for(close)
    previous_state = state_for(previous_close)
    return {
        "state": state,
        "event": "entered_key_side" if state == "key_side" and previous_state != "key_side" else None,
    }


def _confirmation_contract(
    *,
    config: _TimeframeConfig | None,
    candidate: dict[str, Any] | None,
    direction: str | None,
    status: str,
    lifecycle_state: str | None,
    score: int | float,
    latest_close: float | None,
    latest_atr: float | None,
    key_cross_state: str,
) -> dict[str, Any]:
    """Publish the exact independent-confirmation gate for one frame.

    Only the daily frame owns an independent price/score confirmation gate.
    Intraday frames still expose their observed key-side state for diagnostics,
    while their null trigger fields prevent consumers from treating a 15m/60m
    score or key cross as a standalone confirmation.
    """

    policy = config.confirmation_policy if config is not None else None
    close = _number(latest_close)
    key_level = _number(candidate.get("key")) if isinstance(candidate, dict) else None
    independent = policy == "market_close_only"
    contract: dict[str, Any] = {
        "policy": policy,
        "scoreThreshold": config.confirm if independent and config is not None else None,
        "scorePassed": bool(score >= config.confirm) if independent and config is not None else None,
        "latestClose": _rounded(close),
        "keyLevel": _rounded(key_level),
        "buffer": None,
        "operator": None,
        "triggerLevel": None,
        "remainingPoints": None,
        "keyCrossState": key_cross_state,
        "confirmed": str(status or "").strip().lower() == "confirmed",
        "reasonCode": "no_active_signal",
    }

    if not isinstance(candidate, dict) or direction not in {"top", "bottom"} or key_level is None:
        return contract
    if not independent:
        contract["reasonCode"] = policy or "independent_confirmation_not_allowed"
        return contract

    buffer = max(abs(key_level) * 0.005, (latest_atr or 0.0) * 0.25)
    operator = "<=" if direction == "top" else ">="
    trigger_level = key_level - buffer if direction == "top" else key_level + buffer
    remaining = None
    if close is not None:
        remaining = (
            max(0.0, close - trigger_level)
            if direction == "top"
            else max(0.0, trigger_level - close)
        )
    contract.update(
        {
            "buffer": _rounded(buffer),
            "operator": operator,
            "triggerLevel": _rounded(trigger_level),
            "remainingPoints": _rounded(remaining),
        }
    )

    effective_lifecycle = str(lifecycle_state or "").strip().lower()
    if effective_lifecycle in {"invalidated", "expired"}:
        contract["reasonCode"] = f"signal_{effective_lifecycle}"
    elif contract["confirmed"]:
        contract["reasonCode"] = "confirmed"
    elif not contract["scorePassed"]:
        contract["reasonCode"] = "score_below_threshold"
    elif key_cross_state != "key_side":
        contract["reasonCode"] = "awaiting_key_level_break"
    else:
        contract["reasonCode"] = "awaiting_market_close"
    return contract


def confirmation_contract_for_state(
    *,
    timeframe: str,
    direction: str | None,
    status: str,
    lifecycle_state: str | None,
    score: int | float | None,
    key_level: float | None,
    latest_close: float | None,
    latest_atr: float | None,
) -> dict[str, Any]:
    """Build the public confirmation contract for a persisted signal state.

    Callers must supply the latest *completed-bar* close.  The helper does not
    consult a quote or infer finality, so it cannot accidentally substitute an
    intraday market price for the daily confirmation close.
    """

    normalized_timeframe = str(timeframe or "").strip().lower()
    if normalized_timeframe not in _TIMEFRAMES:
        raise ValueError(f"unsupported timeframe: {timeframe}")
    normalized_direction = str(direction or "").strip().lower()
    safe_direction = normalized_direction if normalized_direction in {"top", "bottom"} else None
    numeric_key = _number(key_level)
    numeric_close = _number(latest_close)
    numeric_atr = _number(latest_atr)
    numeric_score = _number(score)
    candidate = {"key": numeric_key}
    cross = _key_cross_details(
        candidate=candidate,
        direction=safe_direction or "",
        close=numeric_close,
        previous_close=None,
        latest_atr=numeric_atr,
    )
    return _confirmation_contract(
        config=_TIMEFRAMES[normalized_timeframe],
        candidate=candidate,
        direction=safe_direction,
        status=str(status or "none").strip().lower(),
        lifecycle_state=str(lifecycle_state or "").strip().lower() or None,
        score=numeric_score if numeric_score is not None else 0,
        latest_close=numeric_close,
        latest_atr=numeric_atr,
        key_cross_state=str(cross["state"]),
    )


def _pattern_priority(candidate: dict[str, Any]) -> int:
    pattern = str(candidate.get("pattern") or "")
    if pattern.startswith("head_shoulders_"):
        return 70
    if pattern == "rounded_bottom":
        return 65
    if pattern.endswith("_channel_break"):
        return 60
    if pattern.endswith("_flag"):
        return 55
    if pattern.endswith("_wedge"):
        return 50
    if pattern.startswith("triple_"):
        return 45
    if pattern.startswith("double_"):
        return 40
    if pattern.startswith("v_"):
        return 30
    return 0


def _best_scored_candidate(
    *,
    rows: list[dict],
    indicators: dict[str, Any],
    candidates: list[dict[str, Any]],
    direction: str,
    relative10: list[float | None],
    relative20: list[float | None],
) -> dict[str, Any]:
    if not candidates:
        return _score_direction(
            rows=rows,
            indicators=indicators,
            candidate=None,
            direction=direction,
            relative10=relative10,
            relative20=relative20,
        )
    scored = [
        _score_direction(
            rows=rows,
            indicators=indicators,
            candidate=candidate,
            direction=direction,
            relative10=relative10,
            relative20=relative20,
        )
        for candidate in candidates
    ]
    return max(
        scored,
        key=lambda item: (
            _pattern_priority(item.get("candidate") or {}),
            int(item.get("score") or 0),
            int((item.get("candidate") or {}).get("secondIndex") or -1),
        ),
    )


def _evaluate_timeframe(
    *,
    rows: list[dict],
    indicators: dict[str, Any],
    benchmark_rows: list[dict] | None,
    benchmark_is_self: bool,
    scan_phase: str,
    config: _TimeframeConfig,
    availability: str = "available",
) -> dict[str, Any]:
    if len(rows) < TIMEFRAME_REQUIRED_BARS[config.id]:
        return _empty_result(
            indicators,
            config=config,
            row_count=len(rows),
            availability=availability,
            rows=rows,
        )

    window_start = max(0, len(rows) - config.lookback)
    window_rows = rows[window_start:]
    local_pivots = find_pivots(window_rows, config.pivot_side)
    pivots = {
        key: [window_start + index for index in indexes]
        for key, indexes in local_pivots.items()
    }
    latest_close = _last(indicators["close"])
    latest_atr = _last(indicators["atr14"])
    if latest_close is None:
        return _empty_result(
            indicators,
            config=config,
            row_count=len(rows),
            availability=availability,
            rows=rows,
        )

    if benchmark_is_self:
        relative10 = [0.0 if index >= 10 else None for index in range(len(rows))]
        relative20 = [0.0 if index >= 20 else None for index in range(len(rows))]
    else:
        relative10 = _aligned_relative_series(rows, benchmark_rows, 10)
        relative20 = _aligned_relative_series(rows, benchmark_rows, 20)

    top = _best_scored_candidate(
        rows=rows,
        indicators=indicators,
        candidates=_pattern_candidates(
            rows=rows,
            pivots=pivots,
            direction="top",
            config=config,
            latest_atr=latest_atr,
            current_price=float(latest_close),
        ),
        direction="top",
        relative10=relative10,
        relative20=relative20,
    )
    bottom = _best_scored_candidate(
        rows=rows,
        indicators=indicators,
        candidates=_pattern_candidates(
            rows=rows,
            pivots=pivots,
            direction="bottom",
            config=config,
            latest_atr=latest_atr,
            current_price=float(latest_close),
        ),
        direction="bottom",
        relative10=relative10,
        relative20=relative20,
    )

    top_candidate = top["candidate"]
    bottom_candidate = bottom["candidate"]

    def has_independent_confirmation(scored: dict[str, Any]) -> bool:
        categories = set(scored.get("supportingCategories") or set())
        return "structure" in categories and bool(categories & CONFIRMATION_CATEGORIES)

    top_warning = (
        top_candidate is not None
        and top["score"] >= config.warning
        and has_independent_confirmation(top)
    )
    bottom_warning = (
        bottom_candidate is not None
        and bottom["score"] >= config.warning
        and has_independent_confirmation(bottom)
    )
    close_phase = _is_close_phase(scan_phase) and config.confirmation_policy == "market_close_only"

    def invalidation_cross(scored: dict[str, Any], direction: str) -> bool:
        candidate = scored["candidate"]
        if candidate is None or candidate["invalidation"] is None:
            return False
        invalidation_level = candidate["invalidation"]
        buffer = max(abs(invalidation_level) * 0.005, (latest_atr or 0.0) * 0.25)
        crossed = (
            latest_close >= invalidation_level + buffer
            if direction == "top"
            else latest_close <= invalidation_level - buffer
        )
        if crossed:
            scored["evidence"].append(
                {
                    "category": "structure",
                    "label": "invalidation_break",
                    "value": {
                        "close": _rounded(latest_close),
                        "level": _rounded(invalidation_level),
                        "buffer": _rounded(buffer),
                    },
                }
            )
        return crossed

    top_invalidation = invalidation_cross(top, "top")
    bottom_invalidation = invalidation_cross(bottom, "bottom")

    def hard_confirmation(scored: dict[str, Any], direction: str) -> bool:
        candidate = scored["candidate"]
        if (
            not close_phase
            or candidate is None
            or scored["score"] < config.confirm
            or not has_independent_confirmation(scored)
        ):
            return False
        cross = _key_cross_details(
            candidate=candidate,
            direction=direction,
            close=latest_close,
            previous_close=indicators["close"][-2] if len(indicators["close"]) >= 2 else None,
            latest_atr=latest_atr,
        )
        return cross["state"] == "key_side"

    top_confirmed = hard_confirmation(top, "top")
    bottom_confirmed = hard_confirmation(bottom, "bottom")
    if top_confirmed and not bottom_confirmed:
        return _public_result(
            direction="top",
            status="confirmed",
            scored=top,
            indicators=indicators,
            config=config,
            row_count=len(rows),
            rows=rows,
        )
    if bottom_confirmed and not top_confirmed:
        return _public_result(
            direction="bottom",
            status="confirmed",
            scored=bottom,
            indicators=indicators,
            config=config,
            row_count=len(rows),
            rows=rows,
        )
    if top_confirmed and bottom_confirmed:
        selected_direction, selected = ("top", top) if top["score"] >= bottom["score"] else ("bottom", bottom)
        return _public_result(
            direction=selected_direction,
            status="confirmed",
            scored=selected,
            indicators=indicators,
            config=config,
            row_count=len(rows),
            rows=rows,
        )

    # All evaluator rows are completed provider bars.  The daily policy limits
    # confirmation to a close, but a completed 15m/60m bar may still prove an
    # existing candidate invalid and must transition it immediately.
    # An invalidation is a terminal lifecycle observation, not a fresh-entry
    # gate.  The score can naturally weaken once price has crossed the
    # invalidation level; requiring it to remain a warning would leave an
    # already-active 15m/60m candidate alive.  The persistence layer only
    # turns this observation into a state transition when that direction was
    # active, so a newly discovered weak shape cannot create a false alert.
    # Do not let a weak, invalidated opposite shape hide a stronger fresh
    # warning (for example, a bull flag beside a stale double-top fragment).
    # The score comparison only resolves the one-result-per-frame display;
    # when no stronger opposite warning exists, report the invalidation even if
    # the invalidated candidate itself has fallen below entry threshold.
    top_is_outweighed = bottom_warning and bottom["score"] > top["score"]
    bottom_is_outweighed = top_warning and top["score"] > bottom["score"]
    if top_invalidation and not top_is_outweighed and not bottom_confirmed:
        return _public_result(
            direction="top",
            status="none",
            scored=top,
            indicators=indicators,
            config=config,
            row_count=len(rows),
            rows=rows,
            lifecycle_state="invalidated",
        )
    if bottom_invalidation and not bottom_is_outweighed and not top_confirmed:
        return _public_result(
            direction="bottom",
            status="none",
            scored=bottom,
            indicators=indicators,
            config=config,
            row_count=len(rows),
            rows=rows,
            lifecycle_state="invalidated",
        )

    if top_warning and bottom_warning:
        difference = abs(top["score"] - bottom["score"])
        if difference < 10:
            combined_evidence = []
            for direction, scored in (("top", top), ("bottom", bottom)):
                combined_evidence.extend(
                    {
                        "category": item["category"],
                        "label": f"{direction}:{item['label']}",
                        "value": item["value"],
                    }
                    for item in scored["evidence"]
                )
            conflict = _empty_result(
                indicators,
                config=config,
                row_count=len(rows),
                availability=availability,
                rows=rows,
            )
            conflict.update(
                {
                    "candidateStatus": "conflict",
                    "score": max(top["score"], bottom["score"]),
                    "pattern": "range_conflict",
                    "patternId": "range_conflict",
                    "evidence": combined_evidence,
                }
            )
            return conflict
        selected_direction, selected = ("top", top) if top["score"] > bottom["score"] else ("bottom", bottom)
        return _public_result(
            direction=selected_direction,
            status="warning",
            scored=selected,
            indicators=indicators,
            config=config,
            row_count=len(rows),
            rows=rows,
        )
    if top_warning:
        return _public_result(
            direction="top",
            status="warning",
            scored=top,
            indicators=indicators,
            config=config,
            row_count=len(rows),
            rows=rows,
        )
    if bottom_warning:
        return _public_result(
            direction="bottom",
            status="warning",
            scored=bottom,
            indicators=indicators,
            config=config,
            row_count=len(rows),
            rows=rows,
        )
    return _empty_result(
        indicators,
        config=config,
        row_count=len(rows),
        availability=availability,
        rows=rows,
    )


def apply_multi_timeframe_confluence(
    results: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Apply only adjacent-frame promotion to independent observations.

    The sequence is deliberately directional: 15m can become resonant only
    with same-direction 60m evidence, and 60m only with same-direction daily
    evidence.  A matching 15m + daily pair is useful context but skips its
    required middle frame, so it is explicitly marked non-adjacent rather than
    promoted.  Opposite active frames block promotion but never overwrite the
    daily close-confirmed status itself.
    """

    copied = {
        timeframe: {
            **dict(result),
            # The function is safe to run repeatedly: replace its own prior
            # annotations rather than accumulating duplicate evidence rows.
            "evidence": [
                dict(item)
                for item in result.get("evidence") or []
                if isinstance(item, dict) and item.get("category") != "multi_timeframe"
            ],
        }
        for timeframe, result in (results or {}).items()
        if isinstance(result, dict)
    }

    def confluence(
        level: str,
        *,
        supporting: list[str] | None = None,
        conflicts: list[str] | None = None,
        promotion_blocked: bool = False,
    ) -> dict[str, Any]:
        return {
            "level": level,
            "supportingTimeframes": list(supporting or []),
            "conflictWith": list(conflicts or []),
            "promotionBlocked": bool(promotion_blocked),
        }

    def inferred_lifecycle(result: dict[str, Any]) -> str:
        lifecycle = str(result.get("lifecycleState") or "").strip().lower()
        if lifecycle in _LIFECYCLE_STATES:
            return lifecycle
        status = str(result.get("candidateStatus") or "none").strip().lower()
        if status == "confirmed":
            return "confirmed"
        if status == "warning":
            return "candidate"
        return "none"

    def is_active(result: dict[str, Any]) -> bool:
        direction = str(result.get("direction") or "").strip().lower()
        status = str(result.get("candidateStatus") or "none").strip().lower()
        return (
            direction in {"top", "bottom"}
            and status in {"warning", "confirmed"}
            and inferred_lifecycle(result) in {"candidate", "resonant", "confirmed"}
        )

    for timeframe in TIMEFRAME_IDS:
        copied.setdefault(
            timeframe,
            {
                "timeframe": timeframe,
                "candidateStatus": "none",
                "direction": None,
                "evidence": [],
                "lifecycleState": "none",
                "confluence": confluence("none"),
            },
        )

    for timeframe in TIMEFRAME_IDS:
        result = copied[timeframe]
        lifecycle = inferred_lifecycle(result)
        result["lifecycleState"] = lifecycle
        direction = str(result.get("direction") or "").strip().lower()
        status = str(result.get("candidateStatus") or "none").strip().lower()
        if not is_active(result):
            result["confluence"] = confluence("none")
            continue

        # Resonance is derived from this complete snapshot, never carried
        # forward from an earlier one.  A vanished/expired required neighbour
        # therefore returns an intraday warning to its base candidate state.
        if lifecycle == "resonant":
            lifecycle = "candidate"
            result["lifecycleState"] = lifecycle

        same_direction = [
            item
            for item in TIMEFRAME_IDS
            if item != timeframe
            and is_active(copied[item])
            and str(copied[item].get("direction") or "").strip().lower() == direction
        ]
        opposite_direction = [
            item
            for item in TIMEFRAME_IDS
            if item != timeframe
            and is_active(copied[item])
            and str(copied[item].get("direction") or "").strip().lower() != direction
        ]

        if opposite_direction:
            # A close-confirmed daily signal remains confirmed; all other
            # frames simply remain candidates rather than becoming resonant.
            if lifecycle == "resonant":
                result["lifecycleState"] = "candidate"
            result["confluence"] = confluence(
                "cross_timeframe_conflict",
                supporting=same_direction,
                conflicts=opposite_direction,
                promotion_blocked=True,
            )
            result["evidence"].append(
                {
                    "category": "multi_timeframe",
                    "label": "cross_timeframe_conflict",
                    "value": {
                        "direction": direction,
                        "sameDirectionTimeframes": same_direction,
                        "conflictWith": opposite_direction,
                        "promotionBlocked": True,
                    },
                }
            )
            continue

        if timeframe == "short":
            if "medium" in same_direction:
                result["lifecycleState"] = "resonant"
                result["confluence"] = confluence("resonant", supporting=["medium"])
            elif "long" in same_direction:
                result["confluence"] = confluence("nonadjacent_alignment_only", supporting=["long"])
            else:
                result["confluence"] = confluence("candidate_only")
        elif timeframe == "medium":
            if "long" in same_direction:
                result["lifecycleState"] = "resonant"
                result["confluence"] = confluence("resonant", supporting=["long"])
            else:
                result["confluence"] = confluence("candidate_only")
        else:
            # Daily is the terminal quality gate.  It can supply confirmation
            # to 60m but must not itself be promoted by lower-frame agreement.
            result["confluence"] = confluence(
                "daily_confirmed" if status == "confirmed" else "daily_candidate"
            )

        level = result["confluence"]["level"]
        if level in {"resonant", "nonadjacent_alignment_only"}:
            result["evidence"].append(
                {
                    "category": "multi_timeframe",
                    "label": level,
                    "value": {
                        "direction": direction,
                        "timeframes": result["confluence"]["supportingTimeframes"],
                        "promotionBlocked": False,
                    },
                }
            )
    return copied


def evaluate_index_signals(
    *,
    spec: Any,
    daily_rows: list[dict],
    current_price: float | None,
    benchmark_rows: list[dict] | None,
    scan_phase: str,
    timeframe_rows: dict[str, list[dict]] | None = None,
    benchmark_timeframe_rows: dict[str, list[dict]] | None = None,
) -> dict[str, dict[str, Any]]:
    """Evaluate real 15m, 60m, and daily price-shape candidates.

    ``timeframe_rows`` must hold independently fetched bars for ``short``
    (15m), ``medium`` (60m), and ``long`` (daily).  The daily-only fallback is
    retained for callers upgrading from the previous API, but production scans
    always pass the three independent series.
    """

    del spec, current_price  # State is calculated from completed provider bars only.
    fallback_rows = [dict(row) for row in (daily_rows or [])]
    fallback_benchmark = [dict(row) for row in benchmark_rows] if benchmark_rows else None
    supplied_frames = timeframe_rows is not None
    result: dict[str, dict[str, Any]] = {}

    for timeframe, config in _TIMEFRAMES.items():
        supplied_rows = timeframe_rows.get(timeframe) if isinstance(timeframe_rows, dict) else None
        # Once a caller supplies a frame map, a missing frame means the source
        # is unavailable.  Never turn it into a mislabeled daily-bar alias.
        rows = (
            [dict(row) for row in supplied_rows]
            if isinstance(supplied_rows, list)
            else []
            if supplied_frames
            else [dict(row) for row in fallback_rows]
        )
        supplied_benchmark = (
            benchmark_timeframe_rows.get(timeframe)
            if isinstance(benchmark_timeframe_rows, dict)
            else None
        )
        frame_benchmark = (
            [dict(row) for row in supplied_benchmark]
            if isinstance(supplied_benchmark, list)
            else None
            if isinstance(benchmark_timeframe_rows, dict)
            else [dict(row) for row in fallback_benchmark] if fallback_benchmark else None
        )
        # The market-data bundle can retain a display-only current daily row
        # while its provider close is pending.  Respect its internal boundary
        # even if an older caller still supplies display ``rows`` instead of
        # the dedicated completed ``signalRows`` series.
        if timeframe == "long":
            rows = [row for row in rows if row.get("_signalEligible") is not False]
            if frame_benchmark is not None:
                frame_benchmark = [
                    row for row in frame_benchmark if row.get("_signalEligible") is not False
                ]
        # Determine self-benchmark identity from completed provider bars.
        benchmark_is_self = _same_close_series(rows, frame_benchmark)
        indicators = _indicator_bundle(rows, interval=config.interval)
        availability = (
            "available"
            if len(rows) >= TIMEFRAME_REQUIRED_BARS[timeframe]
            else "insufficient"
            if rows
            else "unavailable"
            if supplied_frames
            else "legacy_daily_fallback"
        )
        result[timeframe] = _evaluate_timeframe(
            rows=rows,
            indicators=indicators,
            benchmark_rows=frame_benchmark,
            benchmark_is_self=benchmark_is_self,
            scan_phase=scan_phase,
            config=config,
            availability=availability,
        )
    return apply_multi_timeframe_confluence(result)
