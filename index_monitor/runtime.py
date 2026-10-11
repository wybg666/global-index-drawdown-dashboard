"""Standalone extraction of the original index-only workflow."""
from __future__ import annotations
from .paths import DATA_DIR
from .notifier import validate_index_webhook, test_index_signal_dingtalk_webhook
import copy
import json
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List
from zoneinfo import ZoneInfo
from .indices import CN_INDEXES, HK_INDEXES, JP_KR_INDEXES, US_INDEXES, build_index_market_bundle, get_index_signal_timeframes
from .index_signal_store import MAX_EVENTS, IndexSignalStore
from .index_signal_daily_push import IndexSignalDailyPushStore, build_index_signal_judgment_snapshot, channel_hash as index_signal_channel_hash, close_snapshot_ready, plan_index_signal_judgment_delivery
from .index_signals import TIMEFRAME_IDS, evaluate_index_signals, index_signal_rules
from .notifier import notify_index_signal_judgment, notify_index_signal_transition
from .trading_calendar import is_cn_trading_day

ROOT = DATA_DIR


AUTOMATION_STATE = ROOT / "automation_state.json"


INDEX_SIGNAL_NOTIFICATION_SETTINGS = ROOT / "index_signal_notification_settings.json"


INDEX_SIGNAL_STATE = ROOT / "index_signal_state.json"


INDEX_SIGNAL_DAILY_PUSH_STATE = ROOT / "index_signal_daily_push_state.json"


INDEX_SIGNAL_WEBHOOK_MASK = "********"


INDEX_SIGNAL_NOTIFICATION_DEFAULTS = {
    "indexSignalDingTalkEnabled": False,
    "indexSignalDingTalkWebhook": "",
    "shortSignalsEnabled": True,
    "mediumSignalsEnabled": True,
    "longSignalsEnabled": True,
    "warningEnabled": True,
    "confirmedEnabled": True,
    "invalidatedEnabled": True,
}


INDEX_SIGNAL_MARKET_TIMEZONES = {
    "CN": "Asia/Shanghai",
    "HK": "Asia/Hong_Kong",
    "JP_KR": "Asia/Seoul",
    "US": "America/New_York",
}


INDEX_SIGNAL_PHASES = {
    "CN": {"cn_morning", "cn_midday", "cn_tail", "cn_close"},
    "HK": {"hk_morning", "hk_midday", "hk_close"},
    "JP_KR": {"jp_kr_morning", "jp_kr_midday", "jp_kr_close"},
    "US": {"us_close", "us_retry"},
}


INDEX_SIGNAL_TASK_KEYS = {
    ("CN", "cn_morning"): "indexSignalCNMorning",
    ("CN", "cn_midday"): "indexSignalCNMidday",
    ("CN", "cn_tail"): "indexSignalCNTail",
    ("CN", "cn_close"): "indexSignalCNClose",
    ("HK", "hk_morning"): "indexSignalHKMorning",
    ("HK", "hk_midday"): "indexSignalHKMidday",
    ("HK", "hk_close"): "indexSignalHKClose",
    ("JP_KR", "jp_kr_morning"): "indexSignalJPKRMorning",
    ("JP_KR", "jp_kr_midday"): "indexSignalJPKRMidday",
    ("JP_KR", "jp_kr_close"): "indexSignalJPKRClose",
    ("US", "us_close"): "indexSignalUSClose",
    ("US", "us_retry"): "indexSignalUSRetry",
}


INDEX_SIGNAL_BENCHMARK_KEYS = {"CN": "sh000300", "HK": "hkHSI", "JP_KR": "nikkei225", "US": "spy"}


INDEX_SIGNAL_LOCAL_TIMES = {
    ("CN", "cn_morning"): "10:05",
    ("CN", "cn_midday"): "11:35",
    ("CN", "cn_tail"): "14:55",
    ("CN", "cn_close"): "15:10",
    ("HK", "hk_morning"): "10:05",
    ("HK", "hk_midday"): "12:05",
    ("HK", "hk_close"): "16:10",
    ("JP_KR", "jp_kr_morning"): "10:05",
    ("JP_KR", "jp_kr_midday"): "12:05",
    ("JP_KR", "jp_kr_close"): "15:40",
    ("US", "us_close"): "16:20",
    ("US", "us_retry"): "16:50",
}


INDEX_SIGNAL_CN_JUDGMENT_WINDOWS = {
    "cn_tail": ("14:55", "14:59", "judgmentDeliveryPending"),
    "cn_close": ("15:10", "15:15", "judgmentVerificationPending"),
}


INDEX_SIGNAL_ASSET_METADATA = {
    spec.key: {"name": spec.name, "region": spec.region, "regionLabel": spec.region_label}
    for spec in (*US_INDEXES, *CN_INDEXES, *HK_INDEXES, *JP_KR_INDEXES)
}


INDEX_SIGNAL_ASSET_SPECS = {
    spec.key: spec
    for spec in (*US_INDEXES, *CN_INDEXES, *HK_INDEXES, *JP_KR_INDEXES)
}


INDEX_SIGNAL_QUALITY_NONELIGIBLE = frozenset(
    {"partial", "awaiting_completed_bar", "awaiting_daily_close", "unavailable", "insufficient", "stale"}
)


_INDEX_SIGNAL_SCAN_LOCK = threading.Lock()


_AUTOMATION_STATE_LOCK = threading.RLock()


_INDEX_SIGNAL_STORE = IndexSignalStore(INDEX_SIGNAL_STATE)


_INDEX_SIGNAL_DAILY_PUSH_STORE = IndexSignalDailyPushStore(INDEX_SIGNAL_DAILY_PUSH_STATE)


_INDEX_SIGNAL_LAST_TEST: Dict[str, Any] = {}


_INDEX_SIGNAL_SCHEDULER_LOCK = threading.Lock()


_INDEX_SIGNAL_SCHEDULER_STARTED = False
_INDEX_SIGNAL_SCHEDULER_ALLOW_NOTIFICATIONS = False
_INDEX_SIGNAL_SCHEDULER_STOP = threading.Event()


_INDEX_SIGNAL_LAST_SCHEDULE_ATTEMPT: Dict[str, str] = {}


def _parse_bool(value: Any, default: bool = True) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"0", "false", "no", "off", "preview"}:
        return False
    if text in {"1", "true", "yes", "on", "execute"}:
        return True
    return default


def _read_json_file(path: Path, default: Dict[str, Any] | None = None) -> Dict[str, Any]:
    if not path.exists():
        return dict(default or {})
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
        return payload if isinstance(payload, dict) else dict(default or {})
    except Exception:
        return dict(default or {})


def _write_json_file(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.{threading.get_ident()}.tmp")
    try:
        temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def _index_signal_settings_result(settings: Dict[str, Any], *, mask_webhook: bool) -> Dict[str, Any]:
    """Return index settings without leaking the dedicated Webhook to API callers."""

    result = dict(settings)
    webhook = str(result.get("indexSignalDingTalkWebhook") or "").strip()
    result["webhookConfigured"] = bool(webhook)
    result["processAllowsNotifications"] = _INDEX_SIGNAL_SCHEDULER_ALLOW_NOTIFICATIONS
    if mask_webhook and webhook:
        result["indexSignalDingTalkWebhook"] = INDEX_SIGNAL_WEBHOOK_MASK
    return result


def load_index_signal_notification_settings(*, mask_webhook: bool = False) -> Dict[str, Any]:
    """Load the independent index-signal channel without cross-channel fallback."""

    settings: Dict[str, Any] = {
        **INDEX_SIGNAL_NOTIFICATION_DEFAULTS,
        "updatedAt": "",
        "source": "default",
    }
    saved = _read_json_file(INDEX_SIGNAL_NOTIFICATION_SETTINGS)
    if saved:
        settings.update(
            {
                "indexSignalDingTalkEnabled": _parse_bool(
                    saved.get("indexSignalDingTalkEnabled"),
                    default=INDEX_SIGNAL_NOTIFICATION_DEFAULTS["indexSignalDingTalkEnabled"],
                ),
                "indexSignalDingTalkWebhook": str(saved.get("indexSignalDingTalkWebhook") or "").strip(),
                "shortSignalsEnabled": _parse_bool(
                    saved.get("shortSignalsEnabled"),
                    default=INDEX_SIGNAL_NOTIFICATION_DEFAULTS["shortSignalsEnabled"],
                ),
                "mediumSignalsEnabled": _parse_bool(
                    saved.get("mediumSignalsEnabled"),
                    default=INDEX_SIGNAL_NOTIFICATION_DEFAULTS["mediumSignalsEnabled"],
                ),
                "longSignalsEnabled": _parse_bool(
                    saved.get("longSignalsEnabled"),
                    default=INDEX_SIGNAL_NOTIFICATION_DEFAULTS["longSignalsEnabled"],
                ),
                "warningEnabled": _parse_bool(
                    saved.get("warningEnabled"),
                    default=INDEX_SIGNAL_NOTIFICATION_DEFAULTS["warningEnabled"],
                ),
                "confirmedEnabled": _parse_bool(
                    saved.get("confirmedEnabled"),
                    default=INDEX_SIGNAL_NOTIFICATION_DEFAULTS["confirmedEnabled"],
                ),
                "invalidatedEnabled": _parse_bool(
                    saved.get("invalidatedEnabled"),
                    default=INDEX_SIGNAL_NOTIFICATION_DEFAULTS["invalidatedEnabled"],
                ),
                "updatedAt": str(saved.get("updatedAt") or ""),
                "source": "file",
            }
        )
    return _index_signal_settings_result(settings, mask_webhook=mask_webhook)


def save_index_signal_notification_settings(
    settings: Dict[str, Any] | None,
    *,
    mask_webhook: bool = False,
) -> Dict[str, Any]:
    """Save index settings with explicit missing/masked/blank Webhook semantics."""

    current = load_index_signal_notification_settings()
    if not isinstance(settings, dict):
        return _index_signal_settings_result(current, mask_webhook=mask_webhook)

    if "indexSignalDingTalkWebhook" not in settings:
        webhook_value = current.get("indexSignalDingTalkWebhook")
    else:
        submitted_webhook = str(settings.get("indexSignalDingTalkWebhook") or "").strip()
        if submitted_webhook == INDEX_SIGNAL_WEBHOOK_MASK:
            webhook_value = current.get("indexSignalDingTalkWebhook")
        else:
            # An explicit blank is intentional and must remain blank. Never fill it
            # from the general or theme DingTalk channels.
            webhook_value = submitted_webhook

    normalized = {
        "indexSignalDingTalkEnabled": _parse_bool(
            settings.get("indexSignalDingTalkEnabled"),
            default=current.get("indexSignalDingTalkEnabled", False),
        ),
        "indexSignalDingTalkWebhook": str(webhook_value or "").strip(),
        "shortSignalsEnabled": _parse_bool(
            settings.get("shortSignalsEnabled"),
            default=current.get("shortSignalsEnabled", True),
        ),
        "mediumSignalsEnabled": _parse_bool(
            settings.get("mediumSignalsEnabled"),
            default=current.get("mediumSignalsEnabled", True),
        ),
        "longSignalsEnabled": _parse_bool(
            settings.get("longSignalsEnabled"),
            default=current.get("longSignalsEnabled", True),
        ),
        "warningEnabled": _parse_bool(
            settings.get("warningEnabled"),
            default=current.get("warningEnabled", True),
        ),
        "confirmedEnabled": _parse_bool(
            settings.get("confirmedEnabled"),
            default=current.get("confirmedEnabled", True),
        ),
        "invalidatedEnabled": _parse_bool(
            settings.get("invalidatedEnabled"),
            default=current.get("invalidatedEnabled", True),
        ),
        "updatedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    if normalized["indexSignalDingTalkWebhook"]:
        validate_index_webhook(normalized["indexSignalDingTalkWebhook"])
    _write_json_file(INDEX_SIGNAL_NOTIFICATION_SETTINGS, normalized)
    result = {**normalized, "source": "file"}
    return _index_signal_settings_result(result, mask_webhook=mask_webhook)


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _today_text() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _is_cn_trading_day(dt: datetime | None = None) -> bool:
    return is_cn_trading_day(dt or datetime.now())


def _index_market_now(market: str) -> datetime:
    timezone_name = INDEX_SIGNAL_MARKET_TIMEZONES.get(str(market or "").upper(), "Asia/Shanghai")
    return datetime.now(ZoneInfo(timezone_name))


def _history_signal_rows(history_bundle: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Read completed daily bars without falling through to a live display bar.

    New market bundles always provide ``signalRows``.  The narrowly-scoped
    legacy fallback accepts ``rows`` only where the fixture/provider explicitly
    declares a completed close; this keeps old ad-hoc daily-only callers from
    silently becoming intraday aliases.
    """

    supplied = history_bundle.get("signalRows")
    if isinstance(supplied, list):
        return [dict(row) for row in supplied if isinstance(row, dict)]
    if history_bundle.get("completeClose") is True:
        legacy_rows = history_bundle.get("rows")
        if isinstance(legacy_rows, list):
            return [dict(row) for row in legacy_rows if isinstance(row, dict)]
    return []


def _history_display_rows(history_bundle: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = history_bundle.get("rows")
    return [dict(row) for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []


def _daily_close_is_final(item: Dict[str, Any], history_bundle: Dict[str, Any]) -> tuple[bool, str]:
    """Return explicit provider finality; never infer daily finality from clock time."""

    finality = history_bundle.get("dailyCloseFinality")
    if isinstance(finality, dict):
        if finality.get("completeClose") is True:
            return True, str(finality.get("reason") or "provider_close_final")
        return False, str(finality.get("reason") or "provider_close_pending")
    if history_bundle.get("completeClose") is True:
        return True, "legacy_complete_close"
    if item.get("completeClose") is True:
        return True, "legacy_item_complete_close"
    return False, "daily_close_finality_missing"


def _copy_timeframe_rows_with_signal_long(
    timeframe_rows: Dict[str, Any],
    signal_daily_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Keep intraday frame retrieval separate while forcing evaluation onto signalRows."""

    copied: Dict[str, Any] = {}
    for key, value in (timeframe_rows or {}).items():
        if key == "_quality":
            copied[key] = {name: dict(quality) for name, quality in value.items()} if isinstance(value, dict) else {}
        elif isinstance(value, list):
            copied[key] = [dict(row) for row in value if isinstance(row, dict)]
        else:
            copied[key] = value
    copied["long"] = [dict(row) for row in signal_daily_rows]
    return copied


def _unavailable_benchmark_timeframe_rows(
    signal_daily_rows: List[Dict[str, Any]],
    *,
    reason: str,
) -> Dict[str, Any]:
    """Keep the daily benchmark but explicitly disable failed intraday frames.

    ``evaluate_index_signals`` intentionally retains a daily-only compatibility
    fallback when callers do not provide *any* timeframe map.  A production
    scan does provide three independent asset frames, however, so passing
    ``None`` after a benchmark intraday failure would accidentally compare its
    15m/60m asset returns with daily benchmark returns.  An explicit empty map
    preserves the completed daily benchmark while making that comparison
    unavailable for the two intraday frames.
    """

    completed_daily_rows = [dict(row) for row in signal_daily_rows if isinstance(row, dict)]
    long_available = bool(completed_daily_rows)
    return {
        "short": [],
        "medium": [],
        "long": completed_daily_rows,
        "_quality": {
            "short": {"status": "unavailable", "reason": reason, "rowCount": 0},
            "medium": {"status": "unavailable", "reason": reason, "rowCount": 0},
            "long": {
                "status": "available" if long_available else "unavailable",
                "reason": "benchmark_completed_daily_rows" if long_available else "benchmark_daily_rows_missing",
                "rowCount": len(completed_daily_rows),
            },
        },
    }


def _legacy_frame_quality(timeframe_rows: Dict[str, Any], timeframe: str) -> Dict[str, Any]:
    rows = timeframe_rows.get(timeframe) if isinstance(timeframe_rows, dict) else None
    row_count = len(rows) if isinstance(rows, list) else 0
    return {
        "status": "available" if row_count else "unavailable",
        "reason": "legacy_frame_quality_inferred" if row_count else "frame_rows_missing",
        "rowCount": row_count,
    }


def _candidate_data_quality(
    *,
    candidate: Dict[str, Any],
    timeframe_rows: Dict[str, Any],
    timeframe: str,
    daily_close_final: bool,
    daily_close_reason: str,
) -> Dict[str, Any]:
    """Attach an explicit state/notification gate to every evaluated frame."""

    quality_map = timeframe_rows.get("_quality") if isinstance(timeframe_rows, dict) else None
    source_quality = quality_map.get(timeframe) if isinstance(quality_map, dict) else None
    source_quality = dict(source_quality) if isinstance(source_quality, dict) else _legacy_frame_quality(timeframe_rows, timeframe)
    source_status = str(source_quality.get("status") or "unavailable").strip().lower()
    source_reason = str(source_quality.get("reason") or "frame_quality_missing")

    if timeframe == "long" and not daily_close_final:
        source_status = "awaiting_daily_close"
        source_reason = daily_close_reason

    candidate_availability = str(candidate.get("availability") or "available").strip().lower()
    status = source_status
    reason = source_reason
    if source_status == "available" and candidate_availability != "available":
        status = candidate_availability if candidate_availability else "unavailable"
        reason = f"evaluator_{status}"

    eligible = (
        status == "available"
        and status not in INDEX_SIGNAL_QUALITY_NONELIGIBLE
        and candidate_availability == "available"
    )
    return {
        "status": status,
        "reason": reason,
        "stateEligible": eligible,
        "notificationEligible": eligible,
    }


def _frame_is_state_eligible(candidate: Dict[str, Any]) -> bool:
    quality = candidate.get("dataQuality") if isinstance(candidate.get("dataQuality"), dict) else {}
    return bool(quality.get("stateEligible"))


def _frame_has_lifecycle_conflict(candidate: Dict[str, Any]) -> bool:
    """A directional confluence conflict is observation-only, never state input."""

    status = str(candidate.get("candidateStatus") or "none").strip().lower()
    confluence = candidate.get("confluence") if isinstance(candidate.get("confluence"), dict) else {}
    level = str(confluence.get("level") or "").strip().lower()
    return status == "conflict" or level == "cross_timeframe_conflict"


def _frame_lifecycle_context(
    timeframe_rows: Dict[str, Any],
    *,
    timeframe: str,
    candidate: Dict[str, Any],
) -> Dict[str, Any]:
    """Return completed-source context required for Store lifecycle ageing."""

    raw_rows = timeframe_rows.get(timeframe) if isinstance(timeframe_rows, dict) else None
    rows = [row for row in raw_rows if isinstance(row, dict)] if isinstance(raw_rows, list) else []
    timestamps = [str(row.get("date") or "").strip() for row in rows]
    timestamps = [timestamp for timestamp in timestamps if timestamp]
    latest_close = rows[-1].get("close") if rows else None
    metrics = candidate.get("metrics") if isinstance(candidate.get("metrics"), dict) else {}
    return {
        "completed_bar_timestamps": timestamps,
        "latest_close": latest_close,
        "latest_atr": metrics.get("atr14"),
        "availability": str(candidate.get("availability") or "available").lower(),
    }


def _retryable_us_daily_data_lag(
    *,
    market: str,
    phase: str,
    latest_date: str,
    expected_market_date: str,
    history_bundle: Dict[str, Any],
) -> bool:
    """Retry only explicit current-day provider finality lag, never a stale holiday."""

    if market != "US" or phase != "us_close" or latest_date != expected_market_date:
        return False
    finality = history_bundle.get("dailyCloseFinality")
    if not isinstance(finality, dict):
        return False
    return finality.get("completeClose") is False and str(finality.get("status") or "").lower() == "pending"


def _due_index_signal_jobs(now_utc: datetime, tasks: Dict[str, Any]) -> List[Dict[str, Any]]:
    if now_utc.tzinfo is None:
        raise ValueError("now_utc 必须包含时区")
    tasks = tasks if isinstance(tasks, dict) else {}
    jobs: List[Dict[str, Any]] = []
    for (market, phase), local_time in INDEX_SIGNAL_LOCAL_TIMES.items():
        local_now = now_utc.astimezone(ZoneInfo(INDEX_SIGNAL_MARKET_TIMEZONES[market]))
        local_minute = local_now.strftime("%H:%M")
        cn_window = INDEX_SIGNAL_CN_JUDGMENT_WINDOWS.get(phase) if market == "CN" else None
        cn_window_end = cn_window[1] if cn_window else None
        in_due_window = (
            local_time <= local_minute <= cn_window_end
            if cn_window_end is not None
            else local_minute == local_time
        )
        if not in_due_window or local_now.weekday() >= 5:
            continue
        if market == "CN" and not _is_cn_trading_day(local_now.replace(tzinfo=None)):
            continue

        market_date = local_now.date().isoformat()
        task_key = INDEX_SIGNAL_TASK_KEYS[(market, phase)]
        task_state = tasks.get(task_key) if isinstance(tasks.get(task_key), dict) else {}
        current_run_minute = local_now.strftime("%Y-%m-%d %H:%M")
        if task_state.get("running"):
            running_minute = str(task_state.get("lastRunAt") or "").strip().replace("T", " ")[:16]
            if not running_minute or running_minute == current_run_minute:
                continue
        last_trigger = str(task_state.get("lastTrigger") or "")
        legacy_scheduled = last_trigger.startswith("scheduled-")
        last_scheduled_market_date = str(
            task_state.get("lastScheduledMarketDate")
            or (task_state.get("lastRunMarketDate") if legacy_scheduled else "")
            or ""
        )
        last_scheduled_phase = str(
            task_state.get("lastScheduledPhase")
            or (task_state.get("lastRunPhase") if legacy_scheduled else "")
            or ""
        )
        already_ran = (
            last_scheduled_market_date == market_date
            and last_scheduled_phase == phase
        )
        if already_ran:
            pending_field = cn_window[2] if cn_window else None
            delivery_pending = bool(pending_field and task_state.get(pending_field) is True)
            last_run_minute = str(
                task_state.get("lastScheduledRunAt")
                or task_state.get("lastRunAt")
                or ""
            ).strip().replace("T", " ")[:16]
            crossed_minute = bool(
                last_run_minute != current_run_minute
                if last_run_minute
                else local_minute != local_time
            )
            if not delivery_pending or not crossed_minute:
                continue

        if phase == "us_retry":
            close_state = tasks.get(INDEX_SIGNAL_TASK_KEYS[("US", "us_close")])
            close_state = close_state if isinstance(close_state, dict) else {}
            close_summary = close_state.get("summary") if isinstance(close_state.get("summary"), dict) else {}
            close_has_retryable_data_lag = bool(
                close_state.get("retryableDataLag") or close_summary.get("retryableDataLag")
            )
            if (
                str(close_state.get("lastRunMarketDate") or "") != market_date
                or str(close_state.get("lastRunPhase") or "") != "us_close"
                or not close_has_retryable_data_lag
            ):
                continue

        jobs.append(
            {
                "task": task_key,
                "market": market,
                "phase": phase,
                "marketDate": market_date,
                "localTime": local_time,
                "attemptMinute": local_now.strftime("%Y-%m-%d %H:%M"),
                "timezone": INDEX_SIGNAL_MARKET_TIMEZONES[market],
            }
        )
    return jobs


def _expired_cn_judgment_windows(now_utc: datetime, tasks: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return pending CN judgment tasks whose bounded retry window has ended."""

    if now_utc.tzinfo is None:
        raise ValueError("now_utc 必须包含时区")
    tasks = tasks if isinstance(tasks, dict) else {}
    local_now = now_utc.astimezone(ZoneInfo(INDEX_SIGNAL_MARKET_TIMEZONES["CN"]))
    current_date = local_now.date().isoformat()
    current_minute = local_now.strftime("%H:%M")
    expired: List[Dict[str, Any]] = []
    for phase, (_start, end, pending_field) in INDEX_SIGNAL_CN_JUDGMENT_WINDOWS.items():
        task_key = INDEX_SIGNAL_TASK_KEYS[("CN", phase)]
        task_state = tasks.get(task_key) if isinstance(tasks.get(task_key), dict) else {}
        if task_state.get(pending_field) is not True:
            continue
        task_date = str(
            task_state.get("lastScheduledMarketDate")
            or task_state.get("lastRunMarketDate")
            or ""
        )
        window_ended = bool(
            task_date
            and (task_date < current_date or (task_date == current_date and current_minute > end))
        )
        if not window_ended:
            continue
        expired.append(
            {
                "task": task_key,
                "phase": phase,
                "marketDate": task_date,
                "updates": {
                    **task_state,
                    pending_field: False,
                    "judgmentWindowExpired": True,
                    "judgmentWindowExpiredAt": local_now.strftime("%Y-%m-%d %H:%M:%S"),
                    "judgmentWindowExpiredPhase": phase,
                    "running": False,
                },
            }
        )
    return expired


def _validate_index_signal_phase(market: str, phase: str) -> tuple[str, str]:
    normalized_market = str(market or "").strip().upper()
    normalized_phase = str(phase or "").strip().lower()
    if normalized_market not in INDEX_SIGNAL_PHASES:
        raise ValueError(f"不支持的指数市场：{normalized_market or '--'}")
    if normalized_phase not in INDEX_SIGNAL_PHASES[normalized_market]:
        raise ValueError(f"市场 {normalized_market} 不支持扫描阶段：{normalized_phase or '--'}")
    return normalized_market, normalized_phase


def _index_signal_next_scan(market: str, phase: str) -> str:
    return {
        ("CN", "cn_morning"): "今日 11:35（中国市场）",
        ("CN", "cn_midday"): "今日 14:55（中国市场尾盘判断）",
        ("CN", "cn_tail"): "今日 15:10（中国市场收盘复核）",
        ("CN", "cn_close"): "下一交易日 10:05（中国市场）",
        ("HK", "hk_morning"): "今日 12:05（香港市场）",
        ("HK", "hk_midday"): "今日 16:10（香港市场收盘）",
        ("HK", "hk_close"): "下一交易日 10:05（香港市场）",
        ("JP_KR", "jp_kr_morning"): "今日 12:05（日韩市场）",
        ("JP_KR", "jp_kr_midday"): "今日 15:40（日韩市场收盘）",
        ("JP_KR", "jp_kr_close"): "下一交易日 10:05（日韩市场）",
        ("US", "us_close"): "如日线未更新则今日 16:50 ET 重试，否则下一交易日 16:20 ET",
        ("US", "us_retry"): "下一交易日 16:20 ET",
    }.get((market, phase), "按对应市场下一计划扫描时点")


def _index_signal_event_details(
    event: Dict[str, Any],
    *,
    item: Dict[str, Any],
    market: str,
    phase: str,
) -> Dict[str, Any]:
    enriched = _enrich_index_signal_event(event)
    enriched.update(
        {
            "indexName": item.get("name") or item.get("key"),
            "market": market,
            "region": market,
            "price": item.get("price"),
            "currentPrice": item.get("price"),
            "source": item.get("source"),
            "nextScanAt": _index_signal_next_scan(market, phase),
            "structureSummary": event.get("pattern") or "--",
            "pressureSupport": event.get("keyLevel"),
            "repairLevel": event.get("invalidationLevel"),
            "invalidationCondition": (
                "收盘重新站上失效位并保持两个有效收盘" if event.get("direction") == "top" else "收盘跌破失效位并保持两个有效收盘"
            ),
        }
    )
    return enriched


def _enrich_index_signal_event(event: Dict[str, Any]) -> Dict[str, Any]:
    enriched = dict(event)
    index_key = str(enriched.get("indexKey") or enriched.get("key") or "").strip()
    metadata = INDEX_SIGNAL_ASSET_METADATA.get(index_key, {})
    configured_name = str(metadata.get("name") or "").strip()
    current_name = str(enriched.get("indexName") or enriched.get("name") or "").strip()
    if configured_name and (not current_name or current_name == index_key):
        current_name = configured_name
    enriched["indexName"] = current_name or index_key or "指数"
    enriched["region"] = str(enriched.get("region") or metadata.get("region") or "")
    enriched["regionLabel"] = str(enriched.get("regionLabel") or metadata.get("regionLabel") or "")
    enriched["status"] = str(
        enriched.get("status") or enriched.get("toStatus") or enriched.get("candidateStatus") or "none"
    )
    return enriched


def _index_signal_daily_activity(index_tasks: Dict[str, Any]) -> Dict[str, Any]:
    today = _today_text()
    today_tasks = []
    for task in index_tasks.values():
        if not isinstance(task, dict):
            continue
        last_run_at = str(task.get("lastRunAt") or "")
        last_run_date = str(task.get("lastRunDate") or last_run_at[:10])
        if last_run_date == today:
            today_tasks.append(task)

    summaries = [task.get("summary") for task in today_tasks if isinstance(task.get("summary"), dict)]
    event_count = sum(int(summary.get("events") or 0) for summary in summaries)
    notification_count = sum(int(summary.get("notifications") or 0) for summary in summaries)
    judgment_report_count = sum(int(summary.get("judgmentReports") or 0) for summary in summaries)
    judgment_change_count = sum(int(summary.get("judgmentChanges") or 0) for summary in summaries)
    judgment_verification_count = sum(int(summary.get("judgmentVerifications") or 0) for summary in summaries)
    judgment_unchanged_count = sum(int(summary.get("judgmentUnchanged") or 0) for summary in summaries)
    judgment_pending_count = sum(
        int(bool(task.get("judgmentDeliveryPending") or task.get("judgmentVerificationPending")))
        for task in today_tasks
    )
    judgment_window_expired_count = sum(int(bool(task.get("judgmentWindowExpired"))) for task in today_tasks)
    data_pending_count = sum(int(summary.get("dataPending") or 0) for summary in summaries)
    stale_count = sum(
        max(
            int((task.get("summary") or {}).get("stale") or 0)
            if isinstance(task.get("summary"), dict)
            else 0,
            int(bool(task.get("stale"))),
        )
        for task in today_tasks
    )
    error_count = 0
    for task in today_tasks:
        summary = task.get("summary") if isinstance(task.get("summary"), dict) else {}
        task_errors = int(summary.get("errors") or 0)
        if task.get("ok") is False and task_errors == 0:
            task_errors = 1
        error_count += task_errors
    last_scan_at = max((str(task.get("lastRunAt") or "") for task in today_tasks), default="")
    scan_count = len(today_tasks)
    issue_text = "、".join(
        part
        for part in (
            f"数据陈旧 {stale_count} 项" if stale_count else "",
            f"错误 {error_count} 项" if error_count else "",
            f"尾盘/收盘复核窗口超时 {judgment_window_expired_count} 项" if judgment_window_expired_count else "",
        )
        if part
    )
    if not scan_count:
        message = "今日尚未完成指数信号扫描；到计划时点后会自动更新。"
    elif event_count:
        message = (
            f"今日已完成 {scan_count} 次指数信号扫描，最近一次 {last_scan_at}；"
            f"产生 {event_count} 条状态变化，实际推送 {notification_count} 条。"
        )
        if judgment_report_count:
            message += f"其中每日判断报告 {judgment_report_count} 条。"
        if issue_text:
            message += f"另有{issue_text}。"
    elif judgment_report_count:
        message = (
            f"今日已完成 {scan_count} 次指数信号扫描，最近一次 {last_scan_at}；"
            f"已推送 {judgment_report_count} 条尾盘/收盘判断报告。"
        )
        if judgment_change_count:
            message += f"收盘复核发现 {judgment_change_count} 项判断变化并已修正推送。"
        elif judgment_unchanged_count:
            message += "15:10 收盘复核一致，无需重复推送。"
        elif judgment_pending_count:
            message += "收盘数据仍在等待完成，系统会在复核窗口继续验证。"
        if issue_text:
            message += f"另有{issue_text}。"
    elif judgment_pending_count:
        message = (
            f"今日已执行 {scan_count} 次指数信号扫描，最近一次 {last_scan_at}；"
            "尾盘判断发送或收盘复核仍待完成，系统会在计划窗口内重试。"
        )
        if issue_text:
            message += f"另有{issue_text}。"
    elif issue_text:
        message = (
            f"今日已执行 {scan_count} 次指数信号扫描，最近一次 {last_scan_at}；"
            f"未产生状态变化，另有{issue_text}，对应市场数据未完整进入评分。"
        )
    else:
        message = (
            f"今日已完成 {scan_count} 次指数信号扫描，最近一次 {last_scan_at}；"
            "状态未切换，所以没有新增状态变化推送。"
        )
    return {
        "date": today,
        "scanCount": scan_count,
        "eventCount": event_count,
        "notificationCount": notification_count,
        "judgmentReportCount": judgment_report_count,
        "judgmentChangeCount": judgment_change_count,
        "judgmentVerificationCount": judgment_verification_count,
        "judgmentUnchangedCount": judgment_unchanged_count,
        "judgmentPendingCount": judgment_pending_count,
        "judgmentWindowExpiredCount": judgment_window_expired_count,
        "dataPendingCount": data_pending_count,
        "staleCount": stale_count,
        "errorCount": error_count,
        "lastScanAt": last_scan_at,
        "hasStateChange": bool(event_count),
        "hasDataIssue": bool(stale_count or error_count or data_pending_count or judgment_window_expired_count),
        "message": message,
    }


def _index_signal_enabled_timeframes(settings: Dict[str, Any]) -> tuple[str, ...]:
    toggles = {
        "short": "shortSignalsEnabled",
        "medium": "mediumSignalsEnabled",
        "long": "longSignalsEnabled",
    }
    return tuple(
        timeframe
        for timeframe in TIMEFRAME_IDS
        if _parse_bool(settings.get(toggles[timeframe]), default=True)
    )


def _is_scheduled_cn_judgment_run(*, market: str, phase: str, trigger: str) -> bool:
    return (
        market == "CN"
        and phase in {"cn_tail", "cn_close"}
        and str(trigger or "").strip().lower().startswith("scheduled-")
    )


def _capture_cn_index_judgment(
    *,
    phase: str,
    trigger: str,
    market_date: str,
    generated_at: str,
    scan_items: List[Dict[str, Any]],
    expected_item_count: int,
    expected_item_keys: List[str] | None = None,
    settings: Dict[str, Any],
    notify: bool,
) -> Dict[str, Any]:
    """Persist one scheduled CN judgment and apply the tail/close send contract."""

    enabled_timeframes = _index_signal_enabled_timeframes(settings)
    rules = index_signal_rules()
    snapshot = build_index_signal_judgment_snapshot(
        scan_items,
        market="CN",
        market_date=market_date,
        phase=phase,
        generated_at=generated_at,
        rule_version=str(rules.get("version") or ""),
        enabled_timeframes=enabled_timeframes,
        expected_item_count=expected_item_count,
        expected_item_keys=expected_item_keys,
    )
    webhook = str(settings.get("indexSignalDingTalkWebhook") or "").strip()
    channel_enabled = _parse_bool(settings.get("indexSignalDingTalkEnabled"), default=False)
    channel_key = index_signal_channel_hash(webhook)
    record = _INDEX_SIGNAL_DAILY_PUSH_STORE.get(
        market="CN",
        market_date=market_date,
        channel_key=channel_key,
    )
    close_ready = phase != "cn_close" or close_snapshot_ready(snapshot)
    plan = plan_index_signal_judgment_delivery(
        phase=phase,
        market_date=market_date,
        snapshot=snapshot,
        record=record,
        close_ready=close_ready,
    )
    terminal_reasons = {"tail_already_delivered", "close_already_delivered", "close_already_verified"}
    if str(plan.get("reason") or "") in terminal_reasons:
        pass
    elif not enabled_timeframes:
        plan = {
            "send": False,
            "reportType": "tail" if phase == "cn_tail" else "close_same",
            "reason": "all_timeframe_notifications_disabled",
            "comparison": None,
        }
    elif not channel_enabled or not webhook:
        plan = {
            "send": False,
            "reportType": "tail" if phase == "cn_tail" else "close_same",
            "reason": "notification_channel_disabled" if not channel_enabled else "notification_webhook_unconfigured",
            "comparison": None,
        }

    stage = "tail" if phase == "cn_tail" else "close"
    if str(plan.get("reason") or "") not in terminal_reasons:
        _INDEX_SIGNAL_DAILY_PUSH_STORE.record_capture(
            market="CN",
            market_date=market_date,
            channel_key=channel_key,
            stage=stage,
            snapshot=snapshot,
            plan=plan,
        )

    if plan.get("send") and notify:
        notification_result = notify_index_signal_judgment(
            snapshot,
            settings=settings,
            trigger=trigger,
            report_type=str(plan.get("reportType") or stage),
            comparison=plan.get("comparison") if isinstance(plan.get("comparison"), dict) else None,
        )
    else:
        notification_result = {
            "ok": True,
            "skipped": True,
            "reportType": str(plan.get("reportType") or stage),
            "reason": (
                "run_notify_disabled"
                if plan.get("send") and not notify
                else str(plan.get("reason") or "not_required")
            ),
            "message": "本次无需推送" if not plan.get("send") else "本次扫描已关闭通知",
        }

    if str(plan.get("reason") or "") not in terminal_reasons:
        _INDEX_SIGNAL_DAILY_PUSH_STORE.record_delivery_result(
            market="CN",
            market_date=market_date,
            channel_key=channel_key,
            stage=stage,
            result=notification_result,
        )

    delivered = bool(notification_result.get("ok")) and not bool(notification_result.get("skipped"))
    send_required = bool(plan.get("send"))
    comparison = plan.get("comparison") if isinstance(plan.get("comparison"), dict) else {}
    if stage == "close" and str(plan.get("reportType") or "") == "close_fallback" and delivered:
        _INDEX_SIGNAL_DAILY_PUSH_STORE.resolve_tail_by_close_fallback(
            market="CN",
            market_date=market_date,
            channel_key=channel_key,
            resolved_at=str(notification_result.get("notifiedAt") or generated_at),
        )
    return {
        "ok": bool(notification_result.get("ok")),
        "stage": stage,
        "reportType": str(plan.get("reportType") or stage),
        "reason": str(plan.get("reason") or ""),
        "fingerprint": str(snapshot.get("fingerprint") or ""),
        "closeReady": bool(close_ready),
        "enabledTimeframes": list(enabled_timeframes),
        "sendRequired": send_required,
        "delivered": delivered,
        "changeCount": len(comparison.get("changes") or []),
        "deliveryPending": bool(stage == "tail" and send_required and not delivered),
        "verificationPending": bool(
            stage == "close"
            and (
                str(plan.get("reportType") or "") == "close_pending"
                or (send_required and not delivered)
            )
        ),
        "notification": notification_result,
    }


def _resolve_cn_tail_after_close_fallback(resolved_at: str) -> None:
    _save_automation_task(
        INDEX_SIGNAL_TASK_KEYS[("CN", "cn_tail")],
        {
            "judgmentDeliveryPending": False,
            "judgmentResolvedByCloseFallback": True,
            "judgmentResolvedAt": resolved_at,
            "judgmentWindowExpired": False,
        },
    )


def _run_index_signal_scan(
    *,
    market: str,
    phase: str,
    trigger: str,
    notify: bool = False,
) -> Dict[str, Any]:
    market, phase = _validate_index_signal_phase(market, phase)
    calendar_supported = _index_market_now(market).year == 2026
    if not calendar_supported and (str(trigger).startswith("scheduled-") or phase.endswith("_close") or phase == "us_retry"):
        return {
            "ok": False, "skipped": True, "unknownCalendar": True,
            "market": market, "phase": phase, "generatedAt": _now_text(),
            "error": "内置交易日历仅覆盖 2026 年；未知年份暂停自动扫描及收盘信号确认",
        }
    notify = bool(notify and _INDEX_SIGNAL_SCHEDULER_ALLOW_NOTIFICATIONS and calendar_supported)
    task_key = INDEX_SIGNAL_TASK_KEYS[(market, phase)]
    started_at = _now_text()
    if not _INDEX_SIGNAL_SCAN_LOCK.acquire(blocking=False):
        return {
            "ok": False,
            "running": True,
            "market": market,
            "phase": phase,
            "generatedAt": started_at,
            "error": "已有指数信号扫描正在运行",
        }

    try:
        _save_automation_task(
            task_key,
            {
                "ok": True,
                "running": True,
                "lastRunAt": started_at,
                "lastTrigger": trigger,
                "lastRunPhase": phase,
                "summary": "指数顶底信号扫描运行中",
            },
        )
        # Signal scans retain their original 30-second freshness contract while
        # the dashboard can keep its longer cache-first display window.
        bundle = build_index_market_bundle(force_refresh=False, max_age_seconds=30)
        group = next(
            (entry for entry in bundle.get("groups") or [] if str(entry.get("key") or "").upper() == market),
            None,
        )
        if not isinstance(group, dict):
            raise RuntimeError(f"指数行情中缺少 {market} 市场分组")

        items = [
            item
            for item in group.get("items") or []
            if isinstance(item, dict) and item.get("signalEligible") is not False
        ]
        if not items:
            raise RuntimeError(f"{market} 市场没有可用指数行情")
        item_by_key = {str(item.get("key") or ""): item for item in items}
        benchmark_item = item_by_key.get(INDEX_SIGNAL_BENCHMARK_KEYS.get(market, ""))
        benchmark_bundle = benchmark_item.get("_historyBundle") if isinstance(benchmark_item, dict) else None
        benchmark_bundle = benchmark_bundle if isinstance(benchmark_bundle, dict) else {}
        benchmark_display_rows = _history_display_rows(benchmark_bundle)
        benchmark_signal_rows = _history_signal_rows(benchmark_bundle)
        # Production scans always pass an explicit benchmark frame map.  This
        # prevents the evaluator's deliberate legacy daily-only fallback from
        # leaking daily benchmark rows into a successful 15m/60m asset frame.
        benchmark_timeframe_rows: Dict[str, Any] = _unavailable_benchmark_timeframe_rows(
            benchmark_signal_rows,
            reason="benchmark_item_missing",
        )
        if isinstance(benchmark_item, dict):
            benchmark_key = str(benchmark_item.get("key") or "")
            benchmark_spec = INDEX_SIGNAL_ASSET_SPECS.get(benchmark_key)
            if benchmark_spec is not None:
                try:
                    benchmark_timeframe_rows = _copy_timeframe_rows_with_signal_long(
                        get_index_signal_timeframes(
                            benchmark_spec,
                            # This call needs the display session date only to
                            # validate completed intraday bars.  Its daily
                            # frame is replaced with signalRows before use.
                            daily_rows=benchmark_display_rows,
                        ),
                        benchmark_signal_rows,
                    )
                except Exception:  # noqa: BLE001 - daily evaluation remains available below
                    benchmark_timeframe_rows = _unavailable_benchmark_timeframe_rows(
                        benchmark_signal_rows,
                        reason="benchmark_intraday_fetch_failed",
                    )
        expected_market_date = _index_market_now(market).date().isoformat()
        close_phase = phase.endswith("_close") or phase == "us_retry"
        evaluator_phase = "us_close" if phase == "us_retry" else phase
        settings = load_index_signal_notification_settings()
        scheduled_cn_judgment = _is_scheduled_cn_judgment_run(
            market=market,
            phase=phase,
            trigger=trigger,
        )
        snapshot = _INDEX_SIGNAL_STORE.snapshot()
        summary = {
            "processed": 0,
            "warning": 0,
            "confirmed": 0,
            "invalidated": 0,
            "expired": 0,
            "conflict": 0,
            "none": 0,
            "stale": 0,
            "errors": 0,
            "events": 0,
            "notifications": 0,
            "judgmentReports": 0,
            "judgmentChanges": 0,
            "judgmentVerifications": 0,
            "judgmentUnchanged": 0,
            "dataPending": 0,
            "retryableDataLag": False,
        }
        errors: List[Dict[str, Any]] = []
        scan_items: List[Dict[str, Any]] = []
        observations: List[Dict[str, Any]] = []

        def record_transition(
            applied: Dict[str, Any],
            *,
            candidate: Dict[str, Any],
            item: Dict[str, Any],
        ) -> None:
            """Persist/send only transitions that the Store declared notifiable."""

            event = applied.get("event") if isinstance(applied, dict) else None
            if not isinstance(event, dict):
                return
            summary["events"] += 1
            # Expiry and any other suppressed transition are already final in
            # Store.  Do not call mark_notification with a synthetic success,
            # otherwise an expiry would look like a delivered DingTalk alert.
            if not applied.get("shouldNotify"):
                return
            enriched_event = _index_signal_event_details(event, item=item, market=market, phase=phase)
            quality = candidate.get("dataQuality") if isinstance(candidate.get("dataQuality"), dict) else {}
            if scheduled_cn_judgment:
                notification_result = {
                    "ok": True,
                    "skipped": True,
                    "status": "covered_by_daily_report",
                    "coveredByDailyReport": True,
                    "reason": "daily_judgment_report_supersedes_transition",
                    "message": "本次状态变化已合并到每日指数判断报告",
                }
            elif notify and quality.get("notificationEligible"):
                notification_result = notify_index_signal_transition(
                    enriched_event,
                    settings=settings,
                    trigger=trigger,
                )
                if notification_result.get("ok") and not notification_result.get("skipped"):
                    summary["notifications"] += 1
            else:
                notification_result = {
                    "ok": True,
                    "skipped": True,
                    "reason": "run_notify_disabled" if not notify else "event_not_notifiable",
                }
            _INDEX_SIGNAL_STORE.mark_notification(str(event.get("id") or ""), notification_result)

        for item in items:
            index_key = str(item.get("key") or "")
            history_bundle = item.get("_historyBundle") if isinstance(item.get("_historyBundle"), dict) else {}
            display_daily_rows = _history_display_rows(history_bundle)
            daily_rows = _history_signal_rows(history_bundle)
            daily_close_final, daily_close_reason = _daily_close_is_final(item, history_bundle)
            latest_date = str(item.get("latestDate") or history_bundle.get("latestDate") or "")
            stale_reason = ""
            if latest_date != expected_market_date:
                stale_reason = f"最新日线 {latest_date or '--'}，预期 {expected_market_date}"
            elif not display_daily_rows:
                stale_reason = "缺少日线历史"
            elif not close_phase and item.get("price") is None:
                stale_reason = "盘中实时价缺失"

            if _retryable_us_daily_data_lag(
                market=market,
                phase=phase,
                latest_date=latest_date,
                expected_market_date=expected_market_date,
                history_bundle=history_bundle,
            ):
                summary["retryableDataLag"] = True

            if stale_reason:
                summary["stale"] += 1
                scan_items.append({"key": index_key, "name": item.get("name"), "stale": True, "reason": stale_reason})
                continue

            try:
                spec = INDEX_SIGNAL_ASSET_SPECS.get(index_key)
                if spec is None:
                    # A test or future ad-hoc item may not be registered in
                    # IndexSpec. It receives only the trustworthy daily frame,
                    # never a fabricated short/medium alias of that same data.
                    long_status = "available" if daily_rows and daily_close_final else "awaiting_daily_close"
                    timeframe_rows = {
                        "short": [],
                        "medium": [],
                        "long": [dict(row) for row in daily_rows],
                        "_quality": {
                            "short": {"status": "unavailable", "reason": "ad_hoc_daily_only", "rowCount": 0},
                            "medium": {"status": "unavailable", "reason": "ad_hoc_daily_only", "rowCount": 0},
                            "long": {
                                "status": long_status,
                                "reason": "ad_hoc_completed_daily_rows" if long_status == "available" else daily_close_reason,
                                "rowCount": len(daily_rows),
                            },
                        },
                    }
                else:
                    try:
                        timeframe_rows = _copy_timeframe_rows_with_signal_long(
                            get_index_signal_timeframes(
                                spec,
                                # The source helper needs the display market
                                # date to validate intraday frame freshness.
                                # Its returned daily frame is never evaluated.
                                daily_rows=display_daily_rows,
                            ),
                            daily_rows,
                        )
                    except Exception as exc:  # noqa: BLE001 - preserve daily-bar signal availability
                        timeframe_rows = {
                            "short": [],
                            "medium": [],
                            "long": [dict(row) for row in daily_rows],
                            "_quality": {
                                "short": {"status": "unavailable", "reason": str(exc), "rowCount": 0},
                                "medium": {"status": "unavailable", "reason": str(exc), "rowCount": 0},
                                "long": {
                                    "status": "available" if daily_rows else "unavailable",
                                    "reason": "completed_daily_fallback" if daily_rows else "daily_rows_missing",
                                    "rowCount": len(daily_rows),
                                },
                            },
                        }
                        errors.append(
                            {
                                "key": index_key,
                                "name": item.get("name"),
                                "error": f"多周期K线获取失败：{exc}",
                            }
                        )
                evaluated = evaluate_index_signals(
                    spec={"key": index_key, "region": market},
                    daily_rows=daily_rows,
                    current_price=None,
                    benchmark_rows=benchmark_signal_rows,
                    scan_phase=evaluator_phase,
                    timeframe_rows=timeframe_rows,
                    benchmark_timeframe_rows=benchmark_timeframe_rows,
                )
                summary["processed"] += 1
                item_result = {"key": index_key, "name": item.get("name"), "timeframes": {}}
                for timeframe in TIMEFRAME_IDS:
                    candidate = dict(evaluated.get(timeframe) or {})
                    candidate["dataQuality"] = _candidate_data_quality(
                        candidate=candidate,
                        timeframe_rows=timeframe_rows,
                        timeframe=timeframe,
                        daily_close_final=daily_close_final,
                        daily_close_reason=daily_close_reason,
                    )
                    item_result["timeframes"][timeframe] = candidate
                    observations.append(
                        {
                            "indexKey": index_key,
                            "timeframe": timeframe,
                            "marketDate": expected_market_date,
                            "scanPhase": evaluator_phase,
                            "candidate": candidate,
                        }
                    )
                    if not _frame_is_state_eligible(candidate):
                        summary["dataPending"] += 1
                        continue
                    status = str(candidate.get("candidateStatus") or "none").lower()
                    lifecycle = str(candidate.get("lifecycleState") or "").lower()
                    if lifecycle not in {"candidate", "resonant", "confirmed", "invalidated", "expired", "none"}:
                        lifecycle = ""
                    if not lifecycle and status in {"confirmed", "invalidated", "expired"}:
                        lifecycle = status
                        candidate["lifecycleState"] = lifecycle
                    direction = str(candidate.get("direction") or "").lower()

                    if _frame_has_lifecycle_conflict(candidate):
                        summary["conflict"] += 1
                        continue
                    if direction not in {"top", "bottom"}:
                        summary["none"] += 1
                        continue

                    effective_status = lifecycle if lifecycle in {"invalidated", "expired"} else status
                    if effective_status not in {"warning", "confirmed", "invalidated", "expired"}:
                        summary["none"] += 1
                        continue

                    summary[effective_status] += 1
                    applied = _INDEX_SIGNAL_STORE.apply_scan(
                        index_key=index_key,
                        timeframe=timeframe,
                        direction=direction,
                        candidate=candidate,
                        market_date=expected_market_date,
                        scan_phase=evaluator_phase,
                    )
                    record_transition(applied, candidate=candidate, item=item)

                # A qualified no-pattern frame ages both persisted directions;
                # it never clears them immediately.  A directional candidate
                # itself is reconciled by apply_scan, while the opposite
                # direction remains eligible for frozen-boundary invalidation
                # or width-based expiry.  Pending data and conflicts are
                # intentionally excluded from this lifecycle clock.
                reconcile_missing = getattr(_INDEX_SIGNAL_STORE, "reconcile_missing", None)
                if callable(reconcile_missing):
                    for timeframe in TIMEFRAME_IDS:
                        frame_candidate = dict(item_result["timeframes"].get(timeframe) or {})
                        if not _frame_is_state_eligible(frame_candidate):
                            continue
                        frame_status = str(frame_candidate.get("candidateStatus") or "none").lower()
                        if _frame_has_lifecycle_conflict(frame_candidate):
                            continue
                        frame_direction = str(frame_candidate.get("direction") or "").lower()
                        frame_lifecycle = str(frame_candidate.get("lifecycleState") or "").lower()
                        effective_status = (
                            frame_lifecycle
                            if frame_lifecycle in {"invalidated", "expired"}
                            else frame_status
                        )
                        observed_direction = (
                            frame_direction
                            if frame_direction in {"top", "bottom"}
                            and effective_status in {"warning", "confirmed", "invalidated", "expired"}
                            else None
                        )
                        lifecycle_context = _frame_lifecycle_context(
                            timeframe_rows,
                            timeframe=timeframe,
                            candidate=frame_candidate,
                        )
                        for stored_direction in ("top", "bottom"):
                            if stored_direction == observed_direction:
                                continue
                            reconciled = reconcile_missing(
                                index_key=index_key,
                                timeframe=timeframe,
                                direction=stored_direction,
                                market_date=expected_market_date,
                                scan_phase=evaluator_phase,
                                data_quality=frame_candidate.get("dataQuality") if isinstance(frame_candidate.get("dataQuality"), dict) else {},
                                observation_status=frame_status,
                                **lifecycle_context,
                            )
                            reconcile_event = reconciled.get("event") if isinstance(reconciled, dict) else None
                            if isinstance(reconcile_event, dict):
                                transition_status = str(reconcile_event.get("toStatus") or "").lower()
                                if transition_status in summary:
                                    summary[transition_status] += 1
                            record_transition(reconciled, candidate=frame_candidate, item=item)
                scan_items.append(item_result)
            except Exception as exc:  # noqa: BLE001
                summary["errors"] += 1
                errors.append({"key": index_key, "name": item.get("name"), "error": str(exc)})

        for source_error in bundle.get("errors") or []:
            if not isinstance(source_error, dict) or str(source_error.get("region") or "").upper() != market:
                continue
            summary["errors"] += 1
            errors.append(dict(source_error))
            if market == "US" and phase == "us_close":
                summary["stale"] += 1

        _INDEX_SIGNAL_STORE.record_observations(observations)

        judgment: Dict[str, Any] | None = None
        if scheduled_cn_judgment:
            try:
                judgment = _capture_cn_index_judgment(
                    phase=phase,
                    trigger=trigger,
                    market_date=expected_market_date,
                    generated_at=_now_text(),
                    scan_items=scan_items,
                    expected_item_count=len({spec.key for spec in CN_INDEXES}),
                    expected_item_keys=sorted({spec.key for spec in CN_INDEXES}),
                    settings=settings,
                    notify=notify,
                )
                if judgment.get("delivered"):
                    summary["notifications"] += 1
                    summary["judgmentReports"] += 1
                summary["judgmentChanges"] = int(judgment.get("changeCount") or 0)
                if phase == "cn_close" and judgment.get("closeReady"):
                    summary["judgmentVerifications"] = 1
                    if judgment.get("reportType") == "close_same":
                        summary["judgmentUnchanged"] = 1
            except Exception as exc:  # noqa: BLE001 - keep the signal scan result auditable
                summary["errors"] += 1
                errors.append(
                    {
                        "key": "cn_daily_judgment",
                        "name": "A股指数每日判断推送",
                        "error": str(exc),
                    }
                )
                judgment = {
                    "ok": False,
                    "stage": "tail" if phase == "cn_tail" else "close",
                    "reportType": "tail" if phase == "cn_tail" else "close_pending",
                    "reason": "judgment_pipeline_error",
                    "error": str(exc),
                    "deliveryPending": phase == "cn_tail",
                    "verificationPending": phase == "cn_close",
                    "delivered": False,
                }

        finished_at = _now_text()
        result = {
            "ok": not errors,
            "market": market,
            "phase": phase,
            "trigger": trigger,
            "notify": bool(notify),
            "generatedAt": finished_at,
            "marketDate": expected_market_date,
            "summary": summary,
            "retryableDataLag": bool(summary["retryableDataLag"]),
            "items": scan_items,
            "errors": errors,
            "sourceErrors": bundle.get("errors") or [],
            "judgment": judgment,
        }
        task_update: Dict[str, Any] = {
            "ok": result["ok"],
            "running": False,
            "lastRunDate": finished_at[:10],
            "lastRunAt": finished_at,
            "lastRunMarketDate": expected_market_date,
            "lastRunPhase": phase,
            "lastTrigger": trigger,
            "summary": summary,
            "error": "; ".join(str(entry.get("error") or "") for entry in errors) if errors else "",
            "stale": bool(summary["stale"]),
            "retryableDataLag": bool(summary["retryableDataLag"]),
        }
        if str(trigger or "").strip().lower().startswith("scheduled-"):
            task_update.update(
                {
                    "lastScheduledMarketDate": expected_market_date,
                    "lastScheduledPhase": phase,
                    "lastScheduledRunAt": finished_at,
                    "lastScheduledTrigger": trigger,
                }
            )
        if scheduled_cn_judgment:
            task_update.update(
                {
                    "judgment": judgment,
                    "judgmentDeliveryPending": bool((judgment or {}).get("deliveryPending")),
                    "judgmentVerificationPending": bool((judgment or {}).get("verificationPending")),
                    "judgmentWindowExpired": False,
                }
            )
        _save_automation_task(task_key, task_update)
        if (
            scheduled_cn_judgment
            and phase == "cn_close"
            and (judgment or {}).get("reportType") == "close_fallback"
            and (judgment or {}).get("delivered") is True
        ):
            _resolve_cn_tail_after_close_fallback(finished_at)
        return result
    except Exception as exc:  # noqa: BLE001
        failed_at = _now_text()
        scheduled_judgment_failure = _is_scheduled_cn_judgment_run(
            market=market,
            phase=phase,
            trigger=trigger,
        )
        failed_market_date = _index_market_now(market).date().isoformat()
        failure_judgment = (
            {
                "ok": False,
                "stage": "tail" if phase == "cn_tail" else "close",
                "reportType": "tail" if phase == "cn_tail" else "close_pending",
                "reason": "scan_failed_before_judgment",
                "error": str(exc),
                "deliveryPending": phase == "cn_tail",
                "verificationPending": phase == "cn_close",
                "delivered": False,
            }
            if scheduled_judgment_failure
            else None
        )
        payload = {
            "ok": False,
            "running": False,
            "market": market,
            "phase": phase,
            "trigger": trigger,
            "generatedAt": failed_at,
            "error": str(exc),
            "judgment": failure_judgment,
        }
        failure_update: Dict[str, Any] = {
            "ok": False,
            "running": False,
            "lastRunDate": failed_at[:10],
            "lastRunAt": failed_at,
            "lastRunMarketDate": failed_market_date,
            "lastRunPhase": phase,
            "lastTrigger": trigger,
            "summary": "指数顶底信号扫描失败",
            "error": str(exc),
            "stale": bool(market == "US" and phase == "us_close"),
            "retryableDataLag": False,
        }
        if str(trigger or "").strip().lower().startswith("scheduled-"):
            failure_update.update(
                {
                    "lastScheduledMarketDate": failed_market_date,
                    "lastScheduledPhase": phase,
                    "lastScheduledRunAt": failed_at,
                    "lastScheduledTrigger": trigger,
                }
            )
        if scheduled_judgment_failure:
            failure_update.update(
                {
                    "judgment": failure_judgment,
                    "judgmentDeliveryPending": bool((failure_judgment or {}).get("deliveryPending")),
                    "judgmentVerificationPending": bool((failure_judgment or {}).get("verificationPending")),
                    "judgmentWindowExpired": False,
                }
            )
        _save_automation_task(task_key, failure_update)
        return payload
    finally:
        _INDEX_SIGNAL_SCAN_LOCK.release()


def _payload_states_with_current_observation_quality(
    states: Dict[str, Any],
    observations: Dict[str, Any],
) -> Dict[str, Any]:
    """Return audit states annotated with the newest non-durable frame quality.

    States are intentionally durable: an unfinished source frame must not erase
    a prior warning.  The API therefore decorates a copy of each state rather
    than changing the store snapshot, so a current-state consumer can see that
    the latest observation is decision-ineligible without losing the historical
    warning itself.
    """

    payload_states: Dict[str, Any] = {}
    for state_key, raw_state in states.items():
        if not isinstance(raw_state, dict):
            payload_states[state_key] = copy.deepcopy(raw_state)
            continue

        state = copy.deepcopy(raw_state)
        index_key = str(state.get("indexKey") or "").strip()
        timeframe = str(state.get("timeframe") or "").strip().lower()
        if not index_key or timeframe not in TIMEFRAME_IDS:
            # Store-created states always carry these fields, but preserve a
            # legacy/audit entry unchanged when they are absent.
            payload_states[state_key] = state
            continue

        observation = observations.get(f"{index_key}|{timeframe}")
        if isinstance(observation, dict):
            quality = observation.get("dataQuality")
            if isinstance(quality, dict):
                current_quality = copy.deepcopy(quality)
                state["currentDataQuality"] = current_quality
                explicit_eligibility = current_quality.get("stateEligible")
                if isinstance(explicit_eligibility, bool):
                    state["currentStateEligible"] = explicit_eligibility
                else:
                    current_status = str(
                        current_quality.get("status") or observation.get("availability") or ""
                    ).strip().lower()
                    if current_status in INDEX_SIGNAL_QUALITY_NONELIGIBLE:
                        state["currentStateEligible"] = False
        payload_states[state_key] = state
    return payload_states


def _current_index_signal_records(records: Any) -> Dict[str, Any]:
    if not isinstance(records, dict):
        return {}
    current_keys = INDEX_SIGNAL_ASSET_METADATA.keys()
    return {
        str(record_key): record
        for record_key, record in records.items()
        if isinstance(record, dict)
        and str(record.get("indexKey") or str(record_key).split("|", 1)[0]).strip() in current_keys
    }


def get_index_signal_payload() -> Dict[str, Any]:
    snapshot = _INDEX_SIGNAL_STORE.snapshot()
    stored_states = _current_index_signal_records(snapshot.get("states"))
    observations = _current_index_signal_records(snapshot.get("observations"))
    states = _payload_states_with_current_observation_quality(stored_states, observations)
    events = snapshot.get("events") if isinstance(snapshot.get("events"), list) else []
    summary = {
        "topWarning": 0,
        "topConfirmed": 0,
        "bottomWarning": 0,
        "bottomConfirmed": 0,
        "invalidated": 0,
        "expired": 0,
        "conflict": 0,
        "dataErrors": 0,
        "dataPending": 0,
    }
    for state in states.values():
        if not isinstance(state, dict):
            continue
        status = str(state.get("status") or "none").lower()
        direction = str(state.get("direction") or "").lower()
        if status == "warning" and direction == "top":
            summary["topWarning"] += 1
        elif status == "confirmed" and direction == "top":
            summary["topConfirmed"] += 1
        elif status == "warning" and direction == "bottom":
            summary["bottomWarning"] += 1
        elif status == "confirmed" and direction == "bottom":
            summary["bottomConfirmed"] += 1
        elif status == "invalidated":
            summary["invalidated"] += 1
        elif status == "expired":
            summary["expired"] += 1
        elif status == "conflict" or str(state.get("candidateStatus") or "") == "conflict":
            summary["conflict"] += 1
    summary["conflict"] = sum(
        1
        for observation in observations.values()
        if isinstance(observation, dict) and str(observation.get("status") or "") == "conflict"
    )

    automation = _read_json_file(AUTOMATION_STATE)
    automation_tasks = automation.get("tasks") if isinstance(automation.get("tasks"), dict) else {}
    index_tasks = {
        key: value
        for key, value in automation_tasks.items()
        if str(key).startswith("indexSignal") and isinstance(value, dict)
    }
    summary["dataErrors"] = sum(
        int((task.get("summary") or {}).get("errors") or 0)
        for task in index_tasks.values()
        if isinstance(task.get("summary"), dict)
    )
    summary["dataPending"] = sum(
        int((task.get("summary") or {}).get("dataPending") or 0)
        for task in index_tasks.values()
        if isinstance(task.get("summary"), dict)
    )
    recent_events = [_enrich_index_signal_event(event) for event in reversed(events[-20:]) if isinstance(event, dict)]
    transition_latest_push = next(
        (
            event.get("notificationResult")
            for event in reversed(events)
            if isinstance(event, dict)
            and isinstance(event.get("notificationResult"), dict)
            and event["notificationResult"].get("ok") is True
            and event["notificationResult"].get("skipped") is not True
        ),
        None,
    )
    daily_latest_push = _INDEX_SIGNAL_DAILY_PUSH_STORE.latest_delivery()
    push_candidates = [
        candidate
        for candidate in (transition_latest_push, daily_latest_push)
        if isinstance(candidate, dict)
    ]
    latest_push = max(
        push_candidates,
        key=lambda candidate: str(candidate.get("notifiedAt") or ""),
        default=None,
    )
    notification_settings = load_index_signal_notification_settings(mask_webhook=True)
    return {
        "ok": True,
        "generatedAt": _now_text(),
        "rules": index_signal_rules(),
        "summary": summary,
        "states": states,
        "observations": observations,
        "events": recent_events,
        "automation": index_tasks,
        "dailyActivity": _index_signal_daily_activity(index_tasks),
        "notification": {
            "enabled": bool(notification_settings.get("indexSignalDingTalkEnabled")),
            "webhookConfigured": bool(notification_settings.get("webhookConfigured")),
            "processAllowsNotifications": _INDEX_SIGNAL_SCHEDULER_ALLOW_NOTIFICATIONS,
            "latestPush": latest_push,
        },
        "latestPush": latest_push,
        "latestTest": dict(_INDEX_SIGNAL_LAST_TEST),
    }


def get_index_signal_history_payload() -> Dict[str, Any]:
    snapshot = _INDEX_SIGNAL_STORE.snapshot()
    stored_events = snapshot.get("events") if isinstance(snapshot.get("events"), list) else []
    events = [
        _enrich_index_signal_event(event)
        for event in reversed(stored_events)
        if isinstance(event, dict)
    ]
    return {
        "ok": True,
        "generatedAt": _now_text(),
        "total": len(events),
        "retentionLimit": MAX_EVENTS,
        "events": events,
    }


def _build_daily_automation_timeline(schedule: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Only the twelve index jobs; never load unrelated channel settings."""
    market_labels = {"CN": "中国", "HK": "香港", "JP_KR": "日韩", "US": "美国"}
    phase_labels = {
        "cn_tail": "尾盘判断", "us_retry": "日线陈旧复核",
        "morning": "早盘扫描", "midday": "午间扫描", "close": "收盘确认/失效扫描",
    }
    timeline = []
    for (market, phase), task in INDEX_SIGNAL_TASK_KEYS.items():
        label = phase_labels.get(phase, phase_labels.get(phase.rsplit("_", 1)[-1], phase))
        note = "本地扫描；通知需进程开关、页面开关及专属 Webhook"
        if phase == "cn_tail":
            note = "14:55-14:59 交付窗口；未交付时按分钟重试；仍受通知开关约束"
        elif phase == "cn_close":
            note = "15:10-15:15 复核窗口；待复核时按分钟重试"
        timeline.append({"time": schedule.get(task) or INDEX_SIGNAL_LOCAL_TIMES[(market, phase)], "task": f"{market_labels[market]}指数{label}", "key": task, "enabled": True, "note": note})
    return timeline


def _load_automation_state() -> Dict[str, Any]:
    """Only the independent index scheduler's state and timetable."""
    state = _read_json_file(AUTOMATION_STATE)
    tasks = state.get("tasks") if isinstance(state.get("tasks"), dict) else {}
    state["tasks"] = {key: value for key, value in tasks.items() if key in INDEX_SIGNAL_TASK_KEYS.values() and isinstance(value, dict)}
    state.update({"ok": True, "source": "index_monitor", "enabled": _INDEX_SIGNAL_SCHEDULER_STARTED})
    state["schedule"] = {INDEX_SIGNAL_TASK_KEYS[key]: value for key, value in INDEX_SIGNAL_LOCAL_TIMES.items()}
    state["timeline"] = _build_daily_automation_timeline(state["schedule"])
    state["serverTime"] = _now_text()
    return state


def _save_automation_task(task: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    with _AUTOMATION_STATE_LOCK:
        state = _load_automation_state()
        tasks = state.setdefault("tasks", {})
        current = dict(tasks.get(task) if isinstance(tasks.get(task), dict) else {})
        for stale_key in ("skipped", "marketClosed", "marketCalendar", "error", "notificationCount", "stale"):
            if stale_key not in payload:
                current.pop(stale_key, None)
        current.update(payload)
        tasks[task] = current
        state["tasks"] = tasks
        state["updatedAt"] = _now_text()
        _write_json_file(AUTOMATION_STATE, state)
    return _load_automation_state()


def _log_index(message: str) -> None:
    text = f"[INDEX] {datetime.now().strftime('%H:%M:%S')} {message}"
    try:
        print(text, flush=True)
    except UnicodeEncodeError:
        encoding = getattr(sys.stdout, "encoding", None) or "utf-8"
        safe_text = text.encode(encoding, errors="replace").decode(encoding, errors="replace")
        print(safe_text, flush=True)


def _claim_index_signal_schedule_attempt(job: Dict[str, Any]) -> bool:
    task_name = str(job.get("task") or "")
    attempt_token = str(job.get("attemptMinute") or "")
    if not task_name or not attempt_token:
        return True
    if _INDEX_SIGNAL_LAST_SCHEDULE_ATTEMPT.get(task_name) == attempt_token:
        return False
    _INDEX_SIGNAL_LAST_SCHEDULE_ATTEMPT[task_name] = attempt_token
    return True


def _index_signal_scheduler_loop() -> None:
    while not _INDEX_SIGNAL_SCHEDULER_STOP.is_set():
        try:
            raw_state = _read_json_file(AUTOMATION_STATE)
            tasks = raw_state.get("tasks") if isinstance(raw_state.get("tasks"), dict) else {}
            now_utc = datetime.now(timezone.utc)
            for expired in _expired_cn_judgment_windows(now_utc, tasks):
                _save_automation_task(str(expired["task"]), dict(expired["updates"]))
                tasks[str(expired["task"])] = dict(expired["updates"])
            jobs = _due_index_signal_jobs(now_utc, tasks)
            for job in jobs:
                if _index_market_now(str(job["market"])).year != 2026:
                    continue
                if not _claim_index_signal_schedule_attempt(job):
                    continue
                _log_index(
                    f"触发指数信号扫描 market={job['market']} phase={job['phase']} "
                    f"marketDate={job['marketDate']}"
                )
                # Run sequentially: 10:05 CN/HK jobs cannot race on shared state,
                # and this avoids spawning a burst of heavyweight workers.
                _run_index_signal_scan(
                    market=str(job["market"]),
                    phase=str(job["phase"]),
                    trigger=f"scheduled-{job['localTime']}",
                    notify=_INDEX_SIGNAL_SCHEDULER_ALLOW_NOTIFICATIONS,
                )
        except Exception as exc:  # noqa: BLE001
            _log_index(f"指数信号调度异常：{exc}")
        _INDEX_SIGNAL_SCHEDULER_STOP.wait(10)


def ensure_index_signal_scheduler(*, enabled: bool = True, allow_notifications: bool = False) -> bool:
    """Explicitly start scanning; outbound delivery additionally needs opt-in settings."""
    global _INDEX_SIGNAL_SCHEDULER_STARTED, _INDEX_SIGNAL_SCHEDULER_ALLOW_NOTIFICATIONS
    with _INDEX_SIGNAL_SCHEDULER_LOCK:
        _INDEX_SIGNAL_SCHEDULER_ALLOW_NOTIFICATIONS = bool(allow_notifications)
        if not enabled:
            return False
        if _INDEX_SIGNAL_SCHEDULER_STARTED:
            return True
        _INDEX_SIGNAL_SCHEDULER_STOP.clear()
        thread = threading.Thread(target=_index_signal_scheduler_loop, name="index-signal-scheduler", daemon=True)
        thread.start()
        _INDEX_SIGNAL_SCHEDULER_STARTED = True
        _log_index("指数信号调度已启用；通知须同时满足进程开关、页面开关和专属 Webhook 配置")
        return True



def stop_index_signal_scheduler() -> None:
    """Signal the daemon to stop; an in-progress read-only scan may finish."""
    _INDEX_SIGNAL_SCHEDULER_STOP.set()


def test_index_signal_notification(settings: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """Explicit user action only; no call is made during import or startup."""
    if not _INDEX_SIGNAL_SCHEDULER_ALLOW_NOTIFICATIONS:
        return {"ok": False, "skipped": True, "blocked": True, "error": "本进程未启用 --allow-notifications，未发送测试消息"}
    current = load_index_signal_notification_settings()
    if isinstance(settings, dict):
        current.update({key: value for key, value in settings.items() if key in INDEX_SIGNAL_NOTIFICATION_DEFAULTS and key != "indexSignalDingTalkWebhook"})
        if "indexSignalDingTalkWebhook" in settings:
            submitted = str(settings.get("indexSignalDingTalkWebhook") or "").strip()
            if submitted != INDEX_SIGNAL_WEBHOOK_MASK:
                current["indexSignalDingTalkWebhook"] = submitted
    result = test_index_signal_dingtalk_webhook(current)
    _INDEX_SIGNAL_LAST_TEST.clear()
    _INDEX_SIGNAL_LAST_TEST.update(result)
    return result
