"""Standalone extraction of the original index-only workflow."""
from __future__ import annotations
import json
import os
import urllib.error
import urllib.request
from urllib.parse import parse_qsl, urlsplit
from datetime import datetime
from typing import Any, Dict, List

def _external_notification_block_reason() -> str:
    """Fail closed when code is running under tests or an explicit safety stop."""

    if str(os.environ.get("PYTEST_CURRENT_TEST") or "").strip():
        return "pytest 环境禁止外发通知"
    disabled = str(os.environ.get("INDEX_MONITOR_DISABLE_NOTIFICATIONS") or "").strip().lower()
    if disabled in {"1", "true", "yes", "on"}:
        return "外发通知已被安全开关禁用"
    return ""


def _fmt(value: Any, digits: int = 2, suffix: str = "") -> str:
    try:
        if value is None:
            return "--"
        return f"{float(value):.{digits}f}{suffix}"
    except Exception:
        return "--"


def validate_index_webhook(webhook: str) -> str:
    """Only the dedicated DingTalk HTTPS endpoint is a permitted destination."""
    text = str(webhook or "").strip()
    try:
        parsed = urlsplit(text)
        pairs = parse_qsl(parsed.query, keep_blank_values=True, max_num_fields=8)
        tokens = [value for key, value in pairs if key == "access_token"]
        valid = (
            parsed.scheme == "https"
            and parsed.netloc == "oapi.dingtalk.com"
            and parsed.path == "/robot/send"
            and not parsed.fragment
            and len(tokens) == 1
            and bool(tokens[0].strip())
            and all(key in {"access_token", "timestamp", "sign"} for key, _ in pairs)
            and all(ord(char) >= 33 for char in text)
        )
    except (TypeError, ValueError):
        valid = False
    if not valid:
        raise ValueError("Webhook 必须是 https://oapi.dingtalk.com/robot/send 且包含 access_token")
    return text


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _send_dingtalk(payload: Dict[str, Any], webhook: str) -> Dict[str, Any]:
    blocked_reason = _external_notification_block_reason()
    if blocked_reason:
        return {"ok": False, "skipped": True, "blocked": True, "error": blocked_reason}
    try:
        webhook = validate_index_webhook(webhook)
        body = json.dumps(payload, ensure_ascii=False, allow_nan=False).encode("utf-8")
        # Construct the request inside the protected boundary: malformed URLs
        # can otherwise echo their query credentials in ValueError text.
        req = urllib.request.Request(
            webhook,
            data=body,
            headers={"Content-Type": "application/json; charset=utf-8"},
            method="POST",
        )
        # Retain normal TLS verification/system proxies, but never redirect
        # the webhook's credentials or message to another destination.
        opener = urllib.request.build_opener(_NoRedirect())
        with opener.open(req, timeout=8) as resp:
            raw_body = resp.read(65537)
        if len(raw_body) > 65536:
            return {"ok": False, "error": "钉钉响应超过大小限制"}
        raw = raw_body.decode("utf-8")
        data = json.loads(raw) if raw else {}
        if not isinstance(data, dict):
            return {"ok": False, "error": "钉钉响应格式无效"}
        if "errcode" not in data:
            return {"ok": False, "error": "钉钉响应缺少明确确认码"}
        errcode = data.get("errcode")
        if isinstance(errcode, bool) or not isinstance(errcode, int):
            return {"ok": False, "error": "钉钉确认码格式无效"}
        if errcode != 0:
            return {"ok": False, "error": f"钉钉接口拒绝消息（errcode={errcode}）", "response": {"errcode": errcode}}
        return {"ok": True, "response": {"errcode": errcode}}
    except urllib.error.HTTPError as exc:
        return {"ok": False, "error": f"钉钉 HTTP 请求失败（status={exc.code}）"}
    except ValueError:
        return {"ok": False, "error": "Webhook 或钉钉响应格式无效"}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"钉钉传输失败（{exc.__class__.__name__}）"}


def _index_signal_setting_enabled(settings: Dict[str, Any] | None, key: str, default: bool) -> bool:
    if not isinstance(settings, dict) or key not in settings:
        return default
    value = settings.get(key)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"0", "false", "no", "off", "disabled"}:
            return False
        if text in {"1", "true", "yes", "on", "enabled"}:
            return True
    return bool(value)


def _index_signal_webhook_from_settings(
    settings: Dict[str, Any] | None,
    *,
    require_enabled: bool,
) -> str:
    """Resolve only the dedicated index-signal webhook.

    Index transitions must never reuse the general stock or theme robots.
    Explicit test calls may use a configured webhook while the channel is disabled.
    """

    if not isinstance(settings, dict):
        return ""
    if require_enabled and not _index_signal_setting_enabled(settings, "indexSignalDingTalkEnabled", False):
        return ""
    return str(settings.get("indexSignalDingTalkWebhook") or "").strip()


def _index_signal_status_label(value: Any) -> str:
    text = str(value or "none").strip().lower()
    return {
        "none": "无信号",
        "warning": "预警",
        "confirmed": "确认",
        "invalidated": "失效",
        "expired": "过期",
        "conflict": "顶底冲突",
    }.get(text, str(value or "--"))


def _index_signal_timeframe_label(value: Any) -> str:
    text = str(value or "").strip().lower()
    return {"short": "短期·15分钟", "medium": "中期·60分钟", "long": "长期·日线"}.get(text, str(value or "--"))


def _index_signal_scan_phase_label(value: Any) -> str:
    text = str(value or "").strip().lower()
    return {
        "cn_morning": "A股早盘",
        "cn_midday": "A股午间",
        "cn_tail": "A股尾盘判断",
        "cn_close": "A股收盘",
        "hk_morning": "港股早盘",
        "hk_midday": "港股午间",
        "hk_close": "港股收盘",
        "jp_kr_morning": "日韩早盘",
        "jp_kr_midday": "日韩午间",
        "jp_kr_close": "日韩收盘",
        "us_close": "美股收盘",
        "us_retry": "美股收盘复核",
    }.get(text, str(value or "--"))


def _index_signal_direction_label(value: Any) -> str:
    text = str(value or "").strip().lower()
    return {"top": "顶部", "bottom": "底部"}.get(text, str(value or "--"))


def _index_signal_metric(event: Dict[str, Any], key: str) -> Any:
    metrics = event.get("metrics") if isinstance(event.get("metrics"), dict) else {}
    return event.get(key) if event.get(key) is not None else metrics.get(key)


def _index_signal_target_text(value: Any) -> str:
    if isinstance(value, (list, tuple)) and len(value) >= 2:
        return f"{_fmt(value[0])} - {_fmt(value[1])}"
    if isinstance(value, dict):
        lower = value.get("lower") if value.get("lower") is not None else value.get("min")
        upper = value.get("upper") if value.get("upper") is not None else value.get("max")
        if lower is not None or upper is not None:
            return f"{_fmt(lower)} - {_fmt(upper)}"
    if value in (None, ""):
        return "--"
    return str(value)


def _index_signal_evidence_lines(evidence: Any) -> List[str]:
    rows = evidence if isinstance(evidence, list) else []
    if not rows:
        return ["- 暂无结构化证据"]
    lines: List[str] = []
    for row in rows:
        if isinstance(row, dict):
            label = str(row.get("label") or row.get("category") or "证据").strip()
            value = row.get("value")
            detail = row.get("detail") or row.get("description")
            suffix = detail if detail not in (None, "") else value
            lines.append(f"- **{label}**：{suffix if suffix not in (None, '') else '--'}")
        else:
            lines.append(f"- {row}")
    return lines


def _index_signal_redact_webhook(value: Any, webhook: str) -> Any:
    if isinstance(value, str):
        return value.replace(webhook, "[REDACTED]") if webhook else value
    if isinstance(value, dict):
        return {key: _index_signal_redact_webhook(item, webhook) for key, item in value.items()}
    if isinstance(value, list):
        return [_index_signal_redact_webhook(item, webhook) for item in value]
    if isinstance(value, tuple):
        return tuple(_index_signal_redact_webhook(item, webhook) for item in value)
    return value


def _send_index_signal_dingtalk(payload: Dict[str, Any], webhook: str) -> Dict[str, Any]:
    try:
        result = _send_dingtalk(payload, webhook)
    except Exception as exc:  # noqa: BLE001
        result = {"ok": False, "error": f"通知发送失败（{type(exc).__name__}）"}
    if not isinstance(result, dict):
        result = {"ok": False, "error": "钉钉发送器返回了无效结果"}
    return _index_signal_redact_webhook(result, webhook)


def notify_index_signal_transition(
    event: Dict[str, Any],
    *,
    settings: Dict[str, Any],
    trigger: str,
) -> Dict[str, Any]:
    """Format and send one already-deduplicated index state transition."""

    event = event if isinstance(event, dict) else {}
    event_id = str(event.get("id") or event.get("eventId") or "")
    to_status = str(event.get("toStatus") or event.get("status") or "").strip().lower()
    timeframe = str(event.get("timeframe") or "").strip().lower()

    if event.get("shouldNotify") is False:
        return {"ok": True, "skipped": True, "eventId": event_id, "message": "事件已由去重状态标记为无需推送"}
    # Expiry is an audit/lifecycle transition, never a trade-like alert.  Do
    # this before resolving a webhook so an unavailable channel cannot turn a
    # correctly suppressed event into a delivery failure.
    if to_status not in {"warning", "confirmed", "invalidated"}:
        return {"ok": True, "skipped": True, "eventId": event_id, "message": "该状态不属于可推送转换"}
    webhook = _index_signal_webhook_from_settings(settings, require_enabled=True)
    if not webhook:
        return {"ok": False, "skipped": True, "eventId": event_id, "error": "指数信号钉钉未启用或 Webhook 为空"}
    timeframe_toggle = {
        "short": "shortSignalsEnabled",
        "medium": "mediumSignalsEnabled",
        "long": "longSignalsEnabled",
    }.get(timeframe)
    if timeframe_toggle and not _index_signal_setting_enabled(settings, timeframe_toggle, True):
        return {"ok": True, "skipped": True, "eventId": event_id, "message": f"{_index_signal_timeframe_label(timeframe)}通知已关闭"}
    status_toggle = {
        "warning": "warningEnabled",
        "confirmed": "confirmedEnabled",
        "invalidated": "invalidatedEnabled",
    }.get(to_status)
    if status_toggle and not _index_signal_setting_enabled(settings, status_toggle, True):
        return {"ok": True, "skipped": True, "eventId": event_id, "message": f"{_index_signal_status_label(to_status)}通知已关闭"}

    index_key = str(event.get("indexKey") or event.get("index_key") or "--")
    index_name = str(event.get("indexName") or event.get("name") or index_key)
    market = str(event.get("market") or event.get("region") or "--")
    direction = str(event.get("direction") or "").strip().lower()
    direction_label = _index_signal_direction_label(direction)
    timeframe_label = _index_signal_timeframe_label(timeframe)
    status_label = _index_signal_status_label(to_status)
    from_label = _index_signal_status_label(event.get("fromStatus") or event.get("previousStatus"))
    changed_at = str(event.get("changedAt") or event.get("scanTime") or event.get("generatedAt") or datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    scan_phase = str(event.get("scanPhase") or event.get("phase") or "--")
    scan_phase_key = scan_phase.strip().lower()
    scan_phase_label = _index_signal_scan_phase_label(scan_phase)
    scan_phase_display = (
        scan_phase_label
        if scan_phase_label == scan_phase
        else f"{scan_phase_label}（{scan_phase}）"
    )
    close_phase = scan_phase_key == "close" or scan_phase_key.endswith("_close")
    price = event.get("price")
    if price is None:
        price = event.get("currentPrice") if event.get("currentPrice") is not None else event.get("latestPrice")
    key_level = event.get("keyLevel")
    invalidation_level = event.get("invalidationLevel")
    structure = str(event.get("structureSummary") or event.get("structure") or event.get("pattern") or "--")
    pressure_support = str(event.get("pressureSupport") or event.get("resistanceSupport") or "--")
    repair_level = event.get("repairLevel")
    invalidation_condition = str(
        event.get("invalidationCondition")
        or event.get("invalidationReason")
        or ("有效站回失效位上方" if direction == "top" else "有效跌破失效位")
    )
    next_scan = str(event.get("nextScanAt") or event.get("nextScan") or "按对应市场下一计划扫描时点")
    source = str(event.get("source") or event.get("sourceName") or event.get("dataSource") or "指数行情数据源（见扫描结果）")
    dedupe_text = str(
        event.get("dedupeText")
        or "同一指数、周期、方向、状态同一交易日只推送一次；评分或证据变化但状态不变时不重复推送"
    )
    priority = {"warning": "⚠️", "confirmed": "🚨", "invalidated": "ℹ️"}[to_status]
    title = f"{priority} 指数{direction_label}{status_label}：{index_name}（{timeframe_label}）"
    lines = [
        f"### {title}",
        "",
        f"- **市场 / 指数**：{market} / {index_name}（{index_key}）",
        f"- **周期 / 方向 / 当前阶段**：{timeframe_label} / {direction_label} / {status_label}",
        f"- **状态变化**：{from_label} → {status_label}",
        f"- **评分**：{event.get('score') if event.get('score') is not None else '--'}",
        f"- **扫描阶段/时间**：{scan_phase_display} / {changed_at}",
        f"- **触发方式**：{trigger or '--'}",
        "",
        "#### 行情与关键结构",
        f"- **最新价格**：{_fmt(price)}",
        f"- **MA20 / MA60**：{_fmt(_index_signal_metric(event, 'ma20'))} / {_fmt(_index_signal_metric(event, 'ma60'))}",
        f"- **关键位**：{_fmt(key_level)}",
        f"- **结构判断**：{structure}",
        f"- **反压/支撑**：{pressure_support}",
        f"- **修复位**：{_fmt(repair_level)}",
        f"- **失效位**：{_fmt(invalidation_level)}",
        f"- **失效条件**：{invalidation_condition}",
        f"- **目标**：{_index_signal_target_text(event.get('targetRange') or event.get('target'))}",
        "",
        "#### 触发证据",
        *_index_signal_evidence_lines(event.get("evidence")),
        "",
        f"- **下次扫描**：{next_scan}",
        f"- **来源**：{source}",
        f"- **去重说明**：{dedupe_text}",
    ]
    if to_status == "warning":
        stage_note = (
            "收盘预警：本次已按收盘数据评估，当前仍为预警，未升级为确认"
            if close_phase
            else "盘中预警：尚未进入收盘确认"
        )
        lines.insert(8, f"- **阶段说明**：{stage_note}")
    elif to_status == "invalidated":
        lines.insert(8, f"- **失效说明**：{invalidation_condition}")

    result = _send_index_signal_dingtalk(
        {
            "msgtype": "markdown",
            "markdown": {"title": title, "text": "\n".join(lines)},
            "at": {"isAtAll": False},
        },
        webhook,
    )
    notified_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return {
        "eventId": event_id,
        "trigger": str(trigger or ""),
        "status": to_status,
        "notifiedAt": notified_at,
        **result,
    }


_INDEX_SIGNAL_JUDGMENT_REPORTS = {
    "tail": ("14:55尾盘判断", "14:55尾盘判断是收盘前快照；日线仅使用已完成K线，15:10后复核。"),
    "close_changed": ("15:10收盘修正", "15:10收盘判断与14:55尾盘判断存在差异，本次为收盘修正。"),
    "close_fallback": ("15:10尾盘未送达兜底", "14:55尾盘判断未成功送达，本次补发完整收盘判断。"),
}


_INDEX_SIGNAL_JUDGMENT_TIMEFRAMES = ("short", "medium", "long")


_INDEX_SIGNAL_JUDGMENT_ACTIVE_STATUSES = frozenset({"warning", "confirmed"})


_INDEX_SIGNAL_PATTERN_LABELS = {
    "double_top": "双顶",
    "double_bottom": "双底",
    "head_shoulders_top": "头肩顶",
    "head_shoulders_bottom": "头肩底",
    "triple_top": "三重顶",
    "triple_bottom": "三重底",
    "v_top": "V形顶部",
    "v_bottom": "V形底部",
    "bear_flag": "熊旗（下破）",
    "bull_flag": "牛旗（上破）",
    "rising_wedge": "上升楔形（下破）",
    "falling_wedge": "下降楔形（上破）",
    "bear_channel_break": "上行通道下破",
    "bull_channel_break": "下行通道上破",
    "rounded_bottom": "圆弧底",
}


def _index_signal_judgment_status(frame: Dict[str, Any]) -> str:
    return str(frame.get("status") or frame.get("candidateStatus") or "none").strip().lower()


def _index_signal_judgment_frame_is_active(frame: Any) -> bool:
    return isinstance(frame, dict) and _index_signal_judgment_status(frame) in _INDEX_SIGNAL_JUDGMENT_ACTIVE_STATUSES


def _index_signal_judgment_change_is_relevant(change: Dict[str, Any]) -> bool:
    before = change.get("before") if isinstance(change.get("before"), dict) else {}
    after = change.get("after") if isinstance(change.get("after"), dict) else {}
    # Keep active -> inactive changes so a close correction can explicitly
    # withdraw a warning sent in the tail report.  Ignore transitions wholly
    # inside none/conflict/invalidated/expired audit states.
    return _index_signal_judgment_frame_is_active(before) or _index_signal_judgment_frame_is_active(after)


def _index_signal_judgment_timeframes(
    snapshot: Dict[str, Any],
    settings: Dict[str, Any] | None,
) -> List[str]:
    declared = snapshot.get("enabledTimeframes")
    if isinstance(declared, (list, tuple, set)):
        selected = {
            str(value or "").strip().lower()
            for value in declared
            if str(value or "").strip().lower() in _INDEX_SIGNAL_JUDGMENT_TIMEFRAMES
        }
    else:
        selected = set(_INDEX_SIGNAL_JUDGMENT_TIMEFRAMES)
    toggles = {
        "short": "shortSignalsEnabled",
        "medium": "mediumSignalsEnabled",
        "long": "longSignalsEnabled",
    }
    return [
        timeframe
        for timeframe in _INDEX_SIGNAL_JUDGMENT_TIMEFRAMES
        if timeframe in selected
        and _index_signal_setting_enabled(settings, toggles[timeframe], True)
    ]


def _index_signal_judgment_state_text(frame: Dict[str, Any]) -> str:
    status = _index_signal_judgment_status(frame)
    direction = str(frame.get("direction") or "").strip().lower()
    status_label = _index_signal_status_label(status)
    if direction in {"top", "bottom"} and status not in {"none", "conflict"}:
        status_label = f"{_index_signal_direction_label(direction)}{status_label}"
    if status not in _INDEX_SIGNAL_JUDGMENT_ACTIVE_STATUSES:
        return status_label

    details = [status_label]
    score = frame.get("score")
    if score is not None:
        details.append(f"{score}分")
    pattern = str(frame.get("patternId") or frame.get("pattern") or "").strip()
    if pattern:
        details.append(f"结构 {_INDEX_SIGNAL_PATTERN_LABELS.get(pattern, pattern)}")
    if frame.get("keyLevel") is not None:
        details.append(f"关键位 {_fmt(frame.get('keyLevel'))}")
    if frame.get("invalidationLevel") is not None:
        details.append(f"失效位 {_fmt(frame.get('invalidationLevel'))}")
    target_text = _index_signal_target_text(frame.get("targetRange"))
    if target_text != "--":
        details.append(f"目标 {target_text}")
    return "，".join(details)


def _index_signal_judgment_change_text(change: Dict[str, Any]) -> str:
    index_key = str(change.get("indexKey") or "--")
    index_name = str(change.get("indexName") or index_key)
    timeframe = str(change.get("timeframe") or "")
    before = change.get("before") if isinstance(change.get("before"), dict) else {}
    after = change.get("after") if isinstance(change.get("after"), dict) else {}
    return (
        f"- **{index_name}（{index_key}） / {_index_signal_timeframe_label(timeframe)}**："
        f"{_index_signal_judgment_state_text(before)} → {_index_signal_judgment_state_text(after)}"
    )


def _index_signal_judgment_metadata_change_text(change: Dict[str, Any]) -> str:
    field = str(change.get("field") or "--")
    field_label = {
        "fingerprintVersion": "指纹口径版本",
        "ruleVersion": "规则版本",
        "market": "市场",
        "enabledTimeframes": "启用周期",
    }.get(field, field)

    def display(value: Any) -> str:
        if field == "enabledTimeframes" and isinstance(value, list):
            return " / ".join(_index_signal_timeframe_label(item) for item in value) or "--"
        return str(value) if value not in (None, "") else "--"

    return f"- **{field_label}**：{display(change.get('before'))} → {display(change.get('after'))}"


def notify_index_signal_judgment(
    snapshot: Dict[str, Any],
    settings: Dict[str, Any],
    trigger: str,
    report_type: str,
    comparison: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Send one aggregate CN index judgment through the dedicated channel.

    Delivery de-duplication is owned by the daily-push store.  This formatter
    deliberately ignores per-status transition toggles, but only renders
    warning/confirmed frames.  Inactive audit states remain in the durable
    snapshot for coverage and comparison and never bloat operator messages.
    """

    snapshot = snapshot if isinstance(snapshot, dict) else {}
    normalized_report_type = str(report_type or "").strip().lower()
    report_contract = _INDEX_SIGNAL_JUDGMENT_REPORTS.get(normalized_report_type)
    market = str(snapshot.get("market") or "CN").strip().upper() or "CN"
    market_date = str(snapshot.get("marketDate") or "").strip()
    generated_at = str(
        snapshot.get("generatedAt") or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )
    result_base = {
        "reportType": normalized_report_type,
        "market": market,
        "marketDate": market_date,
        "trigger": str(trigger or ""),
        "fingerprint": str(snapshot.get("fingerprint") or ""),
    }
    if report_contract is None:
        return {
            "ok": False,
            "skipped": True,
            "error": f"不支持的指数判断报告类型：{normalized_report_type or '--'}",
            **result_base,
        }

    webhook = _index_signal_webhook_from_settings(settings, require_enabled=True)
    if not webhook:
        return {
            "ok": False,
            "skipped": True,
            "error": "指数信号钉钉未启用或 Webhook 为空",
            **result_base,
        }
    enabled_timeframes = _index_signal_judgment_timeframes(snapshot, settings)
    if not enabled_timeframes:
        return {
            "ok": True,
            "skipped": True,
            "message": "指数判断报告没有已启用周期",
            **result_base,
        }

    report_label, stage_note = report_contract
    title = f"指数{report_label}：{market} {market_date or generated_at[:10]}"
    snapshot_items = snapshot.get("items") if isinstance(snapshot.get("items"), list) else []
    try:
        expected_item_count = int(snapshot.get("expectedItemCount"))
    except (TypeError, ValueError):
        expected_item_count = len(snapshot_items)
    coverage_text = f"{len(snapshot_items)} / {expected_item_count}"
    if len(snapshot_items) != expected_item_count:
        coverage_text += "（数据不完整）"
    active_items: List[tuple[Dict[str, Any], List[tuple[str, Dict[str, Any]]]]] = []
    for item in snapshot_items:
        if not isinstance(item, dict):
            continue
        frames = item.get("timeframes") if isinstance(item.get("timeframes"), dict) else {}
        active_frames = [
            (timeframe, frames[timeframe])
            for timeframe in enabled_timeframes
            if _index_signal_judgment_frame_is_active(frames.get(timeframe))
        ]
        if active_frames:
            active_items.append((item, active_frames))
    active_frame_count = sum(len(frames) for _, frames in active_items)
    lines = [
        f"### {title}",
        "",
        f"- **市场 / 交易日**：{market} / {market_date or '--'}",
        f"- **判断时间**：{generated_at}",
        f"- **触发方式**：{trigger or '--'}",
        f"- **已选周期**：{' / '.join(_index_signal_timeframe_label(value) for value in enabled_timeframes)}",
        f"- **有效信号**：{len(active_items)} 个指数 / {active_frame_count} 个周期",
        f"- **扫描覆盖**：{coverage_text}",
        f"- **阶段说明**：{stage_note}",
        "",
    ]

    if normalized_report_type == "close_changed":
        changes = comparison.get("changes") if isinstance(comparison, dict) else None
        metadata_changes = comparison.get("metadataChanges") if isinstance(comparison, dict) else None
        visible_metadata_changes = [
            change
            for change in (metadata_changes if isinstance(metadata_changes, list) else [])
            if isinstance(change, dict)
        ]
        visible_changes = [
            change
            for change in (changes if isinstance(changes, list) else [])
            if isinstance(change, dict)
            and str(change.get("timeframe") or "").strip().lower() in enabled_timeframes
            and _index_signal_judgment_change_is_relevant(change)
        ]
        lines.extend(["#### 相对14:55尾盘判断的有效变化"])
        if visible_metadata_changes:
            lines.extend(
                _index_signal_judgment_metadata_change_text(change)
                for change in visible_metadata_changes
            )
        if visible_changes:
            lines.extend(_index_signal_judgment_change_text(change) for change in visible_changes)
        elif not visible_metadata_changes:
            lines.append("- 有效信号未变化；仅后台非活动状态发生更新。")
        lines.append("")
    elif normalized_report_type == "close_fallback":
        lines.extend(["- **补发原因**：14:55尾盘判断未成功送达，本次发送完整收盘判断。", ""])

    lines.append("#### 当前有效信号")
    for item, visible_frames in active_items:
        index_key = str(item.get("indexKey") or "--")
        index_name = str(item.get("indexName") or index_key)
        lines.append(f"- **{index_name}（{index_key}）**")
        for timeframe, frame in visible_frames:
            lines.append(
                f"  - {_index_signal_timeframe_label(timeframe)}："
                f"{_index_signal_judgment_state_text(frame)}"
            )
    if active_frame_count == 0:
        lines.append("- 当前没有预警或确认信号。")

    result = _send_index_signal_dingtalk(
        {
            "msgtype": "markdown",
            "markdown": {"title": title, "text": "\n".join(lines)},
            "at": {"isAtAll": False},
        },
        webhook,
    )
    notified_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return {"notifiedAt": notified_at, **result_base, **result}


def test_index_signal_dingtalk_webhook(settings: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """Test only the dedicated index-signal webhook without requiring enablement."""

    webhook = _index_signal_webhook_from_settings(settings, require_enabled=False)
    if not webhook:
        return {"ok": False, "error": "指数信号钉钉 Webhook 不能为空"}
    sent_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    result = _send_index_signal_dingtalk(
        {
            "msgtype": "markdown",
            "markdown": {
                "title": "指数信号钉钉 Webhook 测试",
                "text": "\n".join(
                    [
                        "### 指数信号钉钉 Webhook 测试",
                        "",
                        f"- **发送时间**：{sent_at}",
                        "- **来源**：中美主要指数顶部/底部监测",
                        "- **通道说明**：该机器人接收指数状态变化、14:55尾盘判断和15:10收盘差异复核，不复用股票买卖点或题材通知通道。",
                    ]
                ),
            },
            "at": {"isAtAll": False},
        },
        webhook,
    )
    if result.get("ok"):
        return {"message": "指数信号测试消息已发送，请检查钉钉机器人", "sentAt": sent_at, **result}
    return {"sentAt": sent_at, **result}

