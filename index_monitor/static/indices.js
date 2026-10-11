(() => {
  if (location.protocol === "file:") return;
  const state = {
    loaded: false,
    loading: false,
    timer: null,
    marketData: null,
    marketCached: false,
    signalPayload: null,
    signalStates: {},
    settingsLoaded: false,
    settingsLoading: false,
    settingsDirty: false,
  };

  const byId = (id) => document.getElementById(id);
  const groupsEl = byId("indexGroups") || byId("groups");
  const updatedAtEl = byId("indexUpdatedAt") || byId("updatedAt");
  const errorBox = byId("indexErrorBox") || byId("errorBox");
  const refreshBtn = byId("indexRefreshBtn") || byId("refreshBtn");
  const groupTemplate = byId("indexGroupTemplate") || byId("groupTemplate");
  const cardTemplate = byId("indexCardTemplate") || byId("cardTemplate");
  const signalSummaryEl = byId("indexSignalSummary");
  const signalDailyStatusEl = byId("indexSignalDailyStatus");
  const signalNoticeEl = byId("indexSignalNotice");
  const signalRulesEl = byId("indexSignalRules");
  const signalRuleVersionEl = byId("indexSignalRuleVersion");
  const signalWebhookEl = byId("indexSignalWebhook");
  const signalSaveBtn = byId("indexSignalSaveBtn");
  const signalTestBtn = byId("indexSignalTestBtn");
  const signalTestResultEl = byId("indexSignalTestResult");
  const signalPushResultEl = byId("indexSignalPushResult");
  const standalone = Boolean(byId("groups"));
  const INDEX_MARKET_CACHE_KEY = "indexDrawdownMonitor.market.v1";

  if (
    !groupsEl ||
    !updatedAtEl ||
    !errorBox ||
    !refreshBtn ||
    !groupTemplate ||
    !cardTemplate ||
    !signalSummaryEl ||
    !signalDailyStatusEl ||
    !signalWebhookEl ||
    !signalSaveBtn ||
    !signalTestBtn
  ) {
    return;
  }

  const settingControls = {
    indexSignalDingTalkEnabled: byId("indexSignalEnabled"),
    indexSignalDingTalkWebhook: signalWebhookEl,
    shortSignalsEnabled: byId("indexSignalShortEnabled"),
    mediumSignalsEnabled: byId("indexSignalMediumEnabled"),
    longSignalsEnabled: byId("indexSignalLongEnabled"),
    warningEnabled: byId("indexSignalWarningEnabled"),
    confirmedEnabled: byId("indexSignalConfirmedEnabled"),
    invalidatedEnabled: byId("indexSignalInvalidatedEnabled"),
  };

  const DEFAULT_SETTINGS = {
    indexSignalDingTalkEnabled: false,
    indexSignalDingTalkWebhook: "",
    shortSignalsEnabled: true,
    mediumSignalsEnabled: true,
    longSignalsEnabled: true,
    warningEnabled: true,
    confirmedEnabled: true,
    invalidatedEnabled: true,
  };

  const SUMMARY_FIELDS = [
    ["topWarning", "顶部预警"],
    ["topConfirmed", "顶部确认"],
    ["bottomWarning", "底部预警"],
    ["bottomConfirmed", "底部确认"],
    ["invalidated", "信号失效"],
    ["conflict", "震荡冲突"],
    ["dataErrors", "数据异常"],
    ["dataPending", "数据待完成"],
  ];

  const STATUS_DESCRIPTORS = {
    "top:warning": { label: "顶部预警", className: "signal-top-warning" },
    "top:confirmed": { label: "顶部确认", className: "signal-top-confirmed" },
    "bottom:warning": { label: "底部预警", className: "signal-bottom-warning" },
    "bottom:confirmed": { label: "底部确认", className: "signal-bottom-confirmed" },
    invalidated: { label: "信号失效", className: "signal-invalidated" },
    expired: { label: "信号过期", className: "signal-expired" },
    conflict: { label: "震荡冲突", className: "signal-conflict" },
    awaiting: { label: "等待已完成K线", className: "signal-awaiting" },
    not_scanned: { label: "未扫描", className: "signal-not-scanned" },
    stale: { label: "数据过期", className: "signal-stale" },
    unavailable: { label: "数据不可用", className: "signal-unavailable" },
    none: { label: "无信号", className: "signal-none" },
  };

  const LIFECYCLE_LABELS = {
    candidate: "候选",
    resonant: "共振",
    confirmed: "确认",
    invalidated: "失效",
    expired: "过期",
    none: "无信号",
  };
  const QUALITY_LABELS = {
    available: "已完成K线可评估",
    awaiting_completed_bar: "等待已完成K线",
    not_scanned: "未扫描",
    stale: "数据过期",
    unavailable: "数据不可用",
    insufficient: "样本不足",
  };
  const CONFLUENCE_LABELS = {
    resonant: "相邻周期同向共振",
    candidate_only: "候选，尚无相邻共振",
    nonadjacent_alignment_only: "非相邻同向，仅观察不升级",
    cross_timeframe_conflict: "跨周期方向冲突，阻断升级",
    daily_confirmed: "日线收盘确认",
    daily_candidate: "日线候选",
    none: "无跨周期结论",
  };

  function isObject(value) {
    return Boolean(value) && typeof value === "object" && !Array.isArray(value);
  }

  const TIMEFRAME_IDS = ["short", "medium", "long"];
  const TIMEFRAME_LABELS = {
    short: "短期 · 15分钟",
    medium: "中期 · 60分钟",
    long: "长期 · 日线",
  };
  const INTERVAL_LABELS = {
    "15m": "15分钟",
    "60m": "60分钟",
    "1d": "日线",
  };
  const PATTERN_LABELS = {
    double_top: "双顶",
    double_bottom: "双底",
    head_shoulders_top: "头肩顶",
    head_shoulders_bottom: "头肩底",
    triple_top: "三重顶",
    triple_bottom: "三重底",
    v_top: "V形顶部",
    v_bottom: "V形底部",
    bear_flag: "熊旗（下破）",
    bull_flag: "牛旗（上破）",
    rising_wedge: "上升楔形（下破）",
    falling_wedge: "下降楔形（上破）",
    bear_channel_break: "上行通道下破",
    bull_channel_break: "下行通道上破",
    rounded_bottom: "圆弧底",
  };
  const CONFIRMATION_POLICY_LABELS = {
    requires_higher_timeframe: "仅候选预警；60分钟同向时提高共振等级",
    requires_daily_alignment: "仅候选预警；日线同向时提高共振等级",
    market_close_only: "仅在日线收盘后确认",
  };

  function timeframeLabel(timeframe, fallback = "") {
    return TIMEFRAME_LABELS[timeframe] || fallback || String(timeframe || "未命名周期");
  }

  function hasTimeframeSignals(value) {
    return TIMEFRAME_IDS.some((timeframe) => isObject(value?.[timeframe]));
  }

  function isMarketPayload(value) {
    return (
      isObject(value) &&
      value.ok !== false &&
      Array.isArray(value.groups) &&
      value.groups.some((group) => Array.isArray(group?.items) && group.items.length)
    );
  }

  function cacheSafeMarketPayload(data) {
    const payload = JSON.parse(JSON.stringify(data));
    delete payload.cache;
    (payload.groups || []).forEach((group) => {
      (group.items || []).forEach((item) => delete item.signals);
    });
    return payload;
  }

  function saveMarketCache(data) {
    if (!isMarketPayload(data)) return;
    try {
      window.localStorage.setItem(INDEX_MARKET_CACHE_KEY, JSON.stringify({
        savedAt: Date.now(),
        payload: cacheSafeMarketPayload(data),
      }));
    } catch (_err) {
      // Storage may be unavailable in a locked-down WebView; live loading still works.
    }
  }

  function restoreMarketCache() {
    try {
      const raw = window.localStorage.getItem(INDEX_MARKET_CACHE_KEY);
      if (!raw) return false;
      const cached = JSON.parse(raw);
      if (!isObject(cached) || !isMarketPayload(cached.payload)) {
        window.localStorage.removeItem(INDEX_MARKET_CACHE_KEY);
        return false;
      }
      renderMarketData(cached.payload, { cached: true });
      return true;
    } catch (_err) {
      return false;
    }
  }

  function unwrapPayload(payload) {
    if (!isObject(payload)) return {};
    if (!isObject(payload.data)) return payload;
    return {
      ...payload.data,
      ok: payload.ok ?? payload.data.ok,
      message: payload.message ?? payload.data.message,
      error: payload.error ?? payload.data.error,
    };
  }

  function fmt(value, digits = 2) {
    if (value === null || value === undefined || !Number.isFinite(Number(value))) return "--";
    return Number(value).toFixed(digits);
  }

  function fmtSigned(value, digits = 2) {
    if (value === null || value === undefined || !Number.isFinite(Number(value))) return "--";
    const n = Number(value);
    return `${n > 0 ? "+" : ""}${n.toFixed(digits)}`;
  }

  function pctClass(value) {
    const n = Number(value || 0);
    if (n > 0) return "up";
    if (n < 0) return "down";
    return "flat";
  }

  function sourceText(source) {
    if (source === "yahoo") return "Yahoo";
    if (source === "tencent") return "腾讯财经";
    if (source === "wind_web") return "万得指数官网";
    return source || "--";
  }

  function normalizeSignal(rawSignal) {
    const signal = isObject(rawSignal) ? rawSignal : {};
    const direction = signal.direction === "top" || signal.direction === "bottom" ? signal.direction : null;
    const allowedStatuses = new Set([
      "none", "warning", "confirmed", "invalidated", "expired", "conflict", "awaiting", "not_scanned", "stale", "unavailable",
    ]);
    const allowedLifecycles = new Set(["candidate", "resonant", "confirmed", "invalidated", "expired", "none"]);
    const rawStatus = String(signal.status || signal.candidateStatus || "none").trim().toLowerCase();
    const rawLifecycle = String(signal.lifecycleState || "").trim().toLowerCase();
    const quality = isObject(signal.currentDataQuality)
      ? signal.currentDataQuality
      : (isObject(signal.dataQuality) ? signal.dataQuality : {});
    const qualityStatus = String(quality.status || "").trim().toLowerCase();
    const availability = String(signal.availability || "").trim().toLowerCase();
    let status = allowedStatuses.has(rawStatus) ? rawStatus : "none";
    if (rawLifecycle === "invalidated" || rawLifecycle === "expired") {
      status = rawLifecycle;
    } else if (status === "none") {
      if (qualityStatus === "awaiting_completed_bar") status = "awaiting";
      else if (qualityStatus === "stale" || availability === "stale") status = "stale";
      else if (qualityStatus === "not_scanned" || availability === "not_scanned") status = "not_scanned";
      else if (qualityStatus === "unavailable" || availability === "unavailable" || availability === "insufficient") status = "unavailable";
    }
    const lifecycleState = allowedLifecycles.has(rawLifecycle)
      ? rawLifecycle
      : (status === "confirmed" ? "confirmed" : (status === "invalidated" ? "invalidated" : (status === "expired" ? "expired" : (status === "warning" ? "candidate" : "none"))));
    return {
      ...signal,
      direction,
      status,
      lifecycleState,
      dataQuality: quality,
      confirmation: isObject(signal.confirmation) ? signal.confirmation : {},
      evidence: Array.isArray(signal.evidence) ? signal.evidence : [],
    };
  }

  function describeSignal(signal) {
    if (signal.status === "invalidated") return STATUS_DESCRIPTORS.invalidated;
    if (signal.status === "expired") return STATUS_DESCRIPTORS.expired;
    if (signal.status === "conflict") return STATUS_DESCRIPTORS.conflict;
    if (signal.status === "awaiting") return STATUS_DESCRIPTORS.awaiting;
    if (signal.status === "not_scanned") return STATUS_DESCRIPTORS.not_scanned;
    if (signal.status === "stale") return STATUS_DESCRIPTORS.stale;
    if (signal.status === "unavailable") return STATUS_DESCRIPTORS.unavailable;
    if (signal.status === "warning" || signal.status === "confirmed") {
      return STATUS_DESCRIPTORS[`${signal.direction}:${signal.status}`] || STATUS_DESCRIPTORS.none;
    }
    return STATUS_DESCRIPTORS.none;
  }

  function signalPriority(signal) {
    const priority = { confirmed: 9, conflict: 8, warning: 7, invalidated: 6, expired: 5, awaiting: 4, stale: 3, unavailable: 2, not_scanned: 1, none: 0 };
    return priority[signal.status] || 0;
  }

  function renderSignalBadge(badge, timeframe, rawSignal) {
    const signal = normalizeSignal(rawSignal);
    if (!badge) return signal;
    const descriptor = describeSignal(signal);
    const label = timeframeLabel(timeframe);
    const score = Number.isFinite(Number(signal.score)) ? ` · ${Number(signal.score).toFixed(0)}分` : "";
    badge.className = `signal-badge signal-badge-${timeframe} ${descriptor.className}`;
    badge.textContent = `${label} · ${descriptor.label}${score}`;
    const quality = signalQualityText(signal, { compact: true });
    badge.title = [currentStageText(timeframe, signal), signal.pattern, quality].filter(Boolean).join(" · ");
    return signal;
  }

  function hasSignalData(value) {
    return isObject(value) && Object.keys(value).length > 0;
  }

  function resolvedSignals(item) {
    const itemSignals = isObject(item.signals) ? item.signals : {};
    const stored = isObject(state.signalStates[item.key]) ? state.signalStates[item.key] : {};
    const storedSignals = isObject(stored.signals) ? stored.signals : stored;
    return TIMEFRAME_IDS.reduce((signals, timeframe) => {
      signals[timeframe] = hasSignalData(itemSignals[timeframe])
        ? itemSignals[timeframe]
        : storedSignals[timeframe];
      return signals;
    }, {});
  }

  function evidenceText(label, evidence) {
    if (!isObject(evidence)) return `${label} · ${String(evidence || "证据待更新")}`;
    const category = evidence.category ? `${evidence.category}：` : "";
    const evidenceLabel = evidence.label || evidence.name || "证据";
    let value = evidence.value;
    if (isObject(value) || Array.isArray(value)) value = JSON.stringify(value);
    const valueText = value === null || value === undefined || value === "" ? "" : `（${String(value)}）`;
    return `${label} · ${category}${evidenceLabel}${valueText}`;
  }

  function renderEvidence(node, signals) {
    const evidenceDetails = node.querySelector(".signal-evidence");
    const evidenceList = node.querySelector(".signal-evidence-list");
    const summary = evidenceDetails?.querySelector("summary");
    if (!evidenceDetails || !evidenceList || !summary) return;

    evidenceList.replaceChildren();
    let count = 0;
    TIMEFRAME_IDS.forEach((timeframe) => {
      const label = timeframeLabel(timeframe);
      const signal = normalizeSignal(signals[timeframe]);
      signal.evidence.forEach((evidence) => {
        const evidenceItem = document.createElement("li");
        evidenceItem.textContent = evidenceText(label, evidence);
        evidenceList.appendChild(evidenceItem);
        count += 1;
      });
    });

    if (!count) {
      const evidenceItem = document.createElement("li");
      evidenceItem.textContent = "当前没有活动证据。";
      evidenceList.appendChild(evidenceItem);
    }
    summary.textContent = count ? `查看信号证据（${count}）` : "查看信号证据";
  }

  function dominantSignal(signals) {
    const candidates = TIMEFRAME_IDS.map((timeframe) => ({
      timeframe,
      signal: normalizeSignal(signals[timeframe]),
    }));
    candidates.sort((a, b) => {
      const priorityDiff = signalPriority(b.signal) - signalPriority(a.signal);
      if (priorityDiff) return priorityDiff;
      return Number(b.signal.score || 0) - Number(a.signal.score || 0);
    });
    return candidates[0].signal.status === "none" ? null : candidates[0];
  }

  function signalQualityText(signal, { compact = false } = {}) {
    const quality = isObject(signal.dataQuality) ? signal.dataQuality : {};
    const status = String(quality.status || signal.availability || "").trim().toLowerCase();
    const label = QUALITY_LABELS[status] || (status ? status : "状态未提供");
    const reason = String(quality.reason || "").trim();
    const eligibility = quality.stateEligible === false || signal.currentStateEligible === false
      ? "当前不更新状态"
      : "";
    const details = [label, reason, eligibility].filter(Boolean).join(" · ");
    return compact ? details : `数据质量：${details}`;
  }

  function formatBeijingDateTime(value) {
    if (!value) return "--";
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) return String(value);
    const parts = new Intl.DateTimeFormat("zh-CN", {
      timeZone: "Asia/Shanghai",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hourCycle: "h23",
    }).formatToParts(parsed);
    const values = Object.fromEntries(parts.map((part) => [part.type, part.value]));
    return `${values.year}-${values.month}-${values.day} ${values.hour}:${values.minute}:${values.second}`;
  }

  function numericValue(value) {
    if (value === null || value === undefined || value === "") return null;
    const number = Number(value);
    return Number.isFinite(number) ? number : null;
  }

  function currentStageText(timeframe, signal) {
    const direction = signal.direction === "top" ? "顶部" : (signal.direction === "bottom" ? "底部" : "");
    let stage = describeSignal(signal).label;
    if (signal.status === "warning") {
      const resonance = signal.lifecycleState === "resonant" ? "共振" : "";
      stage = `${direction}${resonance}候选预警`;
    } else if (signal.status === "confirmed") {
      stage = `${direction}${timeframe === "long" ? "收盘" : ""}确认`;
    } else if (signal.status === "none") {
      stage = "无活动信号";
    }
    const score = numericValue(signal.score);
    const configuredMaximum = numericValue(state.signalPayload?.rules?.shared?.scoreMaximum);
    const scoreMaximum = configuredMaximum ?? 80;
    const scoreStatuses = new Set(["warning", "confirmed", "conflict", "invalidated", "expired"]);
    const scoreText = score === null || !scoreStatuses.has(signal.status)
      ? ""
      : `（${score.toFixed(0)}/${scoreMaximum.toFixed(0)}）`;
    return `当前阶段：${stage}${scoreText}`;
  }

  function confirmationText(timeframe, signal) {
    if (!new Set(["warning", "confirmed"]).has(signal.status)) return "确认条件：--";
    if (timeframe === "short") {
      return "确认条件：等待60分钟同向共振；15分钟不能单周期确认";
    }
    if (timeframe === "medium") {
      return "确认条件：等待日线同向共振；60分钟不能单周期确认";
    }
    if (timeframe !== "long") return "确认条件：--";

    const confirmation = isObject(signal.confirmation) ? signal.confirmation : {};
    const triggerLevel = numericValue(confirmation.triggerLevel);
    const latestClose = numericValue(confirmation.latestClose);
    const scoreThreshold = numericValue(confirmation.scoreThreshold);
    const score = numericValue(signal.score);
    const action = signal.direction === "top" ? "跌破" : "站上";
    const confirmed = confirmation.confirmed === true || signal.status === "confirmed";
    const pricePassed = confirmation.keyCrossState === "key_side";
    const scorePassed = confirmation.scorePassed !== false;
    const reasonCode = String(confirmation.reasonCode || "");

    if (triggerLevel === null || latestClose === null) {
      return "确认条件：等待日线收盘关键位评估";
    }
    if (!scorePassed && scoreThreshold !== null && score !== null) {
      if (pricePassed) {
        return `确认条件：评分达到 ${scoreThreshold.toFixed(0)}；当前 ${score.toFixed(0)}，尚未满足`;
      }
      return `确认条件：日线收盘${action} ${triggerLevel.toFixed(2)} 且评分达到 ${scoreThreshold.toFixed(0)}；当前收盘 ${latestClose.toFixed(2)}、评分 ${score.toFixed(0)}，尚未满足`;
    }
    if (!confirmed && reasonCode === "awaiting_market_close" && pricePassed) {
      return `确认条件：日线收盘${action} ${triggerLevel.toFixed(2)}；当前 ${latestClose.toFixed(2)}，价格已满足，等待收盘确认`;
    }
    if (confirmed && !pricePassed) {
      return `确认条件：日线收盘${action} ${triggerLevel.toFixed(2)}；当前 ${latestClose.toFixed(2)}，已确认但目前未保持在确认侧`;
    }
    if (!confirmed && pricePassed) {
      return `确认条件：日线收盘${action} ${triggerLevel.toFixed(2)}；当前 ${latestClose.toFixed(2)}，价格已满足，尚未升级确认`;
    }
    return `确认条件：日线收盘${action} ${triggerLevel.toFixed(2)}；当前 ${latestClose.toFixed(2)}，${confirmed && pricePassed ? "已满足" : "尚未满足"}`;
  }

  function confluenceText(signal) {
    const confluence = isObject(signal.confluence) ? signal.confluence : {};
    const level = String(confluence.level || "none").trim().toLowerCase();
    const label = CONFLUENCE_LABELS[level] || level || CONFLUENCE_LABELS.none;
    const supporting = Array.isArray(confluence.supportingTimeframes)
      ? confluence.supportingTimeframes.map((timeframe) => timeframeLabel(timeframe, String(timeframe))).join("、")
      : "";
    const conflicts = Array.isArray(confluence.conflictWith)
      ? confluence.conflictWith.map((timeframe) => timeframeLabel(timeframe, String(timeframe))).join("、")
      : "";
    const details = [label, supporting ? `支持：${supporting}` : "", conflicts ? `冲突：${conflicts}` : ""].filter(Boolean);
    return { text: `跨周期：${details.join("｜")}`, level };
  }

  function applyMetaClass(element, baseClass, stateClass) {
    if (!element) return;
    element.className = stateClass ? `${baseClass} ${stateClass}` : baseClass;
  }

  function renderSignalMeta(node, signals) {
    const evaluatedAtEl = node.querySelector('[data-field="signalEvaluatedAt"]');
    const phaseEl = node.querySelector('[data-field="signalPhase"]');
    const confirmationEl = node.querySelector('[data-field="signalConfirmation"]');
    const metricsEl = node.querySelector('[data-field="signalMetrics"]');
    const levelEl = node.querySelector('[data-field="signalKeyLevel"]');
    const completedBarEl = node.querySelector('[data-field="signalCompletedBar"]');
    const qualityEl = node.querySelector('[data-field="signalDataQuality"]');
    const confluenceEl = node.querySelector('[data-field="signalConfluence"]');
    const dominant = dominantSignal(signals);

    const metricSource = TIMEFRAME_IDS
      .map((timeframe) => normalizeSignal(signals[timeframe]))
      .find((signal) => isObject(signal.metrics) && Object.values(signal.metrics).some((value) => value !== null && value !== undefined));
    const metrics = isObject(metricSource?.metrics) ? metricSource.metrics : {};
    if (metricsEl) metricsEl.textContent = `周期指标：MA20 / MA60 / RSI14：${fmt(metrics.ma20)} / ${fmt(metrics.ma60)} / ${fmt(metrics.rsi14)}`;

    if (!dominant) {
      if (evaluatedAtEl) evaluatedAtEl.textContent = "信号评估时间：--";
      if (phaseEl) phaseEl.textContent = "当前阶段：无活动信号";
      if (confirmationEl) confirmationEl.textContent = "确认条件：--";
      if (levelEl) levelEl.textContent = "关键结构位：--";
      if (completedBarEl) completedBarEl.textContent = "信号对应K线：--";
      if (qualityEl) qualityEl.textContent = "数据质量：未扫描或无信号";
      if (confluenceEl) confluenceEl.textContent = "跨周期：--";
      return;
    }

    const { timeframe, signal } = dominant;
    const keyLevel = fmt(signal.keyLevel);
    const invalidation = fmt(signal.invalidationLevel);
    if (evaluatedAtEl) {
      evaluatedAtEl.textContent = `信号评估时间：${formatBeijingDateTime(signal.evaluatedAt || signal.updatedAt || signal.changedAt)}`;
    }
    if (phaseEl) {
      phaseEl.textContent = currentStageText(timeframe, signal);
      applyMetaClass(phaseEl, "signal-lifecycle", `signal-lifecycle-${signal.lifecycleState || "none"}`);
    }
    if (confirmationEl) confirmationEl.textContent = confirmationText(timeframe, signal);
    if (levelEl) levelEl.textContent = `关键结构位：${keyLevel}｜失效位：${invalidation}`;
    if (completedBarEl) completedBarEl.textContent = `信号对应K线：${signal.latestCompletedBarAt || "--"}`;
    if (qualityEl) {
      qualityEl.textContent = signalQualityText(signal);
      const qualityStatus = String(signal.dataQuality?.status || signal.availability || "").trim().toLowerCase();
      const qualityClass = qualityStatus === "awaiting_completed_bar"
        ? "signal-quality-awaiting"
        : (qualityStatus === "stale" ? "signal-quality-stale" : "signal-quality-available");
      applyMetaClass(qualityEl, "signal-quality", qualityClass);
    }
    if (confluenceEl) {
      const confluence = confluenceText(signal);
      confluenceEl.textContent = confluence.text;
      const confluenceClass = confluence.level === "cross_timeframe_conflict"
        ? "signal-confluence-conflict"
        : (confluence.level === "resonant" ? "signal-confluence-resonant" : "signal-confluence-neutral");
      applyMetaClass(confluenceEl, "signal-confluence", confluenceClass);
    }
  }

  function renderCard(item, region) {
    const node = cardTemplate.content.firstElementChild.cloneNode(true);
    node.classList.add(item.drawdownClass || "unknown");
    node.dataset.indexKey = item.key || "";
    node.querySelector(".market-badge").textContent = region;
    node.querySelector("h3").textContent = item.name || item.key || "--";
    const referenceStatus = item.signalEligible === false
      ? ({ partial: "（部分数据）", stale: "（缓存）", unavailable: "（暂不可用）" }[item.availability] || "")
      : "";
    node.querySelector(".source").textContent = `${sourceText(item.source)}${referenceStatus}`;

    node.querySelector(".index-price, .price").textContent = fmt(item.price, 2);

    const change = node.querySelector(".index-change, .change");
    change.classList.add(pctClass(item.pct));
    change.querySelector('[data-field="dailyChange"]').textContent = `${fmtSigned(item.change)} (${fmtSigned(item.pct)}%)`;

    const oneYear = node.querySelector(".index-one-year, .one-year-change");
    oneYear.classList.add(pctClass(item.oneYearPct));
    oneYear.querySelector('[data-field="oneYearChange"]').textContent =
      item.oneYearPct === null || item.oneYearPct === undefined
        ? "--"
        : `${fmtSigned(item.oneYearChange)} (${fmtSigned(item.oneYearPct)}%)`;

    const drawdown = Number(item.drawdownPct || 0);
    node.querySelector(".drawdown-focus strong").textContent = `${fmtSigned(item.drawdownPct)}%`;
    node.querySelector(".drawdown-focus small").textContent = `距离高点 ${fmtSigned(item.drawdownPoints)} 点`;
    node.querySelector(".drawdown-track span").style.width = `${Math.min(100, Math.abs(drawdown) * 5)}%`;

    node.querySelector('[data-field="peakHigh"]').textContent = fmt(item.peakHigh);
    node.querySelector('[data-field="peakDate"]').textContent = item.peakDate || "--";
    node.querySelector('[data-field="latestDate"]').textContent = item.latestDate || "--";
    node.querySelector('[data-field="dataTime"]').textContent = item.dataTime || "--";

    const coverage = node.querySelector('[data-field="windowCoverage"]');
    if (coverage) {
      coverage.textContent = ({covered:"已覆盖窗口边界", partial:"历史不足 · 仅样本高点", empty:"无有效历史"}[item.windowCoverage] || "未确认覆盖");
      coverage.title = `窗口 ${item.windowStart || "--"} 至 ${item.windowEnd || "--"}；实际历史 ${item.historyStart || "--"} 至 ${item.historyEnd || "--"}。边界覆盖不保证中间无缺失。`;
      if (item.windowCoverage !== "covered") coverage.classList.add("coverage-warning");
    }
    if (item.windowCoverage === "partial") node.querySelector(".drawdown-focus small").textContent += " · 样本不足";
    if (item.peakSource === "current_quote") node.querySelector('[data-field="peakDate"]').textContent += "（报价新高）";
    if (item.drawdownStatus === "inconsistent_quote_history") {
      node.querySelector(".drawdown-focus strong").textContent = "--";
      node.querySelector(".drawdown-focus small").textContent = "报价与历史时点不一致，暂不计算回撤";
    }

    if (item.signalEligible === false) {
      const badges = node.querySelector(".signal-badges");
      const badge = document.createElement("span");
      badge.className = "signal-badge signal-none";
      badge.textContent = "仅行情展示 · 不参与指数信号";
      badges?.replaceChildren(badge);
      badges?.setAttribute("aria-label", "仅行情展示，不参与指数信号");
      node.querySelector(".signal-card-meta")?.remove();
      node.querySelector(".signal-evidence")?.remove();
      return node;
    }

    const signals = resolvedSignals(item);
    renderSignalBadge(node.querySelector(".signal-badge-short"), "short", signals.short);
    renderSignalBadge(node.querySelector(".signal-badge-medium"), "medium", signals.medium);
    renderSignalBadge(node.querySelector(".signal-badge-long"), "long", signals.long);
    renderSignalMeta(node, signals);
    renderEvidence(node, signals);
    return node;
  }

  function renderMarketErrors(errors) {
    if (Array.isArray(errors) && errors.length) {
      errorBox.hidden = false;
      errorBox.textContent = `部分行情获取失败：${errors
        .map((err) => `${err.region || ""}${err.name || err.key || ""}: ${err.error || err.message || "未知错误"}`)
        .join("；")}`;
      return;
    }
    errorBox.hidden = true;
    errorBox.textContent = "";
  }

  function renderMarketData(data, { cached = false } = {}) {
    state.marketData = data;
    state.marketCached = Boolean(cached);
    updatedAtEl.textContent = cached
      ? `${data.generatedAt || "--"}（缓存，后台刷新中）`
      : data.generatedAt || "--";
    groupsEl.replaceChildren();
    renderMarketErrors([
      ...(Array.isArray(data.errors) ? data.errors : []),
      ...(Array.isArray(data.referenceErrors) ? data.referenceErrors : []),
    ]);

    (data.groups || []).forEach((group) => {
      const groupNode = groupTemplate.content.firstElementChild.cloneNode(true);
      groupNode.dataset.region = group.key;
      const displayCode = group.displayCode || group.key;
      groupNode.querySelector("h2").textContent = `${displayCode} ${group.title}`;
      groupNode.querySelector("p").textContent = group.subtitle || "";
      const grid = groupNode.querySelector(".card-grid");
      (group.items || []).forEach((item) => grid.appendChild(renderCard(item, displayCode)));
      groupsEl.appendChild(groupNode);
    });

    state.loaded = true;
  }

  function countValue(value) {
    if (Array.isArray(value)) return value.length;
    const count = Number(value);
    return Number.isFinite(count) ? count : 0;
  }

  function renderSignalSummary(summary, generatedAt) {
    const safeSummary = isObject(summary) ? summary : {};
    signalSummaryEl.replaceChildren();
    SUMMARY_FIELDS.forEach(([key, label]) => {
      const item = document.createElement("div");
      item.className = `signal-summary-item signal-summary-${key}`;
      const value = document.createElement("strong");
      const caption = document.createElement("span");
      value.textContent = String(countValue(safeSummary[key]));
      caption.textContent = label;
      item.append(value, caption);
      signalSummaryEl.appendChild(item);
    });
    signalSummaryEl.title = generatedAt ? `信号状态更新时间：${generatedAt}` : "等待指数信号状态";
  }

  function renderSignalDailyStatus(activity) {
    const safeActivity = isObject(activity) ? activity : {};
    const scanCount = countValue(safeActivity.scanCount);
    const eventCount = countValue(safeActivity.eventCount);
    const notificationCount = countValue(safeActivity.notificationCount);
    const dataPending = countValue(safeActivity.dataPending);
    let message = String(safeActivity.message || "").trim();
    if (!message && dataPending > 0) {
      message = `有 ${dataPending} 个周期等待已完成K线；该周期暂不更新状态或推送。`;
    } else if (!message && scanCount > 0 && eventCount === 0) {
      message = `今日已完成 ${scanCount} 次指数信号扫描；当前没有新增状态变化。尾盘快照与收盘复核结果以独立推送状态为准。`;
    } else if (!message && eventCount > 0) {
      message = `今日产生 ${eventCount} 条状态变化，实际推送 ${notificationCount} 条。`;
    } else if (!message) {
      message = "今日尚未完成指数信号扫描。";
    }

    const headline = document.createElement("strong");
    const note = document.createElement("span");
    headline.textContent = message;
    note.textContent = "独立通道启用后，14:55（北京时间）强制推送尾盘判断快照；15:10 按收盘数据复核，判断不一致时重新推送。日线确认仍仅以收盘完成数据为准。";
    signalDailyStatusEl.replaceChildren(headline, note);
  }

  function ruleText(value, fallback = "未提供") {
    if (value === null || value === undefined || value === "") return fallback;
    if (Array.isArray(value)) {
      const items = value.map((item) => ruleText(item, "")).filter(Boolean);
      return items.length ? items.join("、") : fallback;
    }
    if (isObject(value)) {
      return ruleText(value.description || value.label || value.name || value.text || value.mode || value.id, fallback);
    }
    const textValue = String(value).trim();
    return textValue || fallback;
  }

  function ruleNumber(value, unit = "") {
    const number = Number(value);
    return Number.isFinite(number) ? `${number}${unit}` : "--";
  }

  function intervalText(interval) {
    const raw = ruleText(interval, "");
    return INTERVAL_LABELS[raw] || raw || "未提供";
  }

  function patternText(patterns) {
    if (Array.isArray(patterns)) {
      const labels = patterns
        .map((pattern) => PATTERN_LABELS[String(pattern)] || ruleText(pattern, ""))
        .filter(Boolean);
      return labels.length ? labels.join("、") : "未提供";
    }
    const raw = ruleText(patterns, "未提供");
    return PATTERN_LABELS[raw] || raw;
  }

  function rulePolicyText(policy) {
    if (typeof policy === "string" && CONFIRMATION_POLICY_LABELS[policy]) {
      return CONFIRMATION_POLICY_LABELS[policy];
    }
    if (isObject(policy)) {
      const policyId = String(policy.id || policy.code || "");
      if (CONFIRMATION_POLICY_LABELS[policyId]) return CONFIRMATION_POLICY_LABELS[policyId];
      const description = ruleText(policy.description || policy.label || policy.text, "");
      if (description) return description;
      if (policy.allowsIndependentConfirmation === true) return "当前周期可独立确认";
      if (policy.allowsIndependentConfirmation === false) return "需按跨周期规则确认";
    }
    return ruleText(policy, "以服务端规则为准");
  }

  function createRuleDetail(label, value) {
    const item = document.createElement("div");
    item.className = "index-signal-rule-detail";
    const caption = document.createElement("span");
    const content = document.createElement("strong");
    caption.textContent = label;
    content.textContent = value;
    item.append(caption, content);
    return item;
  }

  function timeframeRuleLabel(rule) {
    const id = String(rule?.id || "").trim();
    if (TIMEFRAME_LABELS[id]) return TIMEFRAME_LABELS[id];
    const label = ruleText(rule?.label, id || "未命名周期");
    const interval = ruleText(rule?.interval, "");
    return interval ? `${label} · ${interval}` : label;
  }

  function timeframeVolumeText(timeframe) {
    const policy = String(timeframe?.volumePolicy || "").trim().toLowerCase();
    const days = ruleNumber(timeframe?.rvolHistoryDays, " 个交易日");
    if (policy === "same_slot_rvol20") return `同时间槽 RVOL：前 ${days}（不混用开盘与午间成交量）`;
    if (policy === "volume_ma20") return "日线成交量：MA20 基线";
    return ruleText(timeframe?.volumePolicy, "以服务端规则为准");
  }

  function calibrationText(calibration) {
    const value = isObject(calibration) ? calibration : {};
    const status = String(value.status || "").trim().toLowerCase();
    const requirement = ruleText(value.requirement, "");
    if (status === "provisional") return `暂定：待样本外校准${requirement ? `（${requirement}）` : ""}；评分不是概率。`;
    if (status) return `${status}${requirement ? `：${requirement}` : ""}`;
    return "未提供校准结论；评分不是概率。";
  }

  function scoreScaleText(shared) {
    const scale = isObject(shared.scoreScale) ? shared.scoreScale : {};
    const maximum = ruleNumber(shared.scoreMaximum ?? scale.maximum, " 分");
    const structure = ruleNumber(scale.structureMaximum, " 分");
    const correlated = ruleNumber(
      scale.correlatedPriceConfirmationMaximum ?? shared.correlatedPriceConfirmationCap,
      " 分",
    );
    const volume = ruleNumber(scale.volumeMaximum, " 分");
    return `最高 ${maximum}；结构 ${structure}；相关价格类证据合计封顶 ${correlated}；量能 ${volume}。`;
  }

  function thresholdTextFor(timeframe) {
    const warning = ruleNumber(timeframe?.warningThreshold, " 分");
    const applicability = String(
      timeframe?.confirmThresholdApplicability || timeframe?.confirmationPolicy || "",
    ).trim().toLowerCase();
    const confirmation = ruleNumber(timeframe?.confirmThreshold, " 分");
    if (applicability === "market_close_only" && confirmation !== "--") {
      return `预警 ≥ ${warning} · 收盘确认 ≥ ${confirmation}`;
    }
    return `候选预警 ≥ ${warning} · 不单周期确认`;
  }

  function renderSignalRules(rules) {
    if (!signalRulesEl) return;
    const safeRules = isObject(rules) ? rules : {};
    const timeframes = Array.isArray(safeRules.timeframes)
      ? safeRules.timeframes.filter((timeframe) => isObject(timeframe))
      : [];
    const version = ruleText(safeRules.version, "未取得");
    if (signalRuleVersionEl) signalRuleVersionEl.textContent = `规则版本：${version}`;

    signalRulesEl.replaceChildren();
    if (!timeframes.length) {
      const empty = document.createElement("p");
      empty.className = "index-signal-rule-empty";
      empty.textContent = "暂未取得当前多周期规则；不以页面默认参数替代服务端规则。";
      signalRulesEl.appendChild(empty);
      return;
    }

    timeframes.forEach((timeframe) => {
      const card = document.createElement("article");
      card.className = "index-signal-rule-card";
      card.dataset.timeframe = String(timeframe.id || "");

      const head = document.createElement("div");
      head.className = "index-signal-rule-card-head";
      const title = document.createElement("h5");
      title.textContent = timeframeRuleLabel(timeframe);
      const role = document.createElement("span");
      role.className = "index-signal-rule-role";
      role.textContent = ruleText(timeframe.role, "服务端定义");
      head.append(title, role);

      const details = document.createElement("div");
      details.className = "index-signal-rule-details";
      const pivotText = `侧宽 ${ruleNumber(timeframe.pivotSide)} · 最小间隔 ${ruleNumber(timeframe.minSeparation, " 根")}`;
      const thresholdText = thresholdTextFor(timeframe);
      const fetchCount = timeframe.fetchBarCount ?? timeframe.barCount;
      const detectionWindow = timeframe.lookback;
      const requiredBars = timeframe.requiredBarCount ?? timeframe.minimumOperationalBars ?? timeframe.minRows;
      const warmup = timeframe.indicatorWarmupBars;
      details.append(
        createRuleDetail("K线周期", intervalText(timeframe.interval)),
        createRuleDetail("取数留存", `${ruleNumber(fetchCount, " 根")} K线（独立于识别窗）`),
        createRuleDetail("形态识别窗", `${ruleNumber(detectionWindow, " 根")} K线`),
        createRuleDetail("指标预热", `${ruleNumber(warmup, " 根")}；运行下限 ${ruleNumber(requiredBars, " 根")}`),
        createRuleDetail("量能基线", timeframeVolumeText(timeframe)),
        createRuleDetail("枢轴参数", pivotText),
        createRuleDetail("识别形态", patternText(timeframe.patterns)),
        createRuleDetail("形态口径", ruleText(timeframe.detectionRule, "以服务端规则为准")),
        createRuleDetail("评分门槛", thresholdText),
        createRuleDetail("确认策略", rulePolicyText(timeframe.confirmationPolicy)),
      );
      card.append(head, details);
      signalRulesEl.appendChild(card);
    });

    const shared = isObject(safeRules.shared) ? safeRules.shared : {};
    const sharedCard = document.createElement("section");
    sharedCard.className = "index-signal-shared-rules";
    const sharedTitle = document.createElement("h5");
    sharedTitle.textContent = "共同规则";
    const sharedNote = document.createElement("p");
    sharedNote.textContent = "以下边界适用于当前规则版本；未提供的字段不由页面推断。";
    const sharedDetails = document.createElement("div");
    sharedDetails.className = "index-signal-rule-details";
    const evidenceCount = Number(shared.minEvidenceCategories);
    const evidenceText = Number.isFinite(evidenceCount)
      ? `结构必须具备，另加至少 ${evidenceCount - 1} 类独立确认；相关价格类证据不重复累加。`
      : "结构必须具备，另加独立确认；相关价格类证据不重复累加。";
    const keyCrossText = ruleText(
      shared.keyCrossPolicy,
      "事件=本根已收K线首次越过关键位；状态=当前收盘仍在有效一侧；日线确认使用状态。",
    );
    const decisionText = ruleText(
      shared.decisionState,
      "仅观察 / 人工复核；确认不是交易，也不触发自动下单。",
    );
    sharedDetails.append(
      createRuleDetail("评分上限与封顶", scoreScaleText(shared)),
      createRuleDetail("最小证据", evidenceText),
      createRuleDetail("阈值校准", calibrationText(shared.thresholdCalibration)),
      createRuleDetail("关键位：事件与状态", keyCrossText),
      createRuleDetail("确认缓冲", ruleText(shared.confirmationBuffer)),
      createRuleDetail("多周期规则", rulePolicyText(shared.confluencePolicy)),
      createRuleDetail("盘中规则", rulePolicyText(shared.intradayPolicy)),
      createRuleDetail("决策边界", decisionText),
    );
    sharedCard.append(sharedTitle, sharedNote, sharedDetails);
    signalRulesEl.appendChild(sharedCard);
  }

  function collectSignalStates(value, output = {}, fallbackKey = "") {
    if (Array.isArray(value)) {
      value.forEach((item) => collectSignalStates(item, output, fallbackKey));
      return output;
    }
    if (!isObject(value)) return output;

    const key = String(value.indexKey || value.key || fallbackKey || "");
    const directSignals = isObject(value.signals) ? value.signals : value;
    if (key && hasTimeframeSignals(directSignals)) {
      output[key] = directSignals;
    }

    Object.entries(value).forEach(([childKey, child]) => {
      if (!isObject(child) && !Array.isArray(child)) return;
      const candidate = isObject(child) && isObject(child.signals) ? child.signals : child;
      if (hasTimeframeSignals(candidate)) {
        output[String(child.indexKey || child.key || childKey)] = candidate;
      } else if (!TIMEFRAME_IDS.includes(childKey) && childKey !== "signals") {
        collectSignalStates(child, output, childKey);
      }
    });
    return output;
  }

  function resultText(result, fallback) {
    if (typeof result === "string") return result;
    if (!isObject(result)) return fallback;
    if (result.reportType) {
      const reportLabel = {
        tail: "14:55尾盘判断",
        close_changed: "15:10收盘修正",
        close_fallback: "15:10收盘补发",
      }[result.reportType] || result.reportType;
      return `${reportLabel}${result.marketDate ? ` · ${result.marketDate}` : ""}${result.notifiedAt ? ` · ${result.notifiedAt}` : ""}`;
    }
    return result.message || result.summary || result.error || result.result?.message || fallback;
  }

  function renderNotificationStatus(notification) {
    if (!signalPushResultEl || !notification) return;
    signalPushResultEl.textContent = `最近推送：${resultText(notification, "暂无推送记录")}`;
  }

  function renderSignalOverview(payload) {
    state.signalPayload = payload;
    state.signalStates = collectSignalStates(payload.states || {});
    renderSignalSummary(payload.summary, payload.generatedAt);
    const dailyActivity = isObject(payload.dailyActivity) ? payload.dailyActivity : {};
    renderSignalDailyStatus({
      ...dailyActivity,
      dataPending: dailyActivity.dataPending ?? payload.summary?.dataPending,
    });
    renderSignalRules(payload.rules);
    renderNotificationStatus(payload.latestPush || payload.notification?.latestPush || payload.notificationStatus);
    if (signalNoticeEl) {
      signalNoticeEl.hidden = true;
      signalNoticeEl.textContent = "";
    }
    if (state.marketData) renderMarketData(state.marketData, { cached: state.marketCached });
  }

  function showLoadingMessage() {
    if (state.marketData) return;
    if (groupsEl.children.length) return;
    const loading = document.createElement("div");
    loading.className = "loading";
    loading.textContent = "正在获取全球主要指数与核心股票数据...";
    groupsEl.appendChild(loading);
  }

  async function loadMarketData(force = false) {
    try {
      const res = await fetch(`/api/indices${force ? "?force=1" : ""}`, { cache: "no-store" });
      const data = unwrapPayload(await res.json());
      if (!res.ok || data.ok === false) throw new Error(data.error || data.message || "获取失败");
      const cached = data.cache?.state === "stale";
      saveMarketCache(data);
      renderMarketData(data, { cached });
    } catch (err) {
      errorBox.hidden = false;
      if (state.marketData) {
        state.marketCached = true;
        updatedAtEl.textContent = `${state.marketData.generatedAt || "--"}（缓存，本次刷新失败）`;
      }
      errorBox.textContent = `市场数据加载失败：${err.message || err}${state.marketData ? "；保留上次行情。" : ""}`;
    }
  }

  async function loadSignalOverview() {
    try {
      const res = await fetch("/api/index-signals", { cache: "no-store" });
      const payload = unwrapPayload(await res.json());
      if (!res.ok || payload.ok === false) throw new Error(payload.error || payload.message || "获取失败");
      renderSignalOverview(payload);
    } catch (err) {
      if (signalNoticeEl) {
        signalNoticeEl.hidden = false;
        signalNoticeEl.textContent = `指数信号加载失败：${err.message || err}；行情仍可正常刷新。`;
      }
      if (!state.signalPayload) {
        renderSignalSummary({}, null);
        renderSignalDailyStatus({});
        renderSignalRules(null);
      }
    }
  }

  function readSettingsForm() {
    return {
      indexSignalDingTalkEnabled: Boolean(settingControls.indexSignalDingTalkEnabled?.checked),
      indexSignalDingTalkWebhook: String(settingControls.indexSignalDingTalkWebhook?.value || "").trim(),
      shortSignalsEnabled: Boolean(settingControls.shortSignalsEnabled?.checked),
      mediumSignalsEnabled: Boolean(settingControls.mediumSignalsEnabled?.checked),
      longSignalsEnabled: Boolean(settingControls.longSignalsEnabled?.checked),
      warningEnabled: Boolean(settingControls.warningEnabled?.checked),
      confirmedEnabled: Boolean(settingControls.confirmedEnabled?.checked),
      invalidatedEnabled: Boolean(settingControls.invalidatedEnabled?.checked),
    };
  }

  function applySettings(rawSettings, metadata = {}) {
    if (state.settingsDirty) return;
    const settings = { ...DEFAULT_SETTINGS, ...(isObject(rawSettings) ? rawSettings : {}) };
    Object.entries(settingControls).forEach(([key, control]) => {
      if (!control) return;
      if (key === "indexSignalDingTalkWebhook") {
        const webhook = String(settings[key] || "");
        control.value = webhook;
        control.placeholder = webhook ? "" : "https://oapi.dingtalk.com/robot/send?...";
      } else {
        control.checked = Boolean(settings[key]);
      }
    });
  }

  function showSettingsMetadata(payload) {
    const testResult = payload.latestTest || payload.testResult || payload.lastTest;
    const pushResult = payload.latestPush || payload.pushResult || payload.notification;
    if (signalTestResultEl && testResult) {
      signalTestResultEl.textContent = `最近测试：${resultText(testResult, "暂无测试记录")}`;
    }
    if (signalPushResultEl && pushResult) {
      signalPushResultEl.textContent = `最近推送：${resultText(pushResult, "暂无推送记录")}`;
    }
  }

  async function loadIndexSignalSettings() {
    if (state.settingsLoaded || state.settingsLoading) return;
    state.settingsLoading = true;
    try {
      const res = await fetch("/api/index-signal-settings", { cache: "no-store" });
      const payload = unwrapPayload(await res.json());
      if (!res.ok || payload.ok === false) throw new Error(payload.error || payload.message || "读取失败");
      const settings = isObject(payload.settings) ? payload.settings : payload;
      applySettings(settings, payload);
      showSettingsMetadata(payload);
    } catch (err) {
      if (signalTestResultEl) signalTestResultEl.textContent = `设置读取失败：${err.message || err}`;
    } finally {
      state.settingsLoading = false;
      state.settingsLoaded = true;
    }
  }

  async function saveIndexSignalSettings() {
    signalSaveBtn.disabled = true;
    const originalText = signalSaveBtn.textContent;
    signalSaveBtn.textContent = "保存中...";
    try {
      const settings = readSettingsForm();
      const res = await fetch("/api/index-signal-settings", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ settings }),
      });
      const payload = unwrapPayload(await res.json());
      if (!res.ok || payload.ok === false) throw new Error(payload.error || payload.message || "保存失败");
      state.settingsDirty = false;
      applySettings(payload.settings || settings, payload);
      showSettingsMetadata(payload);
      if (signalTestResultEl) signalTestResultEl.textContent = `设置保存：${resultText(payload, "已保存")}`;
    } catch (err) {
      if (signalTestResultEl) signalTestResultEl.textContent = `设置保存失败：${err.message || err}`;
    } finally {
      signalSaveBtn.disabled = false;
      signalSaveBtn.textContent = originalText;
    }
  }

  async function testIndexSignalSettings() {
    signalTestBtn.disabled = true;
    const originalText = signalTestBtn.textContent;
    signalTestBtn.textContent = "测试中...";
    try {
      const settings = readSettingsForm();
      const res = await fetch("/api/index-signal-settings/test", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ settings }),
      });
      const payload = unwrapPayload(await res.json());
      if (!res.ok || payload.ok === false) throw new Error(payload.error || payload.message || "测试失败");
      if (signalTestResultEl) signalTestResultEl.textContent = `最近测试：${resultText(payload.result || payload, "发送成功")}`;
    } catch (err) {
      if (signalTestResultEl) signalTestResultEl.textContent = `Webhook 测试失败：${err.message || err}`;
    } finally {
      signalTestBtn.disabled = false;
      signalTestBtn.textContent = originalText;
    }
  }

  async function load(force = false) {
    if (state.loading) return;
    state.loading = true;
    refreshBtn.disabled = true;
    refreshBtn.textContent = "刷新中...";
    showLoadingMessage();

    const tasks = [loadMarketData(force), loadSignalOverview()];
    if (window.macroTelemetryPanel?.load) tasks.push(window.macroTelemetryPanel.load(force));
    if (!state.settingsLoaded && !state.settingsLoading) tasks.push(loadIndexSignalSettings());
    await Promise.allSettled(tasks);

    state.loading = false;
    refreshBtn.disabled = false;
    refreshBtn.textContent = "↻ 刷新数据";
  }

  function startTimer() {
    stopTimer();
    state.timer = setInterval(() => load(false), 60_000);
  }

  function stopTimer() {
    if (state.timer) clearInterval(state.timer);
    state.timer = null;
  }

  Object.values(settingControls).forEach((control) => {
    if (!control) return;
    const markDirty = () => {
      state.settingsDirty = true;
    };
    control.addEventListener("input", markDirty);
    control.addEventListener("change", markDirty);
  });
  refreshBtn.addEventListener("click", () => load(true));
  signalSaveBtn.addEventListener("click", saveIndexSignalSettings);
  signalTestBtn.addEventListener("click", testIndexSignalSettings);

  restoreMarketCache();

  window.indicesPanel = {
    load,
    startTimer,
    stopTimer,
    isLoaded: () => state.loaded,
  };

  if (standalone) {
    load(false);
    startTimer();
  }
})();
