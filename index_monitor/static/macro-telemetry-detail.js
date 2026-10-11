(() => {
  if (location.protocol === "file:") return;
  const KEYS = {
    us10y: { name: "10年美债收益率", region: "US", source: "U.S. Treasury", unit: "%" },
    us30y: { name: "30年美债收益率", region: "US", source: "U.S. Treasury", unit: "%" },
    cn10y: { name: "10年中国国债收益率", region: "CN", source: "ChinaBond（中央国债登记结算有限责任公司）", unit: "%" },
    cn30y: { name: "30年中国国债收益率", region: "CN", source: "ChinaBond（中央国债登记结算有限责任公司）", unit: "%" },
    kr_margin_financing: { name: "韩国信用融资余额", region: "KR", source: "KOFIA FreeSIS", unit: "万亿韩元" },
    gold: { name: "黄金（COMEX）", region: "Global", source: "Yahoo Finance", unit: "USD/oz" },
    cn_gold_reserves: { name: "中国官方黄金储备（央行）", region: "CN", source: "State Administration of Foreign Exchange (SAFE)", unit: "吨" },
    tr_gold_reserves: { name: "土耳其央行黄金储备", region: "TR", source: "Central Bank of the Republic of Türkiye (TCMB) EVDS3", unit: "吨" },
  };
  const INTERVALS = ["D", "W", "M", "Y"];
  const INTERVAL_LABELS = { D: "日线", W: "周线", M: "月线", Y: "年线" };
  const byId = (id) => document.getElementById(id);

  const elements = {
    title: byId("macroHistoryTitle"),
    subtitle: byId("macroHistorySubtitle"),
    region: byId("macroHistoryRegion"),
    source: byId("macroHistorySource"),
    coverage: byId("macroHistoryCoverage"),
    latestValue: byId("macroHistoryLatestValue"),
    unit: byId("macroHistoryUnit"),
    latestDate: byId("macroHistoryLatestDate"),
    pointCount: byId("macroHistoryPointCount"),
    tabs: byId("macroHistoryTabs"),
    refresh: byId("macroHistoryRefresh"),
    status: byId("macroHistoryStatus"),
    wrap: byId("macroHistoryChartWrap"),
    canvas: byId("macroHistoryCanvas"),
    tooltip: byId("macroHistoryTooltip"),
    generatedAt: byId("macroHistoryGeneratedAt"),
    retry: byId("macroHistoryRetry"),
  };
  if (Object.values(elements).some((element) => !element)) return;

  const query = new URLSearchParams(window.location.search);
  const initialKey = query.get("key") || "";
  const initialInterval = String(query.get("interval") || "D").toUpperCase();
  const state = {
    key: initialKey,
    interval: INTERVALS.includes(initialInterval) ? initialInterval : "D",
    points: [],
    payload: null,
    controller: null,
    frame: 0,
    hoverIndex: -1,
    chart: null,
  };

  function setStatus(message, kind = "") {
    elements.status.hidden = !message;
    elements.status.textContent = message || "";
    elements.status.className = `macro-history-status${kind ? ` is-${kind}` : ""}`;
  }

  function formatValue(value, unit) {
    const number = Number(value);
    if (!Number.isFinite(number)) return "--";
    return number.toFixed(unit === "%" ? 2 : 3);
  }

  function parsePoint(raw, key) {
    if (!raw || typeof raw !== "object" || Array.isArray(raw)) return null;
    const dateText = String(raw.date || "");
    if (!/^\d{4}-\d{2}-\d{2}$/.test(dateText)) return null;
    const timestamp = Date.parse(`${dateText}T00:00:00Z`);
    const value = Number(raw.value);
    if (!Number.isFinite(timestamp) || !Number.isFinite(value) || value <= 0) return null;
    if ((key === "us10y" || key === "us30y" || key === "cn10y" || key === "cn30y") && value >= 20) return null;
    return { date: dateText, value, time: timestamp };
  }

  function normalizePayloadPoints(payload) {
    if (!payload || typeof payload !== "object" || !Array.isArray(payload.points)) return [];
    const byDate = new Map();
    payload.points.forEach((raw) => {
      const point = parsePoint(raw, state.key);
      if (point) byDate.set(point.date, point);
    });
    return [...byDate.values()].sort((left, right) => left.time - right.time);
  }

  function syncUrl() {
    const url = new URL(window.location.href);
    url.searchParams.set("key", state.key);
    url.searchParams.set("interval", state.interval);
    history.replaceState({ key: state.key, interval: state.interval }, "", url);
  }

  function updateTabs() {
    const tabs = [...elements.tabs.querySelectorAll('[role="tab"]')];
    tabs.forEach((tab) => {
      const selected = tab.dataset.interval === state.interval;
      tab.setAttribute("aria-selected", selected ? "true" : "false");
      tab.tabIndex = selected ? 0 : -1;
    });
  }

  function setLoading(loading) {
    elements.refresh.disabled = loading;
    elements.tabs.querySelectorAll("button").forEach((button) => {
      button.disabled = loading;
    });
  }

  function updateIdentity(payload) {
    const meta = KEYS[state.key];
    elements.title.textContent = payload?.name || meta.name;
    document.title = `${elements.title.textContent} · 历史曲线`;
    elements.subtitle.textContent = `${INTERVAL_LABELS[state.interval]}完整历史；横轴按真实数据日期绘制。`;
    elements.region.textContent = payload?.region || meta.region;
    elements.source.textContent = payload?.source || meta.source;
    elements.unit.textContent = `最新值（${payload?.unit || meta.unit}）`;
  }

  function updateSummary(payload) {
    const unit = payload.unit || KEYS[state.key].unit;
    const latest = state.points[state.points.length - 1];
    elements.latestValue.textContent = latest ? formatValue(latest.value, unit) : "--";
    elements.latestDate.textContent = latest?.date || "--";
    elements.pointCount.textContent = String(state.points.length || 0);
    elements.coverage.textContent = state.points.length
      ? `曲线覆盖 ${state.points[0].date} 至 ${latest.date}，共 ${state.points.length} 个有效点位。`
      : "当前周期没有可展示的有效历史点位。";
    elements.generatedAt.textContent = `更新时间：${payload.generatedAt || "--"}`;
    elements.canvas.setAttribute(
      "aria-label",
      `${elements.title.textContent}${INTERVAL_LABELS[state.interval]}历史折线图，共 ${state.points.length} 个有效点位`
    );
  }

  function downsampleForPixels(points, minTime, maxTime, pixelWidth) {
    if (points.length <= Math.max(32, pixelWidth * 2) || maxTime <= minTime) return points;
    const buckets = new Map();
    points.forEach((point, index) => {
      const pixel = Math.max(
        0,
        Math.min(pixelWidth - 1, Math.floor(((point.time - minTime) / (maxTime - minTime)) * pixelWidth))
      );
      const bucket = buckets.get(pixel) || { first: index, last: index, min: index, max: index };
      bucket.last = index;
      if (point.value < points[bucket.min].value) bucket.min = index;
      if (point.value > points[bucket.max].value) bucket.max = index;
      buckets.set(pixel, bucket);
    });
    const indexes = new Set();
    [...buckets.keys()].sort((left, right) => left - right).forEach((key) => {
      const bucket = buckets.get(key);
      [bucket.first, bucket.min, bucket.max, bucket.last]
        .sort((left, right) => left - right)
        .forEach((index) => indexes.add(index));
    });
    return [...indexes].sort((left, right) => left - right).map((index) => points[index]);
  }

  function chartDimensions() {
    const width = Math.max(320, elements.wrap.clientWidth);
    const height = Math.max(300, elements.canvas.clientHeight || elements.wrap.clientHeight);
    const compact = width < 620;
    return {
      width,
      height,
      left: compact ? 48 : 62,
      right: compact ? 15 : 25,
      top: 24,
      bottom: compact ? 38 : 44,
    };
  }

  function resizeCanvas() {
    const dimensions = chartDimensions();
    const ratio = Math.min(2, Math.max(1, window.devicePixelRatio || 1));
    const targetWidth = Math.round(dimensions.width * ratio);
    const targetHeight = Math.round(dimensions.height * ratio);
    if (elements.canvas.width !== targetWidth || elements.canvas.height !== targetHeight) {
      elements.canvas.width = targetWidth;
      elements.canvas.height = targetHeight;
    }
    const context = elements.canvas.getContext("2d");
    context.setTransform(ratio, 0, 0, ratio, 0, 0);
    return { context, ...dimensions };
  }

  function requestDraw() {
    if (state.frame) return;
    state.frame = requestAnimationFrame(() => {
      state.frame = 0;
      drawChart();
    });
  }

  function drawEmpty(context, width, height, message) {
    context.fillStyle = "#7a8ca3";
    context.font = '700 14px Inter, "Microsoft YaHei", sans-serif';
    context.textAlign = "center";
    context.fillText(message, width / 2, height / 2);
  }

  function drawChart() {
    const { context, width, height, left, right, top, bottom } = resizeCanvas();
    context.clearRect(0, 0, width, height);
    if (!state.points.length) {
      state.chart = null;
      drawEmpty(context, width, height, "暂无有效历史曲线");
      return;
    }

    const plotWidth = Math.max(1, width - left - right);
    const plotHeight = Math.max(1, height - top - bottom);
    const minTime = state.points[0].time;
    const maxTime = state.points[state.points.length - 1].time;
    let minValue = Math.min(...state.points.map((point) => point.value));
    let maxValue = Math.max(...state.points.map((point) => point.value));
    const rawRange = maxValue - minValue;
    const padding = rawRange > 0 ? rawRange * 0.1 : Math.max(0.1, Math.abs(maxValue) * 0.04);
    minValue -= padding;
    maxValue += padding;
    const valueRange = Math.max(0.000001, maxValue - minValue);
    const timeRange = Math.max(1, maxTime - minTime);
    const xFor = (point) => left + ((point.time - minTime) / timeRange) * plotWidth;
    const yFor = (point) => top + ((maxValue - point.value) / valueRange) * plotHeight;
    state.chart = { left, right, top, bottom, width, height, plotWidth, plotHeight, minTime, maxTime, xFor, yFor };

    context.lineWidth = 1;
    context.font = '11px Inter, "Microsoft YaHei", sans-serif';
    context.fillStyle = "#8191a5";
    context.strokeStyle = "rgba(111, 137, 166, 0.17)";
    context.textAlign = "right";
    for (let step = 0; step <= 5; step += 1) {
      const ratio = step / 5;
      const y = top + ratio * plotHeight;
      const value = maxValue - ratio * valueRange;
      context.beginPath();
      context.moveTo(left, y);
      context.lineTo(width - right, y);
      context.stroke();
      context.fillText(formatValue(value, state.payload?.unit), left - 8, y + 4);
    }

    const dateFormatter = new Intl.DateTimeFormat("zh-CN", { year: "numeric", month: "2-digit" });
    context.textAlign = "center";
    for (let step = 0; step <= 5; step += 1) {
      const ratio = step / 5;
      const x = left + ratio * plotWidth;
      context.beginPath();
      context.moveTo(x, top);
      context.lineTo(x, top + plotHeight);
      context.stroke();
      context.fillText(dateFormatter.format(new Date(minTime + ratio * timeRange)), x, height - 14);
    }

    const rendered = downsampleForPixels(state.points, minTime, maxTime, Math.max(1, Math.floor(plotWidth)));
    const gradient = context.createLinearGradient(0, top, 0, top + plotHeight);
    gradient.addColorStop(0, "rgba(47, 124, 246, 0.23)");
    gradient.addColorStop(1, "rgba(47, 124, 246, 0.01)");
    context.beginPath();
    rendered.forEach((point, index) => {
      const x = xFor(point);
      const y = yFor(point);
      if (index === 0) context.moveTo(x, y);
      else context.lineTo(x, y);
    });
    const finalPoint = rendered[rendered.length - 1];
    const firstPoint = rendered[0];
    context.lineTo(xFor(finalPoint), top + plotHeight);
    context.lineTo(xFor(firstPoint), top + plotHeight);
    context.closePath();
    context.fillStyle = gradient;
    context.fill();

    context.beginPath();
    rendered.forEach((point, index) => {
      const x = xFor(point);
      const y = yFor(point);
      if (index === 0) context.moveTo(x, y);
      else context.lineTo(x, y);
    });
    context.strokeStyle = state.key === "kr_margin_financing" ? "#12a77d" : "#2f7cf6";
    context.lineWidth = 2.2;
    context.lineJoin = "round";
    context.lineCap = "round";
    context.stroke();

    if (state.hoverIndex >= 0 && state.points[state.hoverIndex]) {
      const point = state.points[state.hoverIndex];
      const x = xFor(point);
      const y = yFor(point);
      context.strokeStyle = "rgba(35, 61, 91, 0.55)";
      context.lineWidth = 1;
      context.setLineDash([4, 4]);
      context.beginPath();
      context.moveTo(x, top);
      context.lineTo(x, top + plotHeight);
      context.moveTo(left, y);
      context.lineTo(width - right, y);
      context.stroke();
      context.setLineDash([]);
      context.beginPath();
      context.arc(x, y, 4.5, 0, Math.PI * 2);
      context.fillStyle = "#fff";
      context.fill();
      context.strokeStyle = state.key === "kr_margin_financing" ? "#12a77d" : "#2f7cf6";
      context.lineWidth = 2.5;
      context.stroke();
    }
  }

  function nearestPoint(clientX) {
    if (!state.chart || !state.points.length) return -1;
    const rect = elements.canvas.getBoundingClientRect();
    const localX = Math.max(state.chart.left, Math.min(state.chart.width - state.chart.right, clientX - rect.left));
    const targetTime = state.chart.minTime + ((localX - state.chart.left) / state.chart.plotWidth) * (state.chart.maxTime - state.chart.minTime);
    let low = 0;
    let high = state.points.length - 1;
    while (low < high) {
      const middle = Math.floor((low + high) / 2);
      if (state.points[middle].time < targetTime) low = middle + 1;
      else high = middle;
    }
    if (low > 0 && Math.abs(state.points[low - 1].time - targetTime) <= Math.abs(state.points[low].time - targetTime)) return low - 1;
    return low;
  }

  function showTooltip(index) {
    const point = state.points[index];
    if (!point || !state.chart) return;
    const x = state.chart.xFor(point);
    const y = state.chart.yFor(point);
    elements.tooltip.querySelector('[data-field="date"]').textContent = point.date;
    elements.tooltip.querySelector('[data-field="value"]').textContent = `${formatValue(point.value, state.payload?.unit)} ${state.payload?.unit || ""}`;
    elements.tooltip.hidden = false;
    const tooltipWidth = elements.tooltip.offsetWidth;
    const tooltipHeight = elements.tooltip.offsetHeight;
    const left = Math.max(8, Math.min(elements.wrap.clientWidth - tooltipWidth - 8, x + 12));
    const top = Math.max(8, Math.min(elements.wrap.clientHeight - tooltipHeight - 8, y - tooltipHeight - 12));
    elements.tooltip.style.left = `${left}px`;
    elements.tooltip.style.top = `${top}px`;
    elements.canvas.setAttribute(
      "aria-label",
      `${elements.title.textContent}，${point.date}，${formatValue(point.value, state.payload?.unit)} ${state.payload?.unit || ""}`
    );
  }

  function handlePointer(event) {
    const index = nearestPoint(event.clientX);
    if (index < 0) return;
    state.hoverIndex = index;
    showTooltip(index);
    requestDraw();
  }

  async function loadHistory(force = false) {
    if (!KEYS[state.key]) return;
    state.controller?.abort();
    const controller = new AbortController();
    state.controller = controller;
    setLoading(true);
    elements.retry.hidden = true;
    setStatus(`正在加载${INTERVAL_LABELS[state.interval]}历史数据……`);
    try {
      const parameters = new URLSearchParams({ key: state.key, interval: state.interval });
      if (force) parameters.set("force", "1");
      const response = await fetch(`/api/macro-telemetry-history?${parameters}`, {
        cache: "no-store",
        signal: controller.signal,
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload?.error || "历史请求参数无效");
      if (!payload || payload.key !== state.key || payload.interval !== state.interval) {
        throw new Error("历史数据响应与当前指标不匹配");
      }
      const points = normalizePayloadPoints(payload);
      if (payload.ok !== false && !points.length) throw new Error("历史数据没有有效点位");
      state.payload = payload;
      state.points = points;
      state.hoverIndex = -1;
      elements.tooltip.hidden = true;
      updateIdentity(payload);
      updateSummary(payload);
      const errors = Array.isArray(payload.errors)
        ? payload.errors.map((error) => `${error?.source || "数据源"}：${error?.message || "暂不可用"}`).join("；")
        : "";
      if (payload.stale) setStatus(`当前展示最后有效缓存。${errors}`, "stale");
      else if (!points.length) setStatus(errors || "当前周期暂无有效历史数据。", "error");
      else if (errors) setStatus(errors, "stale");
      else setStatus("");
      elements.retry.hidden = Boolean(points.length);
      requestDraw();
    } catch (error) {
      if (error?.name === "AbortError") return;
      setStatus(`历史曲线加载失败：${error?.message || error}`, "error");
      elements.retry.hidden = false;
      if (!state.points.length) {
        elements.coverage.textContent = "暂时无法读取历史曲线。";
        requestDraw();
      }
    } finally {
      if (state.controller === controller) {
        state.controller = null;
        setLoading(false);
      }
    }
  }

  function selectInterval(interval, { load = true } = {}) {
    if (!INTERVALS.includes(interval) || interval === state.interval && !load) return;
    state.interval = interval;
    updateTabs();
    syncUrl();
    if (load) loadHistory(false);
  }

  elements.tabs.addEventListener("click", (event) => {
    const tab = event.target.closest('[role="tab"]');
    if (tab) selectInterval(tab.dataset.interval);
  });
  elements.tabs.addEventListener("keydown", (event) => {
    const isRight = event.key === "ArrowRight";
    const isLeft = event.key === "ArrowLeft";
    if (!isRight && !isLeft) return;
    const direction = isRight ? 1 : -1;
    const current = INTERVALS.indexOf(state.interval);
    const next = INTERVALS[(current + direction + INTERVALS.length) % INTERVALS.length];
    event.preventDefault();
    selectInterval(next);
    elements.tabs.querySelector(`[data-interval="${next}"]`)?.focus();
  });
  elements.refresh.addEventListener("click", () => loadHistory(true));
  elements.retry.addEventListener("click", () => loadHistory(true));
  elements.canvas.addEventListener("pointermove", handlePointer, { passive: true });
  elements.canvas.addEventListener("pointerdown", handlePointer, { passive: true });
  elements.canvas.addEventListener("pointerleave", (event) => {
    if (event.pointerType !== "mouse") return;
    state.hoverIndex = -1;
    elements.tooltip.hidden = true;
    requestDraw();
  });

  const observer = new ResizeObserver(() => requestDraw());
  observer.observe(elements.wrap);
  window.addEventListener("popstate", () => {
    const parameters = new URLSearchParams(window.location.search);
    const nextKey = parameters.get("key") || "";
    const nextInterval = String(parameters.get("interval") || "D").toUpperCase();
    if (!KEYS[nextKey] || !INTERVALS.includes(nextInterval)) return;
    state.key = nextKey;
    state.interval = nextInterval;
    updateTabs();
    loadHistory(false);
  });

  updateTabs();
  if (!KEYS[state.key]) {
    elements.title.textContent = "未知指标";
    elements.subtitle.textContent = "当前链接没有可识别的宏观指标。";
    elements.region.textContent = "--";
    elements.source.textContent = "--";
    elements.coverage.textContent = "请返回指数看板并重新选择指标。";
    elements.refresh.disabled = true;
    elements.tabs.querySelectorAll("button").forEach((button) => { button.disabled = true; });
    elements.retry.hidden = true;
    setStatus("未知指标：未发起历史数据请求。", "error");
    requestDraw();
  } else {
    updateIdentity(null);
    syncUrl();
    loadHistory(false);
  }

  window.macroTelemetryHistoryChart = {
    load: loadHistory,
    selectInterval,
    getState: () => ({ key: state.key, interval: state.interval, pointCount: state.points.length }),
  };
})();
