(() => {
  if (location.protocol === "file:") return;
  const PAGE_SIZE = 50;
  const state = { events: [], filtered: [], page: 1, retentionLimit: 0 };
  const byId = (id) => document.getElementById(id);
  const rowsEl = byId("indexSignalHistoryRows");
  const summaryEl = byId("indexSignalHistorySummary");
  const totalEl = byId("indexSignalHistoryTotal");
  const visibleEl = byId("indexSignalHistoryVisible");
  const dateEl = byId("indexSignalHistoryDate");
  const indexEl = byId("indexSignalHistoryIndex");
  const timeframeEl = byId("indexSignalHistoryTimeframe");
  const statusEl = byId("indexSignalHistoryStatus");
  const resetEl = byId("indexSignalHistoryReset");
  const prevEl = byId("indexSignalHistoryPrev");
  const nextEl = byId("indexSignalHistoryNext");
  const pageEl = byId("indexSignalHistoryPage");

  const STATUS_LABELS = {
    none: "无信号",
    warning: "预警",
    confirmed: "确认",
    invalidated: "失效",
    conflict: "震荡冲突",
  };
  const PHASE_LABELS = {
    cn_morning: "A股早盘",
    cn_midday: "A股午间",
    cn_tail: "A股14:55尾盘判断",
    cn_close: "A股收盘",
    hk_morning: "港股早盘",
    hk_midday: "港股午间",
    hk_close: "港股收盘",
    jp_kr_morning: "日韩早盘",
    jp_kr_midday: "日韩午间",
    jp_kr_close: "日韩收盘",
    us_close: "美股收盘",
  };
  const TIMEFRAME_LABELS = {
    short: "短期·15分钟",
    medium: "中期·60分钟",
    long: "长期·日线",
  };

  function statusLabel(value) {
    return STATUS_LABELS[String(value || "none")] || String(value || "--");
  }

  function formatTime(value) {
    if (!value) return "--";
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) return String(value);
    return new Intl.DateTimeFormat("zh-CN", {
      timeZone: "Asia/Shanghai",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
    }).format(parsed);
  }

  function appendCell(row, primary, secondary = "", className = "") {
    const cell = document.createElement("td");
    if (className) cell.className = className;
    const strong = document.createElement("strong");
    strong.textContent = primary;
    cell.appendChild(strong);
    if (secondary) {
      const small = document.createElement("small");
      small.textContent = secondary;
      cell.appendChild(small);
    }
    row.appendChild(cell);
  }

  function notificationText(event) {
    if (!event.shouldNotify) return "不触发";
    if (event.notificationStatus === "success") return "已推送";
    if (event.notificationStatus === "failed") return "推送失败";
    return "待处理";
  }

  function renderRows() {
    rowsEl.replaceChildren();
    const pageCount = Math.max(1, Math.ceil(state.filtered.length / PAGE_SIZE));
    state.page = Math.min(Math.max(1, state.page), pageCount);
    const start = (state.page - 1) * PAGE_SIZE;
    const pageEvents = state.filtered.slice(start, start + PAGE_SIZE);

    if (!pageEvents.length) {
      const row = document.createElement("tr");
      const cell = document.createElement("td");
      cell.colSpan = 7;
      cell.className = "empty-row";
      cell.textContent = "当前筛选条件下没有状态变化。";
      row.appendChild(cell);
      rowsEl.appendChild(row);
    }

    pageEvents.forEach((event) => {
      const row = document.createElement("tr");
      const name = String(event.indexName || event.name || event.indexKey || "指数");
      const code = String(event.indexKey || event.key || "");
      const fromStatus = statusLabel(event.fromStatus);
      const toStatus = statusLabel(event.toStatus || event.status);
      const score = Number.isFinite(Number(event.score)) ? `${Number(event.score).toFixed(0)} 分` : "--";
      appendCell(row, String(event.marketDate || "--"), formatTime(event.changedAt));
      appendCell(row, name, code && code !== name ? code : "");
      appendCell(row, TIMEFRAME_LABELS[event.timeframe] || event.timeframe || "--", event.regionLabel || event.region || "");
      appendCell(row, `${fromStatus} → ${toStatus}`, event.direction === "bottom" ? "底部方向" : "顶部方向", `status-${event.toStatus || event.status || "none"}`);
      appendCell(row, score, event.pattern || "");
      appendCell(row, PHASE_LABELS[event.scanPhase] || event.scanPhase || "--");
      appendCell(row, notificationText(event), event.notificationSuppressionReason || "");
      rowsEl.appendChild(row);
    });

    visibleEl.textContent = String(state.filtered.length);
    pageEl.textContent = `第 ${state.page} / ${pageCount} 页`;
    prevEl.disabled = state.page <= 1;
    nextEl.disabled = state.page >= pageCount;
  }

  function applyFilters({ resetPage = true } = {}) {
    const date = dateEl.value;
    const indexKey = indexEl.value;
    const timeframe = timeframeEl.value;
    const status = statusEl.value;
    state.filtered = state.events.filter((event) => {
      if (date && String(event.marketDate || "") !== date) return false;
      if (indexKey && String(event.indexKey || "") !== indexKey) return false;
      if (timeframe && String(event.timeframe || "") !== timeframe) return false;
      if (status && String(event.toStatus || event.status || "") !== status) return false;
      return true;
    });
    if (resetPage) state.page = 1;
    renderRows();
  }

  function populateIndexOptions() {
    const names = new Map();
    state.events.forEach((event) => {
      const key = String(event.indexKey || "");
      if (!key) return;
      names.set(key, String(event.indexName || event.name || key));
    });
    [...names.entries()]
      .sort((left, right) => left[1].localeCompare(right[1], "zh-CN"))
      .forEach(([key, name]) => {
        const option = document.createElement("option");
        option.value = key;
        option.textContent = `${name}（${key}）`;
        indexEl.appendChild(option);
      });
  }

  async function loadHistory() {
    try {
      const response = await fetch("/api/index-signal-history", { cache: "no-store" });
      const payload = await response.json();
      if (!response.ok || payload.ok === false) throw new Error(payload.error || "历史读取失败");
      state.events = Array.isArray(payload.events) ? payload.events : [];
      state.retentionLimit = Number(payload.retentionLimit || 0);
      totalEl.textContent = String(payload.total ?? state.events.length);
      summaryEl.textContent = state.events.length
        ? `已读取 ${state.events.length} 条状态变化；当前存储上限 ${state.retentionLimit || "--"} 条，按最新变化倒序展示。`
        : "当前还没有已保存的状态变化。";
      populateIndexOptions();
      applyFilters();
    } catch (error) {
      summaryEl.textContent = `历史状态变化加载失败：${error.message || error}`;
      rowsEl.replaceChildren();
      const row = document.createElement("tr");
      const cell = document.createElement("td");
      cell.colSpan = 7;
      cell.className = "empty-row error-row";
      cell.textContent = "无法读取历史记录。";
      row.appendChild(cell);
      rowsEl.appendChild(row);
    }
  }

  [dateEl, indexEl, timeframeEl, statusEl].forEach((control) => {
    control.addEventListener("change", () => applyFilters());
  });
  resetEl.addEventListener("click", () => {
    dateEl.value = "";
    indexEl.value = "";
    timeframeEl.value = "";
    statusEl.value = "";
    applyFilters();
  });
  prevEl.addEventListener("click", () => {
    state.page -= 1;
    renderRows();
  });
  nextEl.addEventListener("click", () => {
    state.page += 1;
    renderRows();
  });

  loadHistory();
})();
