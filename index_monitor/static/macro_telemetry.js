(() => {
  if (location.protocol === "file:") return;
  const CACHE_KEY = "indexDrawdownMonitor.macro.v1";
  const ITEM_ORDER = ["us10y", "us30y", "cn10y", "cn30y", "kr_margin_financing", "gold", "cn_gold_reserves", "tr_gold_reserves"];
  const YIELD_KEYS = new Set(["us10y", "us30y", "cn10y", "cn30y"]);
  const ITEM_META = {
    us10y: { region: "US", source: "U.S. Treasury", unit: "%", changeUnit: "bp" },
    us30y: { region: "US", source: "U.S. Treasury", unit: "%", changeUnit: "bp" },
    cn10y: { region: "CN", source: "ChinaBond（中央国债登记结算有限责任公司）", unit: "%", changeUnit: "bp" },
    cn30y: { region: "CN", source: "ChinaBond（中央国债登记结算有限责任公司）", unit: "%", changeUnit: "bp" },
    kr_margin_financing: { region: "KR", source: "KOFIA FreeSIS", unit: "万亿韩元", changeUnit: "万亿韩元" },
    gold: { region: "Global", source: "Yahoo Finance", unit: "USD/oz", changeUnit: "USD" },
    cn_gold_reserves: { region: "CN", source: "State Administration of Foreign Exchange (SAFE)", unit: "吨", changeUnit: "吨" },
    tr_gold_reserves: { region: "TR", source: "Central Bank of the Republic of Türkiye (TCMB) EVDS3", unit: "吨", changeUnit: "吨" },
  };

  const byId = (id) => document.getElementById(id);
  const section = byId("macroTelemetrySection");
  const cardsEl = byId("macroTelemetryCards");
  const errorEl = byId("macroTelemetryError");
  const updatedAtEl = byId("macroTelemetryUpdatedAt");
  const cardTemplate = byId("macroTelemetryCardTemplate");

  if (!section || !cardsEl || !errorEl || !updatedAtEl || !cardTemplate) return;

  const state = { loading: null, hasData: false };

  function isObject(value) {
    return Boolean(value) && typeof value === "object" && !Array.isArray(value);
  }

  function isFiniteNumber(value) {
    return value !== null && value !== "" && Number.isFinite(Number(value));
  }

  function isDate(value) {
    return typeof value === "string" && /^\d{4}-\d{2}-\d{2}$/.test(value);
  }

  function isRisk(value) {
    return (
      isObject(value) &&
      (value.level === "normal" || value.level === "watch" || value.level === "high") &&
      typeof value.message === "string" &&
      Boolean(value.message.trim())
    );
  }

  function isItem(item) {
    if (!isObject(item) || !ITEM_META[item.key]) return false;
    const meta = ITEM_META[item.key];
    const centralBankGold = item.key === "cn_gold_reserves" || item.key === "tr_gold_reserves";
    return (
      item.region === meta.region &&
      item.source === meta.source &&
      item.unit === meta.unit &&
      item.changeUnit === meta.changeUnit &&
      isFiniteNumber(item.value) &&
      Number(item.value) > 0 &&
      (item.change === null || isFiniteNumber(item.change)) &&
      (item.changePct === null || isFiniteNumber(item.changePct)) &&
      isDate(item.asOf) &&
      (item.previousAsOf === null || isDate(item.previousAsOf)) &&
      (!centralBankGold || (isFiniteNumber(item.previousValue) && Number(item.previousValue) > 0 && isDate(item.previousAsOf))) &&
      (!YIELD_KEYS.has(item.key) || item.risk === undefined || isRisk(item.risk)) &&
      typeof item.stale === "boolean"
    );
  }

  function isPayload(payload) {
    if (!isObject(payload) || !Array.isArray(payload.items) || !payload.items.length) return false;
    const keys = payload.items.map((item) => item?.key);
    return payload.items.every(isItem) && new Set(keys).size === keys.length;
  }

  function cacheSafePayload(payload) {
    const cached = JSON.parse(JSON.stringify(payload));
    delete cached.cache;
    return cached;
  }

  function saveCache(payload) {
    if (!isPayload(payload)) return;
    try {
      window.localStorage.setItem(CACHE_KEY, JSON.stringify({ savedAt: Date.now(), payload: cacheSafePayload(payload) }));
    } catch (_error) {
      // Locked-down WebViews may deny storage; live data remains available.
    }
  }

  function signed(value, digits) {
    if (!isFiniteNumber(value)) return "--";
    const number = Number(value);
    return `${number > 0 ? "+" : ""}${number.toFixed(digits)}`;
  }

  function formatValue(item) {
    if (item.unit === "%") return Number(item.value).toFixed(2);
    return Number(item.value).toFixed(3);
  }

  function formatChange(item) {
    if (!isFiniteNumber(item.change)) return "较前值：--";
    if (item.changeUnit === "bp") return "较前值：" + signed(item.change, 1) + " bp";
    if (item.changeUnit === "USD") return "较前值：" + signed(item.change, 2) + " USD";
    if (item.changeUnit === "吨") {
      const previous = isFiniteNumber(item.previousValue)
        ? Number(item.previousValue).toFixed(3) + " 吨"
        : "数据积累中";
      const dateText = item.previousAsOf ? "（" + item.previousAsOf + "）" : "";
      const change = isFiniteNumber(item.change) ? signed(item.change, 3) + " 吨" : "--";
      return "前值：" + previous + dateText + "｜变动：" + change;
    }
    const percent = isFiniteNumber(item.changePct) ? "（" + signed(item.changePct, 2) + "%）" : "";
    return "较前值：" + signed(item.change, 3) + " 万亿韩元" + percent;
  }

  function renderErrors(errors) {
    if (!Array.isArray(errors) || !errors.length) {
      errorEl.hidden = true;
      errorEl.textContent = "";
      return;
    }
    errorEl.hidden = false;
    errorEl.textContent = `数据源提示：${errors
      .map((error) => `${error?.source || "未知来源"}：${error?.message || "暂不可用"}`)
      .join("；")}`;
  }

  function render(payload, { cached = false } = {}) {
    const itemsByKey = new Map(payload.items.filter(isItem).map((item) => [item.key, item]));
    cardsEl.replaceChildren();
    ITEM_ORDER.forEach((key) => {
      const item = itemsByKey.get(key);
      if (!item) return;
      const card = cardTemplate.content.firstElementChild.cloneNode(true);
      card.dataset.key = item.key;
      card.dataset.region = item.region;
      card.href = `/macro-telemetry-detail?key=${encodeURIComponent(item.key)}`;
      card.setAttribute("aria-label", `查看${item.name}完整历史曲线`);
      card.querySelector('[data-field="region"]').textContent = item.region;
      card.querySelector('[data-field="source"]').textContent = item.source;
      card.querySelector('[data-field="name"]').textContent = item.name;
      card.querySelector('[data-field="value"]').textContent = formatValue(item);
      card.querySelector('[data-field="unit"]').textContent = item.unit;
      card.querySelector('[data-field="change"]').textContent = formatChange(item);
      card.querySelector('[data-field="asOf"]').textContent = `数据日：${item.asOf}${item.previousAsOf ? `｜前值日：${item.previousAsOf}` : ""}`;
      var sigEl = card.querySelector('[data-field="signals"]');
      if (sigEl) {
        sigEl.replaceChildren();
        const pairRiskCard = item.key === "us30y" || item.key === "cn30y";
        const risk = item.risk;
        if (pairRiskCard && !item.stale && isRisk(risk) && (risk.level === "watch" || risk.level === "high")) {
          const riskEl = document.createElement("span");
          riskEl.className = `macro-signal macro-signal-risk is-${risk.level}`;
          riskEl.setAttribute("role", "status");
          riskEl.setAttribute("aria-live", "polite");
          riskEl.textContent = "10Y/30Y：风险提示：" + risk.message;
          sigEl.appendChild(riskEl);
        }
        if (item.trend && item.trend !== "首次采集") {
          var t2 = document.createElement("span");
          t2.className = "macro-signal-trend";
          t2.textContent = item.trend;
          sigEl.appendChild(t2);
        }
        if (item.topSignal) {
          var s2 = document.createElement("span");
          s2.className = "macro-signal macro-signal-top";
          s2.textContent = "\u26A0 \u9876\u90E8\uFF1A" + item.topSignal;
          sigEl.appendChild(s2);
        }
        if (item.bottomSignal) {
          var s3 = document.createElement("span");
          s3.className = "macro-signal macro-signal-bottom";
          s3.textContent = "\u25BC \u5E95\u90E8\uFF1A" + item.bottomSignal;
          sigEl.appendChild(s3);
        }
        if (item.actionSignal) {
          var s4 = document.createElement("span");
          s4.className = "macro-signal macro-signal-action";
          s4.textContent = "\uD83C\uDFC6 " + item.actionSignal;
          sigEl.appendChild(s4);
        }
        if (item.pctReserves != null) {
          var s5 = document.createElement("span");
          s5.className = "macro-signal macro-signal-pct";
          s5.textContent = "\uD83D\uDCCA \u5360\u5916\u6C47\u50A8\u5907 " + Number(item.pctReserves).toFixed(1) + "%";
          sigEl.appendChild(s5);
        }
      }
      const staleEl = card.querySelector('[data-field="stale"]');
      staleEl.hidden = !(cached || item.stale);
      cardsEl.appendChild(card);
    });
    state.hasData = cardsEl.childElementCount > 0;
    const cacheState = payload.cache?.state === "stale" || payload.cache?.refreshing;
    updatedAtEl.textContent = `${payload.generatedAt || "--"}${cached ? "（浏览器缓存）" : cacheState ? "（缓存刷新中）" : ""}`;
    renderErrors(payload.errors);
  }

  function restoreCache() {
    try {
      const raw = window.localStorage.getItem(CACHE_KEY);
      if (!raw) return false;
      const record = JSON.parse(raw);
      if (!isObject(record) || !isPayload(record.payload)) {
        window.localStorage.removeItem(CACHE_KEY);
        return false;
      }
      render(record.payload, { cached: true });
      return true;
    } catch (_error) {
      return false;
    }
  }

  async function load(force = false) {
    if (state.loading) return state.loading;
    state.loading = (async () => {
      try {
        const response = await fetch(`/api/macro-telemetry${force ? "?force=1" : ""}`, { cache: "no-store" });
        const payload = await response.json();
        if (!response.ok || !isPayload(payload) || payload.ok === false) {
          throw new Error(payload?.error || payload?.message || "宏观遥测暂不可用");
        }
        saveCache(payload);
        render(payload, { cached: payload.cache?.state === "stale" });
        return payload;
      } catch (error) {
        errorEl.hidden = false;
        errorEl.textContent = `宏观遥测加载失败：${error?.message || error}${state.hasData ? "；保留上次数据。" : ""}`;
        return null;
      } finally {
        state.loading = null;
      }
    })();
    return state.loading;
  }

  restoreCache();
  window.macroTelemetryPanel = { load, restoreCache };
})();
