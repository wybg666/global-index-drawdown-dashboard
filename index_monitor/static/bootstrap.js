/* Local-only request token and standalone startup controls. */
(() => {
  "use strict";
  if (location.protocol === "file:") {
    document.addEventListener("DOMContentLoaded", () => {
      document.body.textContent = "这是需要 Python 后端的完整看板。请在项目根目录双击 start.bat，访问终端显示的 http://127.0.0.1:8780/，不要直接打开 HTML。";
    });
    return;
  }
  const nativeFetch = window.fetch.bind(window);
  let tokenPromise;
  function sessionToken() {
    if (!tokenPromise) tokenPromise = nativeFetch("/api/session", {cache: "no-store"})
      .then(async (response) => {
        if (!response.ok) throw new Error("本机页面令牌获取失败，请刷新页面");
        const payload = await response.json();
        return payload.csrfToken;
      }).catch((error) => { tokenPromise = null; throw error; });
    return tokenPromise;
  }
  window.fetch = async (input, options = {}) => {
    const url = new URL(typeof input === "string" ? input : input.url, location.href);
    const method = String(options.method || input.method || "GET").toUpperCase();
    if (url.origin === location.origin && url.pathname.startsWith("/api/") && method === "POST") {
      const headers = new Headers(options.headers || input.headers || {});
      headers.set("X-Monitor-Token", await sessionToken());
      return nativeFetch(input, {...options, headers});
    }
    return nativeFetch(input, options);
  };
  document.addEventListener("DOMContentLoaded", async () => {
    const notice = document.getElementById("standaloneStatus");
    try {
      const health = await (await fetch("/api/health", {cache:"no-store"})).json();
      if (notice) notice.textContent = health.notificationsAllowed
        ? "通知外发已允许；仍须保存自己的 Webhook 并勾选启用。测试按钮会向该机器人发送测试消息。"
        : "当前为本地观察模式，不会外发通知。需要钉钉推送时，以 --allow-notifications 启动，再在本页配置自己的 Webhook。";
    } catch (_) {
      if (notice) notice.textContent = "未连接独立 Python 后端，请用 start.bat 启动。";
    }
    const button = document.getElementById("manualScanBtn");
    const status = document.getElementById("manualScanStatus");
    const market = document.getElementById("manualScanMarket");
    if (!button || !status || !market) return;
    let polling = null;
    async function poll() {
      try {
        const result = await (await fetch("/api/scan-status", {cache:"no-store"})).json();
        if (result.running) {
          status.textContent = `${result.market} 扫描中，分钟线可能较慢；不发送通知。`;
          button.disabled = true;
          if (!polling) polling = setInterval(poll, 5000);
        } else {
          if (polling) clearInterval(polling);
          polling = null;
          button.disabled = false;
          const done = result.result;
          if (done) {
            const summary = done.summary || {};
            const errors = Array.isArray(done.errors) ? done.errors.length : (summary.errors || 0);
            const stale = Number(summary.stale || 0);
            status.textContent = done.ok === false ? `扫描未完成：${done.error || done.message || "请查看信号状态"}` : `${result.market || ""} 扫描结束：处理 ${summary.processed || 0} 项${stale ? `，${stale} 项数据日不符（休市或延迟时不确认）` : ""}${errors ? `，${errors} 项异常` : ""}；未推送。`;
            status.title = (done.items || []).filter(item => item.reason).map(item => `${item.name || item.key}：${item.reason}`).join("；");
            window.indicesPanel?.load(false);
          }
        }
      } catch (_) { status.textContent = "扫描状态暂不可读，请刷新页面查看。"; }
    }
    button.addEventListener("click", async () => {
      button.disabled = true;
      try {
        const phase = {CN:"cn_close",HK:"hk_close",JP_KR:"jp_kr_close",US:"us_close"}[market.value];
        const response = await fetch("/api/index-signals/run", {method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({market:market.value,phase,notify:false})});
        const payload = await response.json();
        if (!response.ok) throw new Error(payload.error || "扫描启动失败");
        status.textContent = "扫描已开始，不发送通知。";
        polling = setInterval(poll, 5000);
        await poll();
      } catch (error) {status.textContent=error.message;button.disabled=false;}
    });
    await poll();
  });
})();
