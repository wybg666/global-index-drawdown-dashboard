"""Loopback-only routes for the extracted dashboard, with protected writes."""
from __future__ import annotations

import copy
import hmac
import json
import secrets
import threading
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from . import __version__, indices, runtime
from .notifier import validate_index_webhook
from .macro_telemetry import build_macro_telemetry_bundle
from .macro_telemetry_history import build_macro_telemetry_history, validate_history_request

STATIC = Path(__file__).resolve().parent / "static"
STATIC_FILES = {
    name: ("text/html; charset=utf-8" if name.endswith(".html") else
           "text/css; charset=utf-8" if name.endswith(".css") else "text/javascript; charset=utf-8")
    for name in (
        "indices.html", "indices.js", "indices.css", "bootstrap.js",
        "index-signal-history.html", "index-signal-history.js", "index-signal-history.css",
        "macro_telemetry.js", "macro-telemetry-detail.html", "macro-telemetry-detail.js",
        "macro-telemetry-detail.css", "about.html",
    )
}
ALIASES = {"/": "indices.html", "/indices": "indices.html", "/indices/": "indices.html",
           "/index-signal-history": "index-signal-history.html",
           "/macro-telemetry-detail": "macro-telemetry-detail.html", "/about": "about.html"}


class MonitorServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = False

    def __init__(self, port: int, *, allow_notifications: bool = False, scheduler: bool = True):
        self.csrf_token = secrets.token_urlsafe(32)
        self.allow_notifications = bool(allow_notifications)
        self.scheduler_enabled = bool(scheduler)
        self.scan_lock = threading.Lock()
        self.scan = {"running": False, "result": None}
        super().__init__(("127.0.0.1", port), Handler)


class Handler(BaseHTTPRequestHandler):
    server_version = "IndexDrawdownMonitor/" + __version__
    sys_version = ""

    def setup(self):
        super().setup()
        self.connection.settimeout(15)

    def _json(self, code: int, data: object):
        self._bytes(code, json.dumps(data, ensure_ascii=False, allow_nan=False).encode("utf-8"), "application/json; charset=utf-8")

    def _bytes(self, code: int, body: bytes, content_type: str):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Referrer-Policy", "no-referrer")
        self.end_headers()
        if self.command != "HEAD":
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

    def _target(self):
        hosts = self.headers.get_all("Host", [])
        allowed = {f"127.0.0.1:{self.server.server_port}", f"localhost:{self.server.server_port}"}
        if len(hosts) != 1 or hosts[0].lower() not in allowed:
            raise ValueError("Invalid Host header")
        parsed = urlsplit(self.path)
        if parsed.scheme or parsed.netloc:
            raise ValueError("Absolute URL not allowed")
        return parsed, parse_qs(parsed.query, max_num_fields=20)

    def do_GET(self):
        try:
            parsed, qs = self._target()
        except ValueError:
            self._json(400, {"ok": False, "error": "请求地址无效"})
            return
        path = parsed.path
        force = qs.get("force", ["0"])[0] == "1"
        try:
            if path == "/indices/":
                self.send_response(302)
                self.send_header("Location", "/indices")
                self.send_header("Content-Length", "0")
                self.end_headers()
            elif path == "/api/health":
                self._json(200, {"ok": True, "service": "index-drawdown-monitor", "version": __version__,
                                 "notificationsAllowed": self.server.allow_notifications,
                                 "schedulerEnabled": self.server.scheduler_enabled})
            elif path == "/api/session":
                self._json(200, {"ok": True, "csrfToken": self.server.csrf_token})
            elif path == "/api/indices":
                self._json(200, indices.build_index_dashboard(force_refresh=force,
                           signal_snapshot=runtime._INDEX_SIGNAL_STORE.snapshot(), allow_stale=not force))
            elif path == "/api/index-signals":
                self._json(200, runtime.get_index_signal_payload())
            elif path == "/api/index-signal-history":
                self._json(200, runtime.get_index_signal_history_payload())
            elif path == "/api/index-signal-settings":
                signals = runtime.get_index_signal_payload()
                self._json(200, {"ok": True, "settings": runtime.load_index_signal_notification_settings(mask_webhook=True),
                                 "latestTest": signals.get("latestTest"), "latestPush": signals.get("latestPush"),
                                 "notificationsAllowed": self.server.allow_notifications})
            elif path == "/api/scan-status":
                with self.server.scan_lock:
                    status = copy.deepcopy(self.server.scan)
                self._json(200, {"ok": True, **status})
            elif path == "/api/macro-telemetry":
                self._json(200, build_macro_telemetry_bundle(force_refresh=force, allow_stale=not force))
            elif path == "/api/macro-telemetry-history":
                key, interval = validate_history_request(qs.get("key", [""])[0], qs.get("interval", ["D"])[0])
                self._json(200, build_macro_telemetry_history(key, interval, force_refresh=force, allow_stale=not force))
            else:
                filename = ALIASES.get(path, path.lstrip("/"))
                if filename not in STATIC_FILES:
                    self._json(404, {"ok": False, "error": "Not found"})
                    return
                self._bytes(200, (STATIC / filename).read_bytes(), STATIC_FILES[filename])
        except ValueError:
            self._json(400, {"ok": False, "error": "请求参数或上游数据格式无效"})
        except Exception as exc:
            # Never return raw URLs, local paths, or notification credentials.
            self._json(503, {"ok": False, "error": f"该模块暂不可用（{type(exc).__name__}）"})

    def _body(self):
        self._target()
        host = self.headers.get("Host", "")
        origin = self.headers.get("Origin")
        if origin is not None and origin != f"http://{host}":
            raise PermissionError("Cross-origin write blocked")
        if self.headers.get("Sec-Fetch-Site", "") == "cross-site":
            raise PermissionError("Cross-site write blocked")
        token = self.headers.get("X-Monitor-Token", "")
        if not hmac.compare_digest(token, self.server.csrf_token):
            raise PermissionError("Page token required")
        if self.headers.get_content_type() != "application/json" or self.headers.get("Transfer-Encoding"):
            raise ValueError("JSON body required")
        length = int(self.headers.get("Content-Length", "0"))
        if not 0 < length <= 32768:
            raise ValueError("Invalid body length")
        body = json.loads(self.rfile.read(length).decode("utf-8"))
        if not isinstance(body, dict):
            raise ValueError("Object required")
        return body

    @staticmethod
    def _validate_webhook(settings):
        value = settings.get("indexSignalDingTalkWebhook")
        if value is None or value == "" or value == runtime.INDEX_SIGNAL_WEBHOOK_MASK:
            return
        if not isinstance(value, str):
            raise ValueError("Invalid Webhook")
        validate_index_webhook(value)

    def do_POST(self):
        try:
            payload = self._body()
            path = urlsplit(self.path).path
            if path in ("/api/index-signal-settings", "/api/index-signal-settings/test"):
                settings = payload.get("settings", payload)
                if not isinstance(settings, dict):
                    raise ValueError("Invalid settings")
                self._validate_webhook(settings)
                if path.endswith("/test"):
                    if not self.server.allow_notifications:
                        self._json(403, {"ok": False, "error": "当前进程未允许外发通知，请使用 --allow-notifications 启动"})
                        return
                    result = runtime.test_index_signal_notification(settings)
                    self._json(200 if result.get("ok") else 400, result)
                else:
                    saved = runtime.save_index_signal_notification_settings(settings, mask_webhook=True)
                    self._json(200, {"ok": True, "message": "设置已保存（Webhook 不回显）", "settings": saved})
            elif path == "/api/index-signals/run":
                market, phase = runtime._validate_index_signal_phase(str(payload.get("market", "")).upper(), str(payload.get("phase", "")).lower())
                notify = payload.get("notify") is True
                if notify and not self.server.allow_notifications:
                    raise PermissionError("Notifications disabled")
                with self.server.scan_lock:
                    if self.server.scan["running"]:
                        self._json(409, {"ok": False, "error": "已有扫描进行中，请稍后查看状态"})
                        return
                    self.server.scan = {"running": True, "market": market, "phase": phase,
                                        "startedAt": datetime.now().isoformat(timespec="seconds"), "result": None}
                def run():
                    try:
                        result = runtime._run_index_signal_scan(market=market, phase=phase, trigger="manual", notify=notify)
                    except Exception as exc:
                        result = {"ok": False, "error": f"扫描失败（{type(exc).__name__}）"}
                    with self.server.scan_lock:
                        self.server.scan.update(running=False, result=result, completedAt=datetime.now().isoformat(timespec="seconds"))
                threading.Thread(target=run, name="manual-index-scan", daemon=True).start()
                self._json(202, {"ok": True, "message": "扫描已开始；本次不自动发送通知" if not notify else "扫描已开始"})
            else:
                self._json(404, {"ok": False, "error": "Not found"})
        except PermissionError:
            self._json(403, {"ok": False, "error": "写操作未获本机页面令牌或通知权限，请刷新页面重试"})
        except (ValueError, UnicodeError, TypeError):
            self._json(400, {"ok": False, "error": "参数无效；Webhook 必须为钉钉 HTTPS robot/send 地址"})
        except Exception as exc:
            self._json(503, {"ok": False, "error": f"操作失败（{type(exc).__name__}）"})

    def _not_allowed(self):
        self.close_connection = True
        self._json(405, {"ok": False, "error": "Method not allowed"})

    do_PUT = do_DELETE = do_PATCH = do_OPTIONS = do_HEAD = _not_allowed


def create_server(port=8780, *, allow_notifications=False, scheduler=True):
    if isinstance(port, bool) or not isinstance(port, int) or not 0 <= port <= 65535:
        raise ValueError("Invalid port")
    return MonitorServer(port, allow_notifications=allow_notifications, scheduler=scheduler)
