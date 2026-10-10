"""Start only this standalone dashboard; never import the original workbench."""
from __future__ import annotations

import argparse
import sys
import webbrowser
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


def main():
    parser = argparse.ArgumentParser(description="Local global-index drawdown / signal dashboard")
    parser.add_argument("--port", type=int, default=8780)
    parser.add_argument("--open-browser", action="store_true")
    parser.add_argument("--no-scheduler", action="store_true", help="do not run scheduled signal scans")
    parser.add_argument("--allow-notifications", action="store_true", help="permit configured DingTalk notifications; settings remain disabled by default")
    args = parser.parse_args()
    if sys.version_info < (3, 11) or not 0 <= args.port <= 65535:
        parser.error("需要 Python 3.11+，端口范围为 0–65535")
    try:
        for name in ("Asia/Shanghai", "Asia/Hong_Kong", "America/New_York", "Asia/Tokyo", "Asia/Seoul"):
            ZoneInfo(name)
        from index_monitor.web import create_server
        from index_monitor import runtime
        from index_monitor.paths import DATA_DIR
    except (ImportError, ZoneInfoNotFoundError) as exc:
        print(f"依赖未就绪（{type(exc).__name__}），请执行：python -m pip install -r requirements.txt", file=sys.stderr)
        return 1
    try:
        server = create_server(args.port, allow_notifications=args.allow_notifications, scheduler=not args.no_scheduler)
    except OSError:
        print(f"无法监听端口 {args.port}，请换端口；未停止任何其他进程。", file=sys.stderr)
        return 1
    runtime.ensure_index_signal_scheduler(enabled=not args.no_scheduler, allow_notifications=args.allow_notifications)
    url = f"http://127.0.0.1:{server.server_port}/"
    print(f"全球指数回撤与信号看板：{url}", flush=True)
    print(f"运行数据目录：{DATA_DIR}", flush=True)
    print("通知外发：" + ("允许（仍需页面配置并启用）" if args.allow_notifications else "关闭"), flush=True)
    print("保持此窗口打开；Ctrl+C 退出。", flush=True)
    if args.open_browser:
        try:
            webbrowser.open(url)
        except Exception:
            print("未能自动打开浏览器，请手动打开上面的本机地址。", flush=True)
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        print("\n服务已停止。", flush=True)
    finally:
        runtime.stop_index_signal_scheduler()
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
