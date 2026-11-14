"""独立进程测试用的应用引导脚本。

由 tests/test_process_handoff.py 以子进程方式启动（python -I），
模拟一个真实部署的 Gunicorn worker 进程：导入 app 包即通过
create_app() 启动后台 worker 线程，并另起一个 HTTP 服务线程验证
非领导进程也能立即提供上传/核对/进度/下载能力。

进程间同步全部走文件：
- APP_SYNC_DIR/claimed-<kind>-<job_id>      任务被某个进程认领
- APP_SYNC_DIR/hold-<kind>-<job_id>        认领后阻塞（删除后继续）
- APP_SYNC_DIR/hold-import-row-<jid>-<n>   导入到第 n 行后阻塞
- APP_SYNC_DIR/leader_acquired             本进程取得后台领导锁
- APP_SYNC_DIR/http_ready                  HTTP 服务线程就绪
"""
import os
import sys
import threading

# 允许以 python -I <此脚本> 直接运行：-I 会忽略 PYTHONPATH 与用户
# site-packages，因此依赖目录与仓库根都要显式加入 sys.path
_pylibs = os.environ.get("APP_PYLIBS")
if _pylibs:
    sys.path.insert(0, _pylibs)
sys.path.insert(0, os.environ["APP_REPO_ROOT"])

from werkzeug.serving import make_server  # noqa: E402

from app import config  # noqa: E402
from app.web import create_app  # noqa: E402


def _mark(name):
    try:
        with open(os.path.join(config.SYNC_DIR, name), "w") as fh:
            fh.write(str(os.getpid()))
    except OSError:
        pass


def main():
    httpd = make_server("127.0.0.1", 0, create_app(), threaded=True)
    port = httpd.server_port
    with open(os.environ["APP_PORT_FILE"], "w") as fh:
        fh.write(str(port))
    threading.Thread(
        target=httpd.serve_forever, name="test-http", daemon=True
    ).start()
    _mark("http_ready")

    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.shutdown()


if __name__ == "__main__":
    main()
