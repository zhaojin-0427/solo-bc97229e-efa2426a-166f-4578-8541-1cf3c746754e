"""应用配置。所有路径可通过环境变量覆盖，便于容器与本地运行。"""
import os

BASE_DIR = os.environ.get("APP_BASE_DIR", os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.environ.get("APP_DATA_DIR", os.path.join(BASE_DIR, "data"))
DB_PATH = os.environ.get("APP_DB_PATH", os.path.join(DATA_DIR, "patients.db"))
UPLOAD_DIR = os.environ.get("APP_UPLOAD_DIR", os.path.join(DATA_DIR, "uploads"))

# 单次关系/导入事务的写冲突重试次数
WRITE_RETRY = 50
# 后台轮询间隔（秒）
WORKER_POLL_SECONDS = 0.2

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(UPLOAD_DIR, exist_ok=True)
