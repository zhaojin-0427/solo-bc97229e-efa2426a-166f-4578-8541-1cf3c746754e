"""SQLite 连接管理、建表与事务辅助。

设计要点：
- WAL 模式：读写不互斥，后台分组计算只读不阻塞前台写事务。
- 每次操作使用短连接（check_same_thread=False），busy_timeout 让 SQLite
  自身序列化写事务，避免显式应用锁。
- 写操作统一走 write_tx()，遇 SQLITE_BUSY 重试，满足"并发确认"场景。
- 读操作统一走 snapshot_tx()，单条事务内读取，保证查询/下载来自一个
  完整（一致）状态，绝不跨事务拼接。
"""
import sqlite3
import time

from . import config

SCHEMA = """
CREATE TABLE IF NOT EXISTS patients (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    hospital     TEXT NOT NULL,
    hospital_mrn TEXT NOT NULL,
    created_at   TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(hospital, hospital_mrn)
);

CREATE TABLE IF NOT EXISTS relations (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    patient_a   INTEGER NOT NULL REFERENCES patients(id),
    patient_b   INTEGER NOT NULL REFERENCES patients(id),
    status      TEXT NOT NULL CHECK (status IN ('ACTIVE','REVOKED')),
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    revoked_at  TEXT
);

-- 同一对记录最多一条 ACTIVE 关系；撤销后可再次确认（产生新行）。
CREATE UNIQUE INDEX IF NOT EXISTS idx_relation_active
    ON relations(patient_a, patient_b) WHERE status = 'ACTIVE';
CREATE INDEX IF NOT EXISTS idx_relation_b ON relations(patient_b, patient_a);

CREATE TABLE IF NOT EXISTS groups (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    group_no   INTEGER NOT NULL,           -- 组号 = 组内最小记录号
    patient_id INTEGER NOT NULL UNIQUE REFERENCES patients(id),
    compute_id INTEGER NOT NULL            -- 由哪次分组计算发布
);
CREATE INDEX IF NOT EXISTS idx_groups_group_no ON groups(group_no);
CREATE INDEX IF NOT EXISTS idx_groups_compute ON groups(compute_id);

CREATE TABLE IF NOT EXISTS jobs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    kind        TEXT NOT NULL CHECK (kind IN ('IMPORT','RECOMPUTE')),
    status      TEXT NOT NULL CHECK (
                    status IN ('PENDING','RUNNING','DONE','FAILED')),
    filename    TEXT,
    total_rows  INTEGER NOT NULL DEFAULT 0,
    done_rows   INTEGER NOT NULL DEFAULT 0,
    inserted    INTEGER NOT NULL DEFAULT 0,
    reused      INTEGER NOT NULL DEFAULT 0,
    rejected    INTEGER NOT NULL DEFAULT 0,
    detail      TEXT NOT NULL DEFAULT '',   -- 拒绝原因/失败原因等
    seq_cutoff  INTEGER,                    -- RECOMPUTE 任务基于的关系序列号
    file_path   TEXT,                       -- IMPORT 任务的落盘文件（重启后仍可访问）
    worker_id   TEXT,                       -- 当前/最后持有该任务的后台进程
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    started_at  TEXT,
    finished_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
"""

# 早期版本的 jobs 表没有 worker_id 列；对既有库做幂等增补，
# 列一旦存在即跳过，保证升级不丢数据。
_MIGRATIONS = (
    (
        "jobs", "worker_id",
        "ALTER TABLE jobs ADD COLUMN worker_id TEXT",
    ),
)


def connect(db_path=None):
    conn = sqlite3.connect(
        db_path or config.DB_PATH,
        timeout=30,
        check_same_thread=False,
        isolation_level=None,  # 手动事务
    )
    conn.row_factory = sqlite3.Row
    # WAL 模式持久化在库文件中，只需在初始化时设置一次；
    # 每次连接重复设置会引入额外的写锁竞争。
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db():
    conn = connect()
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.executescript(SCHEMA)
        _apply_migrations(conn)
    finally:
        conn.close()


def _apply_migrations(conn):
    for table, column, ddl in _MIGRATIONS:
        cols = {r["name"] for r in conn.execute(
            f"PRAGMA table_info({table})")}
        if column not in cols:
            conn.execute(ddl)


def write_tx(fn, retries=config.WRITE_RETRY):
    """在 IMMEDIATE 写事务中执行 fn(conn)，返回其结果。

    BEGIN IMMEDIATE 立即获取 RESERVED 锁：两个并发确认会被串行化，
    后到者在同一完整关系状态上重新判定（fn 内部校验），从而实现
    "单独合法、合并违规的两次确认只能成功一次"。
    """
    last_err = None
    for attempt in range(retries):
        conn = connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            result = fn(conn)
            conn.execute("COMMIT")
            return result
        except sqlite3.OperationalError as exc:  # database is locked
            last_err = exc
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            time.sleep(0.02 * (attempt + 1))
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()
    raise last_err


class Snapshot:
    """只读快照事务上下文：事务期间看到固定的已提交状态。"""

    def __init__(self):
        self.conn = None

    def __enter__(self):
        self.conn = connect()
        self.conn.execute("BEGIN")  # 延迟事务，WAL 下取得读快照
        return self.conn

    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type is None:
                self.conn.execute("COMMIT")
            else:
                self.conn.execute("ROLLBACK")
        finally:
            self.conn.close()
        return False
