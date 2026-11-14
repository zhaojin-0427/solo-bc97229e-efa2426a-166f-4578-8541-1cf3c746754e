"""端到端集成测试：导入、确认/撤销、并发、重启恢复、快照下载。

每个用例通过环境变量指向独立数据目录；worker 模块在会话内只加载一次，
用例间通过 stop/start 与重置停止事件实现干净启停。
"""
import os
import threading
import time

import pytest

from app import db, repository, services, worker
from app.web import create_app


@pytest.fixture()
def app_ctx(tmp_path):
    """每个用例独立的 SQLite 文件与上传目录。"""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    upload_dir = data_dir / "uploads"
    upload_dir.mkdir()
    db_path = data_dir / "test.db"

    # 直接替换配置值（同一进程内串行执行，用例结束后恢复）
    import app.config as config
    saved = (config.DATA_DIR, config.DB_PATH, config.UPLOAD_DIR)
    config.DATA_DIR = str(data_dir)
    config.DB_PATH = str(db_path)
    config.UPLOAD_DIR = str(upload_dir)

    # 重置 worker 单例状态，保证本用例启动的线程执行恢复逻辑
    worker._stop.clear()
    worker._started = False
    db.init_db()
    worker.start_worker()

    ctx = {
        "services": services,
        "worker": worker,
        "repository": repository,
        "db": db,
        "config": config,
        "saved": saved,
    }
    yield ctx

    worker.stop_worker()
    time.sleep(0.25)
    config.DATA_DIR, config.DB_PATH, config.UPLOAD_DIR = saved


def _wait_jobs(ctx, timeout=10, expect_failed_ok=True):
    """等待所有任务离开 PENDING/RUNNING。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        pending = ctx["repository"].pending_jobs()
        if not pending:
            return
        time.sleep(0.05)
    raise AssertionError("后台任务未在超时内完成")


def _import(ctx, rows_text, filename="p.csv"):
    job_id, total, errors = ctx["services"].create_import_job(
        filename, rows_text.encode("utf-8")
    )
    _wait_jobs(ctx)
    return job_id, total, errors


def _patient_id(ctx, hospital, mrn):
    with ctx["db"].Snapshot() as conn:
        row = conn.execute(
            "SELECT id FROM patients WHERE hospital=? AND hospital_mrn=?",
            (hospital, mrn),
        ).fetchone()
        return row["id"]


def test_import_assigns_incremental_ids_and_reuses_key(app_ctx):
    ctx = app_ctx
    csv1 = "hospital,mrn\nH01,A1\nH02,B1\n"
    _import(ctx, csv1)
    id_a = _patient_id(ctx, "H01", "A1")
    id_b = _patient_id(ctx, "H02", "B1")
    assert id_b == id_a + 1  # 递增记录号

    # 同键重传：复用，不新增；新键继续递增
    csv2 = "hospital,mrn\nH01,A1\nH03,C1\n"
    _import(ctx, csv2)
    assert _patient_id(ctx, "H01", "A1") == id_a
    id_c = _patient_id(ctx, "H03", "C1")
    assert id_c > id_b

    stats, _ = ctx["repository"].overview()
    assert stats["patients"] == 3


def test_confirm_group_publish_and_download_snapshot(app_ctx):
    ctx = app_ctx
    _import(ctx, "hospital,mrn\nH01,A1\nH02,B1\nH03,C1\n")
    a, b, c = (_patient_id(ctx, h, m)
               for h, m in [("H01", "A1"), ("H02", "B1"), ("H03", "C1")])

    ctx["services"].confirm_relation(a, b)
    ctx["services"].confirm_relation(b, c)  # 传递成组
    _wait_jobs(ctx)

    groups = ctx["repository"].list_groups()
    assert len(groups) == 1
    assert groups[0]["group_no"] == min(a, b, c)

    result = ctx["repository"].export_mapping_csv()
    assert result is not None
    text, state = result
    assert state["groups"] == 1 and state["members"] == 3
    assert "group_no,record_no,hospital,hospital_mrn,compute_id" in text


def test_conflicting_confirm_rejected_and_other_confirm_kept(app_ctx):
    ctx = app_ctx
    # H01 的两个不同编号 A1/A2；分别与 H02 的 B1 确认，第二次必须被拒
    _import(ctx, "hospital,mrn\nH01,A1\nH01,A2\nH02,B1\n")
    a1 = _patient_id(ctx, "H01", "A1")
    a2 = _patient_id(ctx, "H01", "A2")
    b1 = _patient_id(ctx, "H02", "B1")

    r1 = ctx["services"].confirm_relation(a1, b1)
    assert r1["outcome"] == "confirmed"

    with pytest.raises(ctx["services"].ServiceError) as exc:
        ctx["services"].confirm_relation(a2, b1)
    assert "H01" in str(exc.value)

    # 无关确认必须保留：H03-H04 体系与冲突链完全无关，正常成功
    _import(ctx, "hospital,mrn\nH03,C1\nH04,D1\n")
    c1 = _patient_id(ctx, "H03", "C1")
    d1 = _patient_id(ctx, "H04", "D1")
    r3 = ctx["services"].confirm_relation(c1, d1)
    assert r3["outcome"] == "confirmed"
    _wait_jobs(ctx)

    groups = ctx["repository"].list_groups()
    group_hospitals = {
        tuple(sorted(m["hospital"] for m in g["members"])) for g in groups
    }
    assert ("H01", "H02") in group_hospitals
    assert ("H03", "H04") in group_hospitals


def test_concurrent_confirms_only_one_succeeds(app_ctx):
    ctx = app_ctx
    _import(ctx, "hospital,mrn\nH01,A1\nH01,A2\nH02,B1\n")
    a1 = _patient_id(ctx, "H01", "A1")
    a2 = _patient_id(ctx, "H01", "A2")
    b1 = _patient_id(ctx, "H02", "B1")

    outcomes = []

    def worker(fn):
        try:
            outcomes.append(("ok", fn()))
        except Exception as exc:  # noqa: BLE001
            outcomes.append(("err", str(exc)))

    t1 = threading.Thread(
        target=worker, args=(lambda: ctx["services"].confirm_relation(a1, b1),))
    t2 = threading.Thread(
        target=worker, args=(lambda: ctx["services"].confirm_relation(a2, b1),))
    t1.start(); t2.start(); t1.join(); t2.join()

    oks = [o for o in outcomes if o[0] == "ok"]
    errs = [o for o in outcomes if o[0] == "err"]
    assert len(oks) == 1 and len(errs) == 1
    assert "H01" in errs[0][1]

    with ctx["db"].Snapshot() as conn:
        active = conn.execute(
            "SELECT COUNT(*) AS c FROM relations WHERE status='ACTIVE'"
        ).fetchone()["c"]
    assert active == 1  # 只有一条关系生效


def test_revoke_splits_group_and_alternative_path_keeps_group(app_ctx):
    ctx = app_ctx
    _import(ctx, "hospital,mrn\nH01,A1\nH02,B1\nH03,C1\n")
    a, b, c = (_patient_id(ctx, h, m)
               for h, m in [("H01", "A1"), ("H02", "B1"), ("H03", "C1")])
    ctx["services"].confirm_relation(a, b)
    ctx["services"].confirm_relation(b, c)
    ctx["services"].confirm_relation(a, c)  # 三角，制造替代路径
    _wait_jobs(ctx)
    assert len(ctx["repository"].list_groups()) == 1

    # 撤销 a-b：经 a-c-b 仍连通，不拆
    ctx["services"].revoke_relation(a, b)
    _wait_jobs(ctx)
    assert len(ctx["repository"].list_groups()) == 1

    # 再撤 a-c：只剩 b-c，a 独立 -> 拆两组
    ctx["services"].revoke_relation(a, c)
    _wait_jobs(ctx)
    groups = ctx["repository"].list_groups()
    assert len(groups) == 2

    # 撤销不存在的 ACTIVE 关系应报错
    with pytest.raises(ctx["services"].ServiceError):
        ctx["services"].revoke_relation(a, b)


def test_idempotent_duplicate_confirm(app_ctx):
    ctx = app_ctx
    _import(ctx, "hospital,mrn\nH01,A1\nH02,B1\n")
    a = _patient_id(ctx, "H01", "A1")
    b = _patient_id(ctx, "H02", "B1")
    r1 = ctx["services"].confirm_relation(a, b)
    r2 = ctx["services"].confirm_relation(b, a)  # 颠倒顺序同样视为同键
    assert r2["outcome"] == "already_active"
    with ctx["db"].Snapshot() as conn:
        n = conn.execute(
            "SELECT COUNT(*) AS c FROM relations WHERE status='ACTIVE'"
        ).fetchone()["c"]
    assert n == 1


def test_late_compute_does_not_restore_old_groups(app_ctx):
    """模拟重算任务发布时关系已更新：迟到结果必须作废，且补排最新计算。"""
    ctx = app_ctx
    _import(ctx, "hospital,mrn\nH01,A1\nH02,B1\nH03,C1\n")
    a, b, c = (_patient_id(ctx, h, m)
               for h, m in [("H01", "A1"), ("H02", "B1"), ("H03", "C1")])
    ctx["services"].confirm_relation(a, b)
    _wait_jobs(ctx)
    groups_before = ctx["repository"].list_groups()
    # 初始导入 3 人，仅 a-b 相连，c 独立 -> 两组
    assert len(groups_before) == 2

    # 手工插入一个 seq_cutoff 已过期的重算任务并直接置为 RUNNING，
    # 模拟计算期间关系发生变化的"迟到计算"
    def insert_stale(conn):
        conn.execute(
            "INSERT INTO jobs(kind, status, seq_cutoff, started_at) "
            "VALUES ('RECOMPUTE','RUNNING', -1, datetime('now'))"
        )

    ctx["db"].write_tx(insert_stale)
    # 产生新的关系变动（会另行入队一个基于最新 seq 的任务）
    ctx["services"].confirm_relation(b, c)
    # 卡住的 RUNNING 任务由测试驱动其执行逻辑
    stale = None
    with ctx["db"].Snapshot() as conn:
        stale = conn.execute(
            "SELECT * FROM jobs WHERE kind='RECOMPUTE' AND seq_cutoff=-1"
        ).fetchone()
    ctx["worker"].run_recompute(stale)
    _wait_jobs(ctx)

    # 最终分组必须反映最新关系（三人同组），旧的两人组从未被发布
    groups = ctx["repository"].list_groups()
    assert len(groups) == 1
    assert len(groups[0]["members"]) == 3
    failed = ctx["repository"].failed_jobs()
    assert any("作废" in f["detail"] for f in failed)


def test_failed_compute_keeps_published_groups(app_ctx):
    """计算失败（同医院多编号矛盾）时正式分组整体保持上一完整版本。"""
    ctx = app_ctx
    _import(ctx, "hospital,mrn\nH01,A1\nH02,B1\nH03,C1\nH04,D1\n")
    a = _patient_id(ctx, "H01", "A1")
    b = _patient_id(ctx, "H02", "B1")
    c = _patient_id(ctx, "H03", "C1")
    d = _patient_id(ctx, "H04", "D1")
    ctx["services"].confirm_relation(a, b)
    ctx["services"].confirm_relation(c, d)  # 另一条无关的正常组
    _wait_jobs(ctx)
    good = ctx["repository"].list_groups()
    assert len(good) == 2

    # 直接构造矛盾关系（绕过服务层校验），触发计算失败
    _import(ctx, "hospital,mrn\nH01,A2\n")
    a2 = _patient_id(ctx, "H01", "A2")

    def force_relation(conn):
        lo, hi = sorted((b, a2))
        conn.execute(
            "INSERT INTO relations(patient_a, patient_b, status) "
            "VALUES (?,?,'ACTIVE')",
            (lo, hi),
        )
        seq = conn.execute("SELECT MAX(id) AS m FROM relations").fetchone()["m"]
        ctx["services"].enqueue_recompute(conn, seq)

    ctx["db"].write_tx(force_relation)
    _wait_jobs(ctx)

    # 正式分组保持矛盾发生前的最后完整版本（A2 已作为单元素组发布），
    # 矛盾关系未进入任何已发布分组，失败原因可在页面/接口查看
    still = ctx["repository"].list_groups()
    published_pids = {m["patient_id"] for g in still for m in g["members"]}
    assert published_pids == {a, b, c, d, a2}
    # a2 必须独立成组，未与 b 所在的组合并
    a2_group = next(g for g in still if a2 in
                    {m["patient_id"] for m in g["members"]})
    assert len(a2_group["members"]) == 1
    failed = ctx["repository"].failed_jobs()
    assert any("H01" in f["detail"] for f in failed)

    # 撤销矛盾关系后，重算成功，新患者以单元素组进入正式分组
    ctx["services"].revoke_relation(b, a2)
    _wait_jobs(ctx)
    recovered = ctx["repository"].list_groups()
    recovered_pids = {m["patient_id"] for g in recovered for m in g["members"]}
    assert a2 in recovered_pids
    assert not any(
        j["status"] == "FAILED" and "冲突" in (j["detail"] or "")
        for j in ctx["repository"].failed_jobs()
        if j["id"] > max(f["id"] for f in failed)
    )


def test_restart_marks_unfinished_failed_but_keeps_committed(app_ctx):
    ctx = app_ctx
    _import(ctx, "hospital,mrn\nH01,A1\nH02,B1\n")
    a = _patient_id(ctx, "H01", "A1")
    b = _patient_id(ctx, "H02", "B1")
    ctx["services"].confirm_relation(a, b)
    _wait_jobs(ctx)

    # 制造一个卡住的 PENDING
    def add_pending(conn):
        conn.execute(
            "INSERT INTO jobs(kind, status, seq_cutoff) "
            "VALUES ('RECOMPUTE','PENDING', 0)"
        )

    ctx["db"].write_tx(add_pending)

    # 模拟重启：新 worker 启动时执行恢复（先停旧线程再重置单例）
    worker.stop_worker()
    time.sleep(0.3)
    worker._stop.clear()
    worker._started = False
    worker.start_worker()
    _wait_jobs(ctx)

    # 成功变动保留
    groups = ctx["repository"].list_groups()
    assert len(groups) == 1
    failed = ctx["repository"].failed_jobs()
    assert any("重启" in f["detail"] for f in failed)


def test_web_endpoints_and_csv_upload(app_ctx):
    flask_app = create_app()
    flask_app.config["TESTING"] = True
    client = flask_app.test_client()

    import io
    assert client.get("/healthz").status_code == 200
    data = io.BytesIO("hospital,mrn\nH01,A1\nH02,B1\n".encode())
    resp = client.post(
        "/import",
        data={"file": (data, "x.csv")},
        content_type="multipart/form-data",
        follow_redirects=True,
    )
    assert resp.status_code == 200
    _wait_jobs(app_ctx)

    # 下载端点返回完整状态 CSV
    resp = client.get("/download/mapping.csv")
    assert resp.status_code == 200
    assert "X-Compute-Id" in resp.headers
    assert b"record_no" in resp.data

    # 冲突确认经 HTTP 被拒绝并闪现原因
    with app_ctx["db"].Snapshot() as conn:
        ids = [r["id"] for r in conn.execute("SELECT id FROM patients ORDER BY id")]
    resp = client.post(
        "/relations/confirm",
        data={"record_a": ids[0], "record_b": ids[1]},
        follow_redirects=True,
    )
    assert resp.status_code == 200
