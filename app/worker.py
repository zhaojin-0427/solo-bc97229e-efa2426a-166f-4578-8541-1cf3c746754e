"""后台工作线程：导入任务与分组重算。

关键保证：
- 分组计算（读快照 + 内存并查集）在任何写事务之外完成，期间导入、
  确认、撤销、查询全部正常进行。
- 发布阶段在单个 IMMEDIATE 事务内原子替换 groups 并更新任务状态，
  关系、组成员、组号一起生效；查询永远只能看到整组前或整组后。
- 发布前再次比对关系序列号：迟到计算直接作废，不会恢复旧分组。
- 计算失败（同医院多编号冲突等）绝不修改正式分组，任务标 FAILED 并
  记录拒绝原因。
- 启动时把所有 PENDING/RUNNING 任务标 FAILED（重启后未完成操作），
  已提交的成功变动因 WAL 持久化而完整保留。
"""
import threading
import time

from . import config, db, groups as group_mod, services

_stop = threading.Event()
_started = False
_lock = threading.Lock()


def mark_unfinished_failed(conn):
    conn.execute(
        "UPDATE jobs SET status='FAILED', "
        "finished_at=datetime('now'), "
        "detail=CASE WHEN detail='' OR detail IS NULL THEN "
        "'服务重启时该任务尚未完成，已标记失败；请重新发起' "
        "ELSE detail || '；服务重启，未完成操作标记失败' END "
        "WHERE status IN ('PENDING','RUNNING')"
    )


def claim_job(conn, kind):
    row = conn.execute(
        "SELECT * FROM jobs WHERE kind=? AND status='PENDING' "
        "ORDER BY id LIMIT 1",
        (kind,),
    ).fetchone()
    if row is None:
        return None
    conn.execute(
        "UPDATE jobs SET status='RUNNING', started_at=datetime('now') "
        "WHERE id=?",
        (row["id"],),
    )
    return row


def run_import(job):
    job_id = job["id"]
    path = job["file_path"]
    try:
        with open(path, "rb") as fh:
            text = fh.read().decode("utf-8-sig")
    except OSError as exc:
        _fail(job_id, f"上传文件不可读：{exc}")
        return

    rows, parse_errors = services.parse_csv(text)
    inserted = reused = rejected = 0
    done = 0
    row_errors = list(parse_errors)

    for hospital, mrn in rows:
        # 每行一个短写事务：同键重传复用，且不阻塞其他前台操作
        def tx(conn, hospital=hospital, mrn=mrn):
            existing = conn.execute(
                "SELECT id FROM patients WHERE hospital=? AND hospital_mrn=?",
                (hospital, mrn),
            ).fetchone()
            if existing:
                return "reused", existing["id"]
            cur = conn.execute(
                "INSERT INTO patients(hospital, hospital_mrn) VALUES (?, ?)",
                (hospital, mrn),
            )
            return "inserted", cur.lastrowid

        try:
            outcome, _pid = db.write_tx(tx)
            if outcome == "inserted":
                inserted += 1
            else:
                reused += 1
        except Exception as exc:  # 单行失败不拖垮整批
            rejected += 1
            row_errors.append(f"({hospital}, {mrn}) 入库失败：{exc}")
        done += 1
        if done % 100 == 0 or done == len(rows):
            _progress(job_id, done, inserted, reused, rejected, row_errors)

    # 导入完成后安排一次重算（包含新患者的单元素组）
    def schedule(conn):
        seq = conn.execute(
            "SELECT COALESCE(MAX(id), 0) AS m FROM relations"
        ).fetchone()["m"]
        services.enqueue_recompute(conn, seq)
        conn.execute(
            "UPDATE jobs SET status='DONE', finished_at=datetime('now'), "
            "done_rows=?, inserted=?, reused=?, rejected=?, detail=? "
            "WHERE id=?",
            (done, inserted, reused, rejected, "; ".join(row_errors), job_id),
        )

    db.write_tx(schedule)


def _progress(job_id, done, inserted, reused, rejected, errors):
    def tx(conn):
        conn.execute(
            "UPDATE jobs SET done_rows=?, inserted=?, reused=?, rejected=?, "
            "detail=? WHERE id=?",
            (done, inserted, reused, rejected, "; ".join(errors), job_id),
        )

    try:
        db.write_tx(tx)
    except Exception:
        pass  # 进度更新失败不影响导入本身


def run_recompute(job):
    job_id = job["id"]
    cutoff = job["seq_cutoff"]

    # ① 只读快照内取数，读完立即提交并关闭连接；并查集计算完全在
    #    数据库事务之外，不占任何读/写锁，前台导入与核对不受阻塞
    with db.Snapshot() as conn:
        components, violations, current_seq = group_mod.compute(conn)
    # —— 此处起无任何打开的数据库事务 ——

    # ② 计算期间若有更新的关系变动，本任务已过期：作废后补排新任务
    if current_seq > (cutoff or 0):
        def stale_tx(conn):
            conn.execute(
                "UPDATE jobs SET status='FAILED', "
                "finished_at=datetime('now'), "
                "detail='计算期间关系发生变化（seq %d > %d），本次迟到结果作废；"
                "已安排基于最新关系的重算' WHERE id=?"
                % (current_seq, cutoff or 0),
                (job_id,),
            )
            latest = conn.execute(
                "SELECT COALESCE(MAX(id), 0) AS m FROM relations"
            ).fetchone()["m"]
            services.enqueue_recompute(conn, latest)

        db.write_tx(stale_tx)
        return

    if violations:
        # ③ 冲突：正式分组保持不变，失败原因可在页面查看
        _fail(
            job_id,
            "分组计算失败，正式分组未改动。冲突明细：" + "；".join(violations),
        )
        return

    # ④ 原子发布：一个写事务内替换全部 groups，关系/成员/组号一起生效
    def publish(conn):
        # 发布前最后一次防迟到校验
        latest = conn.execute(
            "SELECT COALESCE(MAX(id), 0) AS m FROM relations"
        ).fetchone()["m"]
        if latest > (cutoff or 0):
            services.enqueue_recompute(conn, latest)
            conn.execute(
                "UPDATE jobs SET status='FAILED', finished_at=datetime('now'), "
                "detail='发布前检测到更新的关系变动，本次迟到结果作废，"
                "已安排最新重算' WHERE id=?",
                (job_id,),
            )
            return

        next_compute = conn.execute(
            "SELECT COALESCE(MAX(compute_id), 0) + 1 AS n FROM groups"
        ).fetchone()["n"]
        conn.execute("DELETE FROM groups")
        for comp in components:
            for pid, _h, _m in comp["members"]:
                conn.execute(
                    "INSERT INTO groups(group_no, patient_id, compute_id) "
                    "VALUES (?, ?, ?)",
                    (comp["group_no"], pid, next_compute),
                )
        conn.execute(
            "UPDATE jobs SET status='DONE', finished_at=datetime('now'), "
            "detail=? WHERE id=?",
            (f"发布成功（compute_id={next_compute}，共 {len(components)} 组）",
             job_id),
        )

    db.write_tx(publish)


def _fail(job_id, detail):
    def tx(conn):
        conn.execute(
            "UPDATE jobs SET status='FAILED', finished_at=datetime('now'), "
            "detail=? WHERE id=?",
            (detail, job_id),
        )

    db.write_tx(tx)


def _loop():
    # 启动恢复：未完成任务标失败（成功变动已随 WAL 持久保留）
    db.write_tx(mark_unfinished_failed)
    while not _stop.is_set():
        processed = False
        try:
            job = db.write_tx(lambda conn: claim_job(conn, "IMPORT"))
            if job is not None:
                processed = True
                run_import(job)
                continue
            job = db.write_tx(lambda conn: claim_job(conn, "RECOMPUTE"))
            if job is not None:
                processed = True
                run_recompute(job)
                continue
        except Exception as exc:  # worker 不因单任务异常退出
            try:
                print(f"[worker] 循环异常: {exc!r}", flush=True)
            except Exception:
                pass
        if not processed:
            _stop.wait(config.WORKER_POLL_SECONDS)


def start_worker():
    global _started
    with _lock:
        if _started:
            return
        _started = True
    db.init_db()
    t = threading.Thread(target=_loop, name="background-worker", daemon=True)
    t.start()


def stop_worker():
    _stop.set()
