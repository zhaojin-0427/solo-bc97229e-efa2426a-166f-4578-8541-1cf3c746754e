"""后台工作线程：导入任务与分组重算（支持平滑重载下的跨进程交接）。

关键保证：
- 全局只有一个后台执行者：进程启动后台线程时先竞争数据目录内的
  ``worker.lock`` 排他咨询锁（见 :mod:`app.leader`）。未获锁的进程
  不接触任务队列，但其 Web 能力（上传、核对、进度、下载）立即可用，
  无需等待旧进程结束。
- 持锁进程（leader）崩溃/退出后锁由内核自动释放；存活进程（含被
  SIGSTOP 暂时暂停的进程）始终持锁，不会被误判为死亡。新 leader
  当选后只把前任遗留在 RUNNING 状态的任务标失败，PENDING 排队任务
  原样保留并继续执行，因此重载不会丢失排队任务。
- 任务认领时写入本进程的 worker_id；之后所有状态回写（进度、失败、
  发布、完成）都带 ``worker_id + status='RUNNING'`` 条件，旧进程
  无法回写已被终结的任务，也无法覆盖新进程发布的分组。
- 分组计算（读快照 + 内存并查集）在任何写事务之外完成，期间导入、
  确认、撤销、查询全部正常进行。
- 发布阶段在单个 IMMEDIATE 事务内原子替换 groups 并更新任务状态，
  关系、组成员、组号一起生效；查询永远只能看到整组前或整组后。
- 发布前再次比对关系序列号：迟到计算直接作废，不会恢复旧分组。
- 计算失败（同医院多编号冲突等）绝不修改正式分组，任务标 FAILED 并
  记录拒绝原因。
"""
import os
import threading
import time
import uuid

from . import config, db, groups as group_mod, leader as leader_mod, services

_stop = threading.Event()
_started = False
_lock = threading.Lock()
_worker_id = None
_leader_lock = None
_thread = None


class WorkerLost(RuntimeError):
    """任务执行期间本进程失去该任务所有权（领导权已交接）。"""


# ---- 测试同步钩子（仅当 APP_SYNC_DIR 配置时生效，生产环境为空操作）----

def _sync_event(name):
    sync_dir = config.SYNC_DIR
    if not sync_dir:
        return
    try:
        with open(os.path.join(sync_dir, name), "w") as fh:
            fh.write(str(os.getpid()))
    except OSError:
        pass


def _sync_gate(name):
    """门文件存在则阻塞至其被删除；用于独立进程测试控制执行重叠。"""
    sync_dir = config.SYNC_DIR
    if not sync_dir:
        return
    gate = os.path.join(sync_dir, name)
    while os.path.exists(gate):
        if _stop.wait(0.05):
            return
        time.sleep(0)


# ---- 交接恢复与任务认领 ----

def mark_dead_leader_jobs_failed(conn):
    """新 leader 当选后的恢复：仅终结 RUNNING 任务。

    能走到这里说明领导锁已被本进程取得，即前任进程已经退出或崩溃
    （暂停的进程仍持锁，不会触发本路径）。其 RUNNING 任务不可能再
    推进，标记失败；已逐行提交的患者与已发布分组随 WAL 完整保留。
    PENDING 任务从未开始，必须保留排队，由本进程接着执行。
    """
    conn.execute(
        "UPDATE jobs SET status='FAILED', "
        "finished_at=datetime('now'), "
        "detail=CASE WHEN detail='' OR detail IS NULL THEN "
        "'后台进程退出时该任务仍在执行，已标记失败；请重新发起' "
        "ELSE detail || '；后台进程退出，未完成的运行任务标记失败' END "
        "WHERE status='RUNNING'"
    )


def claim_job(conn, kind, worker_id):
    """认领一个 PENDING 任务并登记持有者；无任务返回 None。"""
    row = conn.execute(
        "SELECT * FROM jobs WHERE kind=? AND status='PENDING' "
        "ORDER BY id LIMIT 1",
        (kind,),
    ).fetchone()
    if row is None:
        return None
    cur = conn.execute(
        "UPDATE jobs SET status='RUNNING', started_at=datetime('now'), "
        "worker_id=? WHERE id=? AND status='PENDING'",
        (worker_id, row["id"]),
    )
    if cur.rowcount == 0:
        # 理论上单领导锁下不会发生；防御性地视为未认领
        return None
    return row


def _after_claim(job, kind):
    _sync_event(f"claimed-{kind}-{job['id']}")
    _sync_gate(f"hold-{kind}-{job['id']}")


# ---- 带所有权围栏的任务回写 ----

def _fail(job_id, worker_id, detail):
    """仅任务持有者可标失败；返回是否完成回写。"""
    def tx(conn):
        cur = conn.execute(
            "UPDATE jobs SET status='FAILED', finished_at=datetime('now'), "
            "detail=? WHERE id=? AND worker_id=? AND status='RUNNING'",
            (detail, job_id, worker_id),
        )
        return cur.rowcount > 0

    return db.write_tx(tx)


def _progress(job_id, worker_id, done, inserted, reused, rejected, errors):
    def tx(conn):
        cur = conn.execute(
            "UPDATE jobs SET done_rows=?, inserted=?, reused=?, rejected=?, "
            "detail=? WHERE id=? AND worker_id=? AND status='RUNNING'",
            (done, inserted, reused, rejected, "; ".join(errors),
             job_id, worker_id),
        )
        if cur.rowcount == 0:
            # 任务已被接管进程终结：立即停止，禁止继续回写
            raise WorkerLost(job_id)

    db.write_tx(tx)


# ---- 具体任务执行 ----

def run_import(job, worker_id):
    job_id = job["id"]
    path = job["file_path"]
    try:
        with open(path, "rb") as fh:
            text = fh.read().decode("utf-8-sig")
    except OSError as exc:
        _fail(job_id, worker_id, f"上传文件不可读：{exc}")
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
            try:
                _progress(job_id, worker_id, done, inserted, reused,
                          rejected, row_errors)
            except WorkerLost:
                return
        # 测试钩子：逐行门控（生产环境 SYNC_DIR 为空时零成本跳过）
        _sync_gate(f"hold-import-row-{job_id}-{done}")

    # 导入完成后安排一次重算（包含新患者的单元素组）
    def schedule(conn):
        owner = conn.execute(
            "SELECT worker_id, status FROM jobs WHERE id=?", (job_id,)
        ).fetchone()
        if owner is None or owner["worker_id"] != worker_id \
                or owner["status"] != "RUNNING":
            # 已被接管进程终结：不得补排重算，也不得改写任务状态
            return
        seq = conn.execute(
            "SELECT COALESCE(MAX(id), 0) AS m FROM relations"
        ).fetchone()["m"]
        services.enqueue_recompute(conn, seq)
        conn.execute(
            "UPDATE jobs SET status='DONE', finished_at=datetime('now'), "
            "done_rows=?, inserted=?, reused=?, rejected=?, detail=? "
            "WHERE id=? AND worker_id=? AND status='RUNNING'",
            (done, inserted, reused, rejected, "; ".join(row_errors),
             job_id, worker_id),
        )

    db.write_tx(schedule)


def run_recompute(job, worker_id):
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
            owner = conn.execute(
                "SELECT worker_id, status FROM jobs WHERE id=?", (job_id,)
            ).fetchone()
            if owner is None or owner["worker_id"] != worker_id \
                    or owner["status"] != "RUNNING":
                return
            conn.execute(
                "UPDATE jobs SET status='FAILED', "
                "finished_at=datetime('now'), "
                "detail='计算期间关系发生变化（seq %d > %d），本次迟到结果作废；"
                "已安排基于最新关系的重算' WHERE id=? AND worker_id=? "
                "AND status='RUNNING'"
                % (current_seq, cutoff or 0),
                (job_id, worker_id),
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
            job_id, worker_id,
            "分组计算失败，正式分组未改动。冲突明细：" + "；".join(violations),
        )
        return

    # ④ 原子发布：一个写事务内替换全部 groups，关系/成员/组号一起生效
    def publish(conn):
        owner = conn.execute(
            "SELECT worker_id, status FROM jobs WHERE id=?", (job_id,)
        ).fetchone()
        if owner is None or owner["worker_id"] != worker_id \
                or owner["status"] != "RUNNING":
            # 旧进程不得回写已终结任务或覆盖新结果
            return

        # 发布前最后一次防迟到校验
        latest = conn.execute(
            "SELECT COALESCE(MAX(id), 0) AS m FROM relations"
        ).fetchone()["m"]
        if latest > (cutoff or 0):
            services.enqueue_recompute(conn, latest)
            conn.execute(
                "UPDATE jobs SET status='FAILED', finished_at=datetime('now'), "
                "detail='发布前检测到更新的关系变动，本次迟到结果作废，"
                "已安排最新重算' WHERE id=? AND worker_id=? AND status='RUNNING'",
                (job_id, worker_id),
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
            "detail=? WHERE id=? AND worker_id=? AND status='RUNNING'",
            (f"发布成功（compute_id={next_compute}，共 {len(components)} 组）",
             job_id, worker_id),
        )

    db.write_tx(publish)


# ---- 后台主循环与进程内单例管理 ----

def _loop():
    global _leader_lock
    lock = leader_mod.LeaderLock(stop_event=_stop)
    if not lock.acquire():
        return  # 等待锁期间收到退出信号
    _leader_lock = lock
    try:
        # 当选唯一后台进程：前任已退出，其 RUNNING 任务标失败、
        # PENDING 任务保留（成功变动已随 WAL 持久保留）
        db.write_tx(mark_dead_leader_jobs_failed)
        _sync_event("leader_acquired")

        while not _stop.is_set():
            processed = False
            try:
                job = db.write_tx(
                    lambda conn: claim_job(conn, "IMPORT", _worker_id))
                if job is not None:
                    processed = True
                    _after_claim(job, "import")
                    if _stop.is_set():
                        # 退出信号到达：保持 RUNNING，交由下一任 leader 判定
                        return
                    run_import(job, _worker_id)
                    continue
                job = db.write_tx(
                    lambda conn: claim_job(conn, "RECOMPUTE", _worker_id))
                if job is not None:
                    processed = True
                    _after_claim(job, "recompute")
                    if _stop.is_set():
                        return
                    run_recompute(job, _worker_id)
                    continue
            except WorkerLost:
                # 领导权已交接，本进程不再触碰任何任务
                return
            except Exception as exc:  # worker 不因单任务异常退出
                try:
                    print(f"[worker] 循环异常: {exc!r}", flush=True)
                except Exception:
                    pass
            if not processed:
                _stop.wait(config.WORKER_POLL_SECONDS)
    finally:
        lock.release()
        _leader_lock = None


def start_worker():
    """启动本进程的后台线程（进程内幂等）。"""
    global _started, _worker_id, _leader_lock, _thread
    with _lock:
        if _started:
            return
        _started = True
        _stop.clear()
        _worker_id = f"{os.getpid()}-{uuid.uuid4().hex[:8]}"
        _leader_lock = None
    db.init_db()
    _thread = threading.Thread(
        target=_loop,
        name=f"background-worker-{_worker_id}",
        daemon=True,
    )
    _thread.start()


def stop_worker():
    _stop.set()


def join_worker(timeout=10):
    """等待后台线程退出（测试与优雅停机用），返回线程是否已结束。"""
    t = _thread
    if t is not None and t.is_alive() and t is not threading.current_thread():
        t.join(timeout)
        return not t.is_alive()
    return True


def reset_for_tests():
    """测试专用：停止并回收线程，清空单例状态，释放可能残留的领导锁。"""
    global _started, _worker_id, _leader_lock, _thread
    _stop.set()
    # 先让线程自行退出并释放它持有的锁，避免与手动释放相互竞争
    join_worker()
    with _lock:
        if _leader_lock is not None:
            _leader_lock.release()
            _leader_lock = None
        _started = False
        _worker_id = None
        _thread = None
    _stop.clear()
