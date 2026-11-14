"""独立进程回归：Gunicorn 单 worker 平滑重载下的后台交接。

这些用例各自拉起真实的操作系统进程（python -I 子进程），用文件锁/
文件门控精确控制多个进程的启动重叠、暂停恢复与退出交接，验证：

1. 启动重叠：旧进程仍存活（含被 SIGSTOP 暂时暂停）时，新进程不会
   把旧进程的运行任务误报失败，也不会重复执行同一个任务。
2. 新进程无需等待旧任务结束即可提供上传、核对、进度与完整对照下载。
3. 排队任务不会因重载丢失；旧进程退出后，存活 worker 自动接手队列，
   并把退出进程未完成的 RUNNING 任务标失败，已提交患者与成功分组保留。
4. 旧进程无法回写已终结任务或覆盖新结果。
"""
import http.client
import os
import signal
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
HARNESS = Path(__file__).resolve().parent / "process_harness.py"
PYLIBS = Path(__file__).resolve().parent / "_pylibs"
POLL = 0.05


# ---------- 辅助 ----------

def _wait_file(path, timeout=15):
    deadline = time.time() + timeout
    while not path.exists():
        if time.time() > deadline:
            raise AssertionError(f"等待文件超时：{path}")
        time.sleep(POLL)


def _wait_vanish(path, timeout=15):
    deadline = time.time() + timeout
    while path.exists():
        if time.time() > deadline:
            raise AssertionError(f"文件未按预期消失：{path}")
        time.sleep(POLL)


def _wait_db(db_path, sql, params=(), timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        conn = sqlite3.connect(db_path)
        try:
            row = conn.execute(sql, params).fetchone()
        finally:
            conn.close()
        if row is not None:
            return row
        time.sleep(POLL)
    raise AssertionError(f"数据库条件未满足：{sql}")


def _query(db_path, sql, params=()):
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute(sql, params).fetchall()
    finally:
        conn.close()


def _http(port, method, path, body=None, headers=None):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    conn.request(method, path, body=body, headers=headers or {})
    resp = conn.getresponse()
    data = resp.read()
    conn.close()
    return resp.status, dict(resp.getheaders()), data


def _spawn(data_dir: Path, sync_dir: Path, port_file: Path):
    env = dict(os.environ)
    env.update(
        APP_REPO_ROOT=str(REPO_ROOT),
        APP_PYLIBS=str(PYLIBS),
        APP_DATA_DIR=str(data_dir),
        APP_DB_PATH=str(data_dir / "patients.db"),
        APP_UPLOAD_DIR=str(data_dir / "uploads"),
        APP_LEADER_LOCK_PATH=str(data_dir / "worker.lock"),
        APP_SYNC_DIR=str(sync_dir),
        APP_PORT_FILE=str(port_file),
        PYTHONPATH=str(REPO_ROOT),
    )
    proc = subprocess.Popen(
        [sys.executable, "-I", str(HARNESS)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    _wait_file(port_file)
    port = int(port_file.read_text().strip())
    _wait_file(sync_dir / "http_ready")
    return proc, port


def _stop(proc, sig=signal.SIGTERM):
    if proc.poll() is None:
        proc.send_signal(sig)


def _drain(proc, timeout=15):
    try:
        out, _ = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        out, _ = proc.communicate(timeout=5)
    return out


def _enqueue_import(db_path, filename, csv_text, upload_dir: Path):
    """绕过 Web 直接在库里造一个 IMPORT 任务，便于精确门控执行。"""
    stored = f"manual_{filename}"
    (upload_dir / stored).write_text(csv_text, encoding="utf-8")
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute(
            "INSERT INTO jobs(kind, status, filename, total_rows, file_path) "
            "VALUES ('IMPORT','PENDING', ?, ?, ?)",
            (filename, csv_text.count("\n") - 1, str(upload_dir / stored)),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def _enqueue_recompute(db_path):
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute(
            "INSERT INTO jobs(kind, status, seq_cutoff) "
            "VALUES ('RECOMPUTE','PENDING', 0)"
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


@pytest.fixture()
def dirs(tmp_path):
    data_dir = tmp_path / "data"
    upload_dir = data_dir / "uploads"
    sync_dir = tmp_path / "sync"
    data_dir.mkdir(parents=True)
    upload_dir.mkdir()
    sync_dir.mkdir()
    return data_dir, upload_dir, sync_dir


# ---------- 用例 ----------

def test_overlapping_startup_does_not_fail_or_duplicate_live_job(dirs):
    """启动重叠：新进程不得误报旧进程存活任务失败，也不得重复执行。"""
    data_dir, upload_dir, sync_dir = dirs
    proc_a, port_a = _spawn(data_dir, sync_dir, sync_dir / "port_a")
    try:
        _wait_file(sync_dir / "leader_acquired")

        csv_text = "hospital,mrn\nH01,A1\nH02,B1\nH03,C1\n"
        job_id = _enqueue_import(data_dir / "patients.db", "a.csv",
                                 csv_text, upload_dir)
        # 在第一行处理后挂住，制造"旧任务仍存活"的窗口
        gate = sync_dir / f"hold-import-row-{job_id}-1"
        gate.write_text("")
        _wait_file(sync_dir / f"claimed-import-{job_id}")

        # 启动第二个进程（模拟平滑重载中新 worker 上线）
        proc_b, port_b = _spawn(data_dir, sync_dir, sync_dir / "port_b")
        try:
            # 给足新进程初始化与轮询锁的时间
            time.sleep(1.5)
            # 旧任务仍在运行：不允许被误标失败，也不允许被第二个进程认领
            rows = _query(
                data_dir / "patients.db",
                "SELECT status, worker_id FROM jobs WHERE id=?", (job_id,),
            )
            assert rows[0][0] == "RUNNING"
            owners = _query(
                data_dir / "patients.db",
                "SELECT DISTINCT worker_id FROM jobs WHERE id=? AND worker_id "
                "IS NOT NULL",
                (job_id,),
            )
            assert len(owners) == 1  # 全局只执行一次，没有重复认领

            # 新进程的 Web 能力立即可用，无需等待旧任务结束
            status, _, _ = _http(port_b, "GET", "/healthz")
            assert status == 200
            status, _, body = _http(port_b, "GET", "/")
            assert status == 200 and "上传患者 CSV".encode() in body
        finally:
            _stop(proc_b)
            _drain(proc_b)

        # 放行旧进程，任务正常完成；患者全部入库且只入一次
        gate.unlink()
        _wait_db(
            data_dir / "patients.db",
            "SELECT id FROM jobs WHERE id=? AND status='DONE'", (job_id,),
        )
        count = _query(
            data_dir / "patients.db",
            "SELECT COUNT(*) FROM patients",
        )[0][0]
        assert count == 3
        failed = _query(
            data_dir / "patients.db",
            "SELECT COUNT(*) FROM jobs WHERE status='FAILED'",
        )[0][0]
        assert failed == 0
    finally:
        _stop(proc_a)
        _drain(proc_a)


def test_paused_leader_keeps_lock_and_standby_serves_web(dirs):
    """暂停恢复：被 SIGSTOP 的进程仍持锁，不发生误交接。"""
    data_dir, upload_dir, sync_dir = dirs
    proc_a, port_a = _spawn(data_dir, sync_dir, sync_dir / "port_a")
    try:
        _wait_file(sync_dir / "leader_acquired")
        first_leader = (sync_dir / "leader_acquired").read_text().strip()
        assert first_leader == str(proc_a.pid)
        csv_text = "hospital,mrn\nH01,A1\nH02,B1\n"
        job_id = _enqueue_import(data_dir / "patients.db", "p.csv",
                                 csv_text, upload_dir)
        gate = sync_dir / f"hold-import-row-{job_id}-1"
        gate.write_text("")
        _wait_file(sync_dir / f"claimed-import-{job_id}")

        proc_b, port_b = _spawn(data_dir, sync_dir, sync_dir / "port_b")
        try:
            # 暂停持锁进程：锁不释放，备用进程只能等待，不能接管
            proc_a.send_signal(signal.SIGSTOP)
            time.sleep(1.5)
            rows = _query(
                data_dir / "patients.db",
                "SELECT status FROM jobs WHERE id=?", (job_id,),
            )
            assert rows[0][0] == "RUNNING"
            # 备用进程在等待锁期间仍完整提供 Web 服务
            status, _, _ = _http(port_b, "GET", "/healthz")
            assert status == 200
            status, _, body = _http(port_b, "GET", "/groups")
            assert status == 200 and "分组".encode() in body

            # 恢复旧进程，其继续完成任务，备用进程始终没有抢到锁
            proc_a.send_signal(signal.SIGCONT)
            time.sleep(0.3)
            gate.unlink()
            _wait_db(
                data_dir / "patients.db",
                "SELECT id FROM jobs WHERE id=? AND status='DONE'", (job_id,),
            )
            # 全程只有最初的进程持锁：备用进程从未当选 leader
            assert (sync_dir / "leader_acquired").read_text().strip() == \
                first_leader
        finally:
            _stop(proc_b)
            _drain(proc_b)
    finally:
        # 若用例中途失败，确保进程恢复后再终止，避免残留停止态进程
        try:
            proc_a.send_signal(signal.SIGCONT)
        except ProcessLookupError:
            pass
        _stop(proc_a)
        _drain(proc_a)


def test_exit_handoff_fails_running_keeps_committed_and_drains_queue(dirs):
    """退出交接：旧进程退出后存活 worker 接手队列；RUNNING 标失败、
    已提交患者与成功分组保留，PENDING 不丢失。"""
    data_dir, upload_dir, sync_dir = dirs
    proc_a, port_a = _spawn(data_dir, sync_dir, sync_dir / "port_a")
    try:
        _wait_file(sync_dir / "leader_acquired")

        # 先完成一批导入与一条确认，得到已发布分组
        first = "hospital,mrn\nH01,A1\nH02,B1\n"
        j1 = _enqueue_import(data_dir / "patients.db", "first.csv",
                             first, upload_dir)
        _wait_db(
            data_dir / "patients.db",
            "SELECT id FROM jobs WHERE id=? AND status='DONE'", (j1,),
        )
        patients = _query(
            data_dir / "patients.db",
            "SELECT id, hospital, hospital_mrn FROM patients ORDER BY id",
        )
        a_id = next(r[0] for r in patients if r[1] == "H01")
        b_id = next(r[0] for r in patients if r[1] == "H02")
        status, _, _ = _http(
            port_a, "POST", "/relations/confirm",
            body=f"record_a={a_id}&record_b={b_id}",
            headers={
                "Content-Type": "application/x-www-form-urlencoded"},
        )
        assert status == 302
        _wait_db(
            data_dir / "patients.db",
            "SELECT id FROM groups",
        )

        # 第二个导入：第一行后挂起，随后杀掉旧进程
        second = "hospital,mrn\nH03,C1\nH04,D1\nH05,E1\n"
        j2 = _enqueue_import(data_dir / "patients.db", "second.csv",
                             second, upload_dir)
        gate = sync_dir / f"hold-import-row-{j2}-1"
        gate.write_text("")
        _wait_file(sync_dir / f"claimed-import-{j2}")
        time.sleep(0.3)  # 确保第一行已提交

        # 再排一个 PENDING 重算，验证排队任务不丢失（该任务 seq 落后于
        # 现有关系，按既有规则会作废旧任务并补排新任务，最终都被接手）
        j3 = _enqueue_recompute(data_dir / "patients.db")

        # 备用进程先上线（等待锁），随后旧进程被强杀
        proc_b, port_b = _spawn(data_dir, sync_dir, sync_dir / "port_b")
        try:
            time.sleep(1.0)
            proc_a.kill()
            _drain(proc_a)
            gate.unlink()  # 旧门控不再有意义；清理避免干扰
            # 新进程拿到锁并完成交接
            _wait_db(
                data_dir / "patients.db",
                "SELECT id FROM jobs WHERE id=? AND status='FAILED'", (j2,),
            )
            # 排队的重算被接手处理：过期任务作废后补排的新重算成功发布，
            # 队列里不再残留 PENDING/RUNNING（排队任务没有因重载丢失）
            _wait_db(
                data_dir / "patients.db",
                "SELECT id FROM jobs WHERE kind='RECOMPUTE' "
                "AND detail LIKE '发布成功%' AND worker_id LIKE ?",
                (f"{proc_b.pid}-%",),
            )
            deadline = time.time() + 15
            while time.time() < deadline:
                left = _query(
                    data_dir / "patients.db",
                    "SELECT COUNT(*) FROM jobs WHERE status IN "
                    "('PENDING','RUNNING')",
                )[0][0]
                if left == 0:
                    break
                time.sleep(POLL)
            assert left == 0

            # 已提交患者保留（第一行 H03/C1 已落库）
            kept = _query(
                data_dir / "patients.db",
                "SELECT hospital, hospital_mrn FROM patients",
            )
            assert ("H03", "C1") in kept
            assert ("H04", "D1") not in kept  # 未执行到的行不存在
            # 成功分组保留：已确认的同组关系在交接后仍然成立（重算会
            # 基于最新完整关系重新发布，组号取组内最小记录号）
            after = _query(
                data_dir / "patients.db",
                "SELECT patient_id FROM groups WHERE group_no=? ORDER BY patient_id",
                (min(a_id, b_id),),
            )
            assert {r[0] for r in after} == {a_id, b_id}
            # 已提交的 H03 作为独立患者被纳入发布，且未被错误并入其他组
            h03 = _query(
                data_dir / "patients.db",
                "SELECT id FROM patients WHERE hospital='H03'",
            )[0][0]
            h03_group = _query(
                data_dir / "patients.db",
                "SELECT group_no FROM groups WHERE patient_id=?", (h03,),
            )
            assert h03_group and h03_group[0][0] == h03  # 单元素组，组号=自身
            # 失败任务带有明确的交接原因
            detail = _query(
                data_dir / "patients.db",
                "SELECT detail FROM jobs WHERE id=?", (j2,),
            )[0][0]
            assert "退出" in detail

            # 接手后新导入仍可正常工作并完成发布
            third = "hospital,mrn\nH06,F1\n"
            j4 = _enqueue_import(data_dir / "patients.db", "third.csv",
                                 third, upload_dir)
            _wait_db(
                data_dir / "patients.db",
                "SELECT id FROM jobs WHERE id=? AND status='DONE'", (j4,),
            )
            _wait_db(
                data_dir / "patients.db",
                "SELECT id FROM patients WHERE hospital='H06'",
            )
            # 导入触发的重算发布完成后，H06 才进入可下载的正式分组
            _wait_db(
                data_dir / "patients.db",
                "SELECT patient_id FROM groups g JOIN patients p "
                "ON p.id=g.patient_id WHERE p.hospital='H06'",
            )

            # 完整对照下载来自接手后的完整状态
            status, headers, body = _http(port_b, "GET",
                                          "/download/mapping.csv")
            assert status == 200
            assert "X-Compute-Id" in headers
            assert b"group_no,record_no,hospital,hospital_mrn,compute_id" in body
            assert b"H06" in body
        finally:
            _stop(proc_b)
            _drain(proc_b)
    finally:
        _stop(proc_a)
        _drain(proc_a)


def test_http_upload_and_verify_flow_on_standby_process(dirs):
    """备用（非领导）进程同样支持上传、核对、进度查询与下载演示。"""
    data_dir, upload_dir, sync_dir = dirs
    proc_a, _port_a = _spawn(data_dir, sync_dir, sync_dir / "port_a")
    try:
        _wait_file(sync_dir / "leader_acquired")
        proc_b, port_b = _spawn(data_dir, sync_dir, sync_dir / "port_b")
        try:
            # 通过备用进程上传：任务由持锁的旧进程执行
            boundary = "----handofftestboundary"
            body = (
                f"--{boundary}\r\n"
                'Content-Disposition: form-data; name="file"; '
                'filename="up.csv"\r\n'
                "Content-Type: text/csv\r\n\r\n"
                "hospital,mrn\r\nH01,A1\r\nH02,B1\r\n\r\n"
                f"--{boundary}--\r\n"
            ).encode()
            status, _, _ = _http(
                port_b, "POST", "/import", body=body,
                headers={"Content-Type":
                         f"multipart/form-data; boundary={boundary}"},
            )
            assert status == 302
            _wait_db(
                data_dir / "patients.db",
                "SELECT id FROM jobs WHERE kind='IMPORT' AND status='DONE'",
            )
            # 备用进程读到完整状态
            status, _, page = _http(port_b, "GET", "/")
            assert status == 200 and b"H01" not in page  # 首页只展示统计
            patients = _query(
                data_dir / "patients.db",
                "SELECT id FROM patients ORDER BY id",
            )
            a_id, b_id = patients[0][0], patients[1][0]
            status, _, _ = _http(
                port_b, "POST", "/relations/confirm",
                body=f"record_a={a_id}&record_b={b_id}",
                headers={
                    "Content-Type": "application/x-www-form-urlencoded"},
            )
            assert status == 302
            _wait_db(data_dir / "patients.db", "SELECT id FROM groups")
            status, headers, csv_data = _http(
                port_b, "GET", "/download/mapping.csv")
            assert status == 200
            assert headers.get("X-Compute-Id")
            assert b"MRN" not in csv_data  # 表头固定
            assert b"H01" in csv_data and b"H02" in csv_data
            # 进度/核对页同样可访问
            assert _http(port_b, "GET", "/relations")[0] == 200
            assert _http(port_b, "GET", "/groups")[0] == 200
        finally:
            _stop(proc_b)
            _drain(proc_b)
    finally:
        _stop(proc_a)
        _drain(proc_a)
