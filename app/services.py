"""业务服务层：导入入队、关系确认/撤销、重算调度。

所有写操作通过 db.write_tx 串行化，确认/撤销在同一 IMMEDIATE 事务内
基于"提交时的完整关系"做判定，保证并发确认的原子语义。
"""
import csv
import io
import os
import uuid

from . import config, db, groups as group_mod


class ServiceError(Exception):
    """可直接展示给用户的业务错误。"""


def _canonical_pair(a, b):
    lo, hi = (a, b) if a < b else (b, a)
    return lo, hi


def get_patient(conn, pid):
    row = conn.execute(
        "SELECT id, hospital, hospital_mrn FROM patients WHERE id=?",
        (pid,),
    ).fetchone()
    if row is None:
        raise ServiceError(f"记录号 {pid} 不存在")
    return row


def _active_edges(conn):
    return [
        (r["patient_a"], r["patient_b"])
        for r in conn.execute(
            "SELECT patient_a, patient_b FROM relations "
            "WHERE status='ACTIVE'"
        )
    ]


def _would_conflict(components_by_pid, a, b):
    """模拟把 a、b 合并后是否违反"同组同医院唯一编号"约束。"""
    ga = components_by_pid.get(a)
    gb = components_by_pid.get(b)
    if ga is None or gb is None or ga is gb:
        return None  # 不产生新合并，无新增冲突
    hospital_mrns = {}
    for comp in (ga, gb):
        for pid, hospital, mrn in comp["members"]:
            hospital_mrns.setdefault(hospital, set()).add(mrn)
    for hospital, mrns in hospital_mrns.items():
        if len(mrns) > 1:
            return (
                f"合并后医院 {hospital} 在同组内出现不同院内编号 "
                f"{sorted(mrns)}，违反同组唯一编号约束"
            )
    return None


def confirm_relation(a_id, b_id):
    """确认两条记录为同人。成功返回 dict，冲突/错误抛 ServiceError。"""

    def tx(conn):
        pa = get_patient(conn, a_id)
        pb = get_patient(conn, b_id)
        if pa["id"] == pb["id"]:
            raise ServiceError("不能将同一条记录与自身建立同人关系")

        lo, hi = _canonical_pair(pa["id"], pb["id"])

        existing = conn.execute(
            "SELECT id, status FROM relations "
            "WHERE patient_a=? AND patient_b=? ORDER BY id DESC LIMIT 1",
            (lo, hi),
        ).fetchone()
        if existing and existing["status"] == "ACTIVE":
            return {
                "outcome": "already_active",
                "relation_id": existing["id"],
                "message": "该同人关系已确认，重复提交已幂等复用",
            }

        # 提交时完整关系上构建分量（计算在写事务内，但仅为内存运算，
        # 不涉及额外的数据库写；外部导入/查询使用独立连接不受阻塞）
        patients, edges, _ = group_mod.read_snapshot(conn)
        components, violations = group_mod.build_components(patients, edges)
        by_pid = {}
        for comp in components:
            for pid, _h, _m in comp["members"]:
                by_pid[pid] = comp

        # 已有违规时拒绝一切新确认，要求先撤销矛盾关系
        if violations:
            raise ServiceError(
                "当前关系已存在未解决的同医院多编号冲突："
                + "；".join(violations)
            )

        reason = _would_conflict(by_pid, lo, hi)
        if reason:
            raise ServiceError(
                f"拒绝确认（记录 {lo} ↔ {hi}）：{reason}"
            )

        cur = conn.execute(
            "INSERT INTO relations(patient_a, patient_b, status) "
            "VALUES (?, ?, 'ACTIVE')",
            (lo, hi),
        )
        rel_id = cur.lastrowid
        seq = conn.execute("SELECT MAX(id) AS m FROM relations").fetchone()["m"]
        enqueue_recompute(conn, seq)
        return {
            "outcome": "confirmed",
            "relation_id": rel_id,
            "message": f"已确认记录 {lo} 与 {hi} 的同人关系",
        }

    return db.write_tx(tx)


def revoke_relation(a_id, b_id):
    """撤销两条记录间的同人关系；按剩余关系重新拆组。"""

    def tx(conn):
        pa = get_patient(conn, a_id)
        pb = get_patient(conn, b_id)
        if pa["id"] == pb["id"]:
            raise ServiceError("不能撤销记录与自身的关系")
        lo, hi = _canonical_pair(pa["id"], pb["id"])

        row = conn.execute(
            "SELECT id FROM relations "
            "WHERE patient_a=? AND patient_b=? AND status='ACTIVE'",
            (lo, hi),
        ).fetchone()
        if row is None:
            raise ServiceError(
                f"记录 {lo} 与 {hi} 之间不存在有效的（ACTIVE）同人关系，"
                "无可撤销内容"
            )
        conn.execute(
            "UPDATE relations SET status='REVOKED', "
            "revoked_at=datetime('now') WHERE id=?",
            (row["id"],),
        )
        seq = conn.execute("SELECT MAX(id) AS m FROM relations").fetchone()["m"]
        enqueue_recompute(conn, seq)
        return {
            "outcome": "revoked",
            "relation_id": row["id"],
            "message": f"已撤销记录 {lo} 与 {hi} 的同人关系，将按剩余关系重新分组",
        }

    return db.write_tx(tx)


def enqueue_recompute(conn, seq_cutoff):
    """安排一次后台重算。

    规则：若已有 PENDING 任务，复用最新的 seq_cutoff（合并抖动）；
    否则新建。RUNNING 的任务不改动，其发布时会因 seq 过期自行作废并
    由后续 enqueue 补上新任务，迟到计算不会恢复旧分组。
    """
    pending = conn.execute(
        "SELECT id, seq_cutoff FROM jobs "
        "WHERE kind='RECOMPUTE' AND status='PENDING' "
        "ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if pending:
        new_seq = max(seq_cutoff or 0, pending["seq_cutoff"] or 0)
        conn.execute(
            "UPDATE jobs SET seq_cutoff=? WHERE id=?",
            (new_seq, pending["id"]),
        )
        return pending["id"]
    cur = conn.execute(
        "INSERT INTO jobs(kind, status, seq_cutoff) "
        "VALUES ('RECOMPUTE', 'PENDING', ?)",
        (seq_cutoff,),
    )
    return cur.lastrowid


def create_import_job(filename, raw_bytes):
    """保存上传文件并创建 IMPORT 任务，返回 job_id。"""
    safe = os.path.basename(filename).replace("/", "_")
    stored = f"{uuid.uuid4().hex}_{safe}"
    path = os.path.join(config.UPLOAD_DIR, stored)
    with open(path, "wb") as fh:
        fh.write(raw_bytes)

    # 先解析一遍，得到总行数与可读错误（正式入库仍在 worker 中逐行做）
    text = raw_bytes.decode("utf-8-sig")
    rows, parse_errors = parse_csv(text)
    total = len(rows)

    def tx(conn):
        detail = "; ".join(parse_errors)
        cur = conn.execute(
            "INSERT INTO jobs(kind, status, filename, total_rows, detail, "
            "file_path) VALUES ('IMPORT', 'PENDING', ?, ?, ?, ?)",
            (safe, total, detail, path),
        )
        return cur.lastrowid

    job_id = db.write_tx(tx)
    return job_id, total, parse_errors


def parse_csv(text):
    """解析 CSV 文本，返回 (rows[(hospital, mrn)], errors[str])。

    要求表头包含 hospital(医院) 与 mrn(院内编号) 两列（中英文均可）。
    """
    reader = csv.reader(io.StringIO(text))
    try:
        header = next(reader)
    except StopIteration:
        return [], ["CSV 为空，缺少表头"]

    normalized = [h.strip().strip("﻿").lower() for h in header]
    alias = {
        "hospital": "hospital", "医院": "hospital", "hospital_code": "hospital",
        "mrn": "mrn", "hospital_mrn": "mrn", "院内编号": "mrn",
        "医院内编号": "mrn", "患者编号": "mrn", "编号": "mrn",
    }
    cols = {}
    for idx, name in enumerate(normalized):
        key = alias.get(name)
        if key and key not in cols:
            cols[key] = idx
    if "hospital" not in cols or "mrn" not in cols:
        return [], [
            "表头必须包含医院列（hospital/医院）与院内编号列（mrn/院内编号），"
            f"实际表头：{header}"
        ]

    rows, errors = [], []
    seen_in_file = set()
    for line_no, record in enumerate(reader, start=2):
        if not any(cell.strip() for cell in record):
            continue  # 跳过空行
        try:
            hospital = record[cols["hospital"]].strip()
            mrn = record[cols["mrn"]].strip()
        except IndexError:
            errors.append(f"第 {line_no} 行列数不足，已跳过")
            continue
        if not hospital or not mrn:
            errors.append(f"第 {line_no} 行医院或院内编号为空，已跳过")
            continue
        key = (hospital, mrn)
        if key in seen_in_file:
            errors.append(
                f"第 {line_no} 行 ({hospital}, {mrn}) 在文件内重复，已跳过"
            )
            continue
        seen_in_file.add(key)
        rows.append((hospital, mrn))
    return rows, errors
