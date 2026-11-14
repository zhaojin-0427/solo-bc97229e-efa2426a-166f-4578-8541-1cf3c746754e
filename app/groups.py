"""分组计算（纯函数 + 只读快照读取，计算不持有数据库写事务）。

关系语义：ACTIVE 关系是无向边，传递闭包形成连通分量（同人组）。
组号 = 分量内最小患者记录号。
硬约束：同一组内，同一医院最多出现一个院内编号，否则整次计算失败、
正式分组保持不变，冲突原因写入任务并可在页面查看。
"""


class UnionFind:
    def __init__(self):
        self.parent = {}

    def add(self, x):
        if x not in self.parent:
            self.parent[x] = x

    def find(self, x):
        root = x
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[x] != x:  # 路径压缩
            nxt = self.parent[x]
            self.parent[x] = root
            x = nxt
        return root

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            # 以较小 id 为根，顺带让组号计算稳定
            if ra < rb:
                self.parent[rb] = ra
            else:
                self.parent[ra] = rb


def build_components(patients, edges):
    """根据患者集合与 ACTIVE 关系边构造连通分量。

    patients: [(id, hospital, hospital_mrn), ...]
    edges:    [(patient_a, patient_b), ...]
    返回 (components, violations)
      components: list[dict(group_no, members[(id, hospital, mrn)])]
                  按 group_no 升序
      violations: list[str] 同医院多编号冲突的人类可读原因
    """
    uf = UnionFind()
    for pid, _h, _m in patients:
        uf.add(pid)
    for a, b in edges:
        uf.add(a)
        uf.add(b)
        uf.union(a, b)

    groups = {}  # root -> list[patient rows]
    for row in patients:
        root = uf.find(row[0])
        groups.setdefault(root, []).append(row)

    components, violations = [], []
    for root, members in groups.items():
        members.sort(key=lambda r: r[0])
        group_no = members[0][0]
        # 同医院编号冲突检测
        seen = {}  # hospital -> set(mrn)
        hospital_mrns = {}
        for _pid, hospital, mrn in members:
            hospital_mrns.setdefault(hospital, set()).add(mrn)
        for hospital, mrns in hospital_mrns.items():
            if len(mrns) > 1:
                ids = sorted(
                    pid for pid, h, _m in members if h == hospital
                )
                violations.append(
                    f"组 {group_no} 中医院 {hospital} 存在不同院内编号 "
                    f"{sorted(mrns)}（涉及记录号 {ids}），同人关系矛盾"
                )
        components.append(
            {
                "group_no": group_no,
                "members": [(pid, h, m) for pid, h, m in members],
            }
        )

    components.sort(key=lambda c: c["group_no"])
    return components, violations


def read_snapshot(conn):
    """在给定连接（快照事务内）读取当前全部患者与 ACTIVE 关系。"""
    patients = [
        (r["id"], r["hospital"], r["hospital_mrn"])
        for r in conn.execute(
            "SELECT id, hospital, hospital_mrn FROM patients ORDER BY id"
        )
    ]
    edges = [
        (r["patient_a"], r["patient_b"])
        for r in conn.execute(
            "SELECT patient_a, patient_b FROM relations "
            "WHERE status='ACTIVE' ORDER BY id"
        )
    ]
    max_rel = conn.execute(
        "SELECT COALESCE(MAX(id), 0) AS m FROM relations"
    ).fetchone()["m"]
    return patients, edges, max_rel


def compute(conn):
    """完整算一遍：返回 (components, violations, max_rel_id)。"""
    patients, edges, max_rel = read_snapshot(conn)
    components, violations = build_components(patients, edges)
    return components, violations, max_rel
