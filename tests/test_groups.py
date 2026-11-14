"""分组算法纯函数测试：传递成组、组号取最小、同医院冲突检测。"""
from app.groups import build_components


def test_transitive_groups_and_min_group_no():
    patients = [
        (1, "H01", "A1"),
        (2, "H02", "B1"),
        (3, "H03", "C1"),
        (4, "H01", "A2"),  # 孤立记录
    ]
    edges = [(1, 2), (2, 3)]  # 1-2-3 传递相连
    components, violations = build_components(patients, edges)

    assert violations == []
    assert len(components) == 2
    big = components[0]
    assert big["group_no"] == 1  # 组号取最小记录号
    assert sorted(pid for pid, _h, _m in big["members"]) == [1, 2, 3]
    singleton = components[1]
    assert singleton["group_no"] == 4


def test_revoke_split_by_remaining_edges_keeps_alternative_path():
    # 撤销一条边后仍有其他路径 -> 不拆
    patients = [(1, "H01", "A1"), (2, "H02", "B1"), (3, "H03", "C1")]
    edges_after_revoke = [(1, 3), (2, 3)]  # 1-2 被撤销，但经 3 仍连通
    components, violations = build_components(patients, edges_after_revoke)
    assert len(components) == 1
    assert components[0]["group_no"] == 1

    # 无替代路径 -> 拆成两组
    edges_cut = [(2, 3)]
    components, violations = build_components(patients, edges_cut)
    assert len(components) == 2
    assert [c["group_no"] for c in components] == [1, 2]


def test_same_hospital_different_mrn_conflict_detected():
    patients = [
        (1, "H01", "A1"),
        (2, "H02", "B1"),
        (3, "H01", "A9"),  # 同医院 H01 的不同编号
    ]
    edges = [(1, 2), (2, 3)]
    components, violations = build_components(patients, edges)
    assert len(components) == 1
    assert len(violations) == 1
    assert "H01" in violations[0]
    assert "A1" in violations[0] and "A9" in violations[0]


def test_same_hospital_same_mrn_is_impossible_duplicate_key():
    # (医院,编号) 唯一，同医院同编号只会是同一条记录，不会出现在两个成员中
    patients = [(1, "H01", "A1"), (2, "H02", "B1")]
    components, violations = build_components(patients, [(1, 2)])
    assert violations == []
