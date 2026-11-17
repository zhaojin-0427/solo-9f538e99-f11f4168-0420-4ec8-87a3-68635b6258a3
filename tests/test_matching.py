"""配对算法单元测试：先最大化容差内配对数，再最小化绝对差总和。"""
from __future__ import annotations

from app.matching import MeasuredPoint, ReferencePoint, solve_pairings


def mp(value: float, shard: int = 1, row: int = 0) -> MeasuredPoint:
    return MeasuredPoint(value=value, shard_id=shard, row_index=row)


def rp(value: float, tolerance: float, source: str | None = None) -> ReferencePoint:
    return ReferencePoint(value=value, tolerance=tolerance, source=source)


def feasible_pairs(pairings) -> list[tuple[float, float]]:
    return [(p.measured.value, p.reference.value) for p in pairings if p.feasible]


def test_single_side_unpaired_reported():
    # 多一个实测、少一个对照
    pairings = solve_pairings([mp(1.0), mp(2.0)], [rp(1.0, 0.5)])
    assert len(feasible_pairs(pairings)) == 1
    leftover = [p for p in pairings if p.measured is not None and p.reference is None]
    assert len(leftover) == 1 and leftover[0].measured.value == 2.0

    # 多一个对照
    pairings = solve_pairings([mp(1.0)], [rp(1.0, 0.5), rp(9.0, 0.1)])
    missing = [p for p in pairings if p.measured is None and p.reference is not None]
    assert len(missing) == 1 and missing[0].reference.value == 9.0


def test_maximize_feasible_count_first_even_if_diff_larger():
    # 贪心就近会把 10->10（差0），导致 11 无对照可配（9±0.6 都超），只得 1 对；
    # 最优应是 10->9.5（差0.5 在容差内）+ 11->10.5（差0.5），共 2 对。
    measured = [mp(10.0), mp(11.0)]
    references = [rp(9.5, 0.6), rp(10.5, 0.6)]
    pairings = solve_pairings(measured, references)
    assert len(feasible_pairs(pairings)) == 2


def test_minimize_total_diff_when_count_tied():
    # 两种配法都能配成 2 对：
    #   顺配：|1-1.1| + |2-2.1| = 0.2（最优）
    #   交叉：|1-2.1| + |2-1.1| = 2.0
    measured = [mp(1.0), mp(2.0)]
    references = [rp(1.1, 5.0), rp(2.1, 5.0)]
    pairings = solve_pairings(measured, references)
    assert feasible_pairs(pairings) == [(1.0, 1.1), (2.0, 2.1)]
    assert round(sum(p.abs_diff for p in pairings), 9) == 0.2


def test_tolerance_boundary_inclusive():
    # 差恰好等于容差也算可行
    pairings = solve_pairings([mp(1.0)], [rp(1.5, 0.5)])
    assert len(feasible_pairs(pairings)) == 1
    pairings = solve_pairings([mp(1.0)], [rp(1.5, 0.4999)])
    assert feasible_pairs(pairings) == []


def test_per_tolerance_independent():
    # 同一差值，对照各自容差不同
    pairings = solve_pairings(
        [mp(1.0), mp(2.0)],
        [rp(1.2, 0.1), rp(2.2, 0.3)],
    )
    pairs = feasible_pairs(pairings)
    # 1.0 vs 1.2 差 0.2 超 0.1 容差；2.0 vs 2.2 差 0.2 在 0.3 内容差
    assert pairs == [(2.0, 2.2)]
    assert len(pairings) == 2


def test_larger_scale_twenty_points():
    measured = [mp(float(i)) for i in range(20)]
    references = [rp(float(i) + 0.05, 0.1) for i in range(20)]
    pairings = solve_pairings(measured, references)
    assert len(feasible_pairs(pairings)) == 20
    assert round(sum(p.abs_diff for p in pairings), 9) == 20 * 0.05


def test_deterministic_and_empty():
    assert solve_pairings([], []) == []
    pairings = solve_pairings(
        [mp(1.0, shard=2, row=1), mp(1.0, shard=1, row=0)],
        [rp(1.0, 0.0), rp(1.0, 0.0)],
    )
    again = solve_pairings(
        [mp(1.0, shard=2, row=1), mp(1.0, shard=1, row=0)],
        [rp(1.0, 0.0), rp(1.0, 0.0)],
    )
    first = [(p.measured.shard_id, p.measured.row_index) for p in pairings if p.feasible]
    second = [(p.measured.shard_id, p.measured.row_index) for p in again if p.feasible]
    assert first == second
