"""一对一配对算法：先最大化容差内配对数，再最小化绝对差总和。

问题规模有硬性上限（实测、对照各不超过 20），采用方阵匈牙利算法（O(k³)）。
通过对成本做字典序编码，一次最小化即可同时满足两个目标：

* 实-实且在容差内（可行边）：成本 = 绝对差；
* 实-实但超出容差（不可行边）：成本 = 2*BIG + 绝对差；
* 与虚拟节点相连的边（两侧数量不等时补方）：成本 = BIG。

设可行配对数为 f，不可行实-实配对数为 g，则总 BIG 单元数 = k - f
（f 条可行边贡献 0，g 条不可行边贡献 2g，(k-f-g) 条虚拟边贡献 k-f-g，
合计 2g + k - f - g = k - f + g）。f 越大该值越小，因此最小化成本
首先最大化容差内配对数；f 相同的方案 BIG 单元数相同，随后由绝对差
总和（含不可行边的差，不影响可行性比较）决定胜负，即第二目标最小化
绝对差总和。BIG 取严格大于任何可行方案绝对差总和的有限值，保证两层
目标不会串扰。
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import List, Optional, Sequence


@dataclass(frozen=True)
class MeasuredPoint:
    value: float
    shard_id: int
    row_index: int


@dataclass(frozen=True)
class ReferencePoint:
    value: float
    tolerance: float
    source: Optional[str]


@dataclass(frozen=True)
class Pairing:
    """一条配对结果。任一侧为 None 表示该侧未配上（与虚拟节点配对）。"""
    measured: Optional[MeasuredPoint]
    reference: Optional[ReferencePoint]
    abs_diff: Optional[float]
    feasible: bool


def _hungarian(cost: List[List[float]]) -> List[int]:
    """标准方阵匈牙利最小权匹配（1-下标势能实现），返回每行匹配的列号。"""
    n = len(cost)
    if n == 0:
        return []
    inf = math.inf
    u = [0.0] * (n + 1)
    v = [0.0] * (n + 1)
    p = [0] * (n + 1)   # p[j] = 与列 j 配对的行
    way = [0] * (n + 1)  # way[j] = 增广路上列 j 的前驱列
    for i in range(1, n + 1):
        p[0] = i
        j0 = 0
        minv = [inf] * (n + 1)
        used = [False] * (n + 1)
        while p[j0] != 0:
            used[j0] = True
            i0 = p[j0]
            delta = inf
            j1 = 0
            row = cost[i0 - 1]
            for j in range(1, n + 1):
                if not used[j]:
                    cur = row[j - 1] - u[i0] - v[j]
                    if cur < minv[j]:
                        minv[j] = cur
                        way[j] = j0
                    if minv[j] < delta:
                        delta = minv[j]
                        j1 = j
            for j in range(n + 1):
                if used[j]:
                    u[p[j]] += delta
                    v[j] -= delta
                else:
                    minv[j] -= delta
            j0 = j1
        # 增广
        while True:
            j1 = way[j0]
            p[j0] = p[j1]
            j0 = j1
            if j0 == 0:
                break
    assignment = [0] * n
    for j in range(1, n + 1):
        assignment[p[j] - 1] = j - 1
    return assignment


def solve_pairings(
    measured: Sequence[MeasuredPoint],
    references: Sequence[ReferencePoint],
) -> List[Pairing]:
    """求最优一对一配对。返回长度 max(m, r) 的配对列表，未配上的一侧为 None。"""
    m = len(measured)
    r = len(references)
    k = max(m, r)
    if k == 0:
        return []

    # BIG 严格大于任意实-实边绝对差总和的上界，且保持有限
    max_diff = 0.0
    for a in measured:
        for b in references:
            d = abs(a.value - b.value)
            if d > max_diff:
                max_diff = d
    big = max_diff * max(m, 1) * max(r, 1) + 1.0

    cost: List[List[float]] = [[0.0] * k for _ in range(k)]
    for i in range(k):
        for j in range(k):
            if i < m and j < r:
                diff = abs(measured[i].value - references[j].value)
                if diff <= references[j].tolerance:
                    cost[i][j] = diff          # 可行边
                else:
                    cost[i][j] = 2.0 * big + diff  # 不可行实-实边
            else:
                cost[i][j] = big               # 虚拟边

    assignment = _hungarian(cost)

    pairings: List[Pairing] = []
    for i in range(k):
        j = assignment[i]
        if i < m and j < r:
            diff = abs(measured[i].value - references[j].value)
            pairings.append(
                Pairing(
                    measured=measured[i],
                    reference=references[j],
                    abs_diff=diff,
                    feasible=diff <= references[j].tolerance,
                )
            )
        elif i < m:
            pairings.append(Pairing(measured=measured[i], reference=None, abs_diff=None, feasible=False))
        else:
            pairings.append(Pairing(measured=None, reference=references[j], abs_diff=None, feasible=False))
    return pairings
