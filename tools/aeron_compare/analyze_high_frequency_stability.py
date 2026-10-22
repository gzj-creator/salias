#!/usr/bin/env python3
"""Analyze high-frequency stability patterns from a runner summary."""

import argparse
import json
from pathlib import Path
import statistics
import sys


HIGH_MODE_THRESHOLD_MMSG_S = 15.0
INTERVALS = (
    ("no_sleep", 0, 5),
    ("light_1s", 5, 10),
    ("medium_10s", 10, 15),
    ("heavy_60s", 15, 20),
)


def load_summary(output):
    """读取 runner 的组摘要，并拒绝缺失的组号。"""
    path = output / "summary.json"
    data = json.loads(path.read_text())
    groups = [int(group["group"]) for group in data]
    if groups != list(range(len(groups))):
        raise ValueError(f"Expected contiguous groups from zero, got {groups}")
    return data


def throughputs(data):
    """按测试时序展开所有吞吐量。"""
    return [
        float(rate)
        for group in data
        for rate in group.get("throughputs", [])
    ]


def analyze_by_interval(data, threshold=HIGH_MODE_THRESHOLD_MMSG_S):
    """按启动前等待时间汇总高模式比例。"""
    results = {}
    for name, start, end in INTERVALS:
        rates = [
            float(rate)
            for group in data[start:end]
            for rate in group.get("throughputs", [])
        ]
        high_count = sum(rate >= threshold for rate in rates)
        results[name] = {
            "high_mode_rate": high_count / len(rates) * 100 if rates else 0.0,
            "high_count": high_count,
            "total": len(rates),
        }
    return results


def analyze_transitions(data, threshold=HIGH_MODE_THRESHOLD_MMSG_S):
    """计算高/低模式连续段与状态切换次数。"""
    states = [
        "H" if rate >= threshold else "L"
        for rate in throughputs(data)
    ]
    runs = []
    if states:
        current = states[0]
        length = 1
        for state in states[1:]:
            if state == current:
                length += 1
            else:
                runs.append((current, length))
                current = state
                length = 1
        runs.append((current, length))

    high_runs = [length for state, length in runs if state == "H"]
    low_runs = [length for state, length in runs if state == "L"]
    return {
        "avg_high_run": statistics.mean(high_runs) if high_runs else 0.0,
        "avg_low_run": statistics.mean(low_runs) if low_runs else 0.0,
        "max_high_run": max(high_runs, default=0),
        "max_low_run": max(low_runs, default=0),
        "high_runs": high_runs,
        "low_runs": low_runs,
        "total_transitions": max(len(runs) - 1, 0),
        "runs": [{"state": state, "length": length} for state, length in runs],
    }


def analyze_group_starts(data, threshold=HIGH_MODE_THRESHOLD_MMSG_S):
    """比较高模式在每组开头与其后轮次中的占比。"""
    first = []
    later = []
    for group in data:
        rates = [float(rate) for rate in group.get("throughputs", [])]
        if rates:
            first.append(rates[0])
            later.extend(rates[1:])
    return {
        "first_round_high_rate": (
            sum(rate >= threshold for rate in first) / len(first) * 100
            if first else 0.0
        ),
        "later_rounds_high_rate": (
            sum(rate >= threshold for rate in later) / len(later) * 100
            if later else 0.0
        ),
    }


def analyze_round_positions(data, threshold=HIGH_MODE_THRESHOLD_MMSG_S):
    """按组内轮次位置统计高模式率，识别启动/预热偏差。"""
    rounds = max((len(group.get("throughputs", [])) for group in data), default=0)
    rows = []
    for round_idx in range(rounds):
        rates = [
            float(group["throughputs"][round_idx])
            for group in data
            if round_idx < len(group.get("throughputs", []))
        ]
        high = sum(rate >= threshold for rate in rates)
        rows.append({
            "round": round_idx,
            "high_mode_rate": high / len(rates) * 100 if rates else 0.0,
            "high_count": high,
            "total": len(rates),
        })
    return rows


def analyze_by_client(data, environment):
    """记录客户端身份，防止把不同构建的结果混为同一实验。"""
    return {
        "path": environment.get("client"),
        "sha256": environment.get("client_sha256"),
    }


def summarize(data, environment=None):
    """生成可供报告引用的完整分析对象。"""
    rates = throughputs(data)
    if not rates:
        raise ValueError("Summary contains no throughput observations")
    expected_groups = int((environment or {}).get("groups", len(data)))
    threshold = float(
        (environment or {}).get("high_mode_threshold_mmsg_s", HIGH_MODE_THRESHOLD_MMSG_S)
    )
    high_count = sum(rate >= threshold for rate in rates)
    result = {
        "threshold_mmsg_s": threshold,
        "groups": len(data),
        "expected_groups": expected_groups,
        "complete": len(data) == expected_groups,
        "rounds": len(rates),
        "high_mode_count": high_count,
        "high_mode_rate": high_count / len(rates) * 100,
        "median_mmsg_s": statistics.median(rates),
        "mean_mmsg_s": statistics.mean(rates),
        "min_mmsg_s": min(rates),
        "max_mmsg_s": max(rates),
        "by_interval": analyze_by_interval(data, threshold),
        "transitions": analyze_transitions(data, threshold),
        "group_starts": analyze_group_starts(data, threshold),
        "round_positions": analyze_round_positions(data, threshold),
        "groups_detail": [
            {
                "group": int(group["group"]),
                "interval": group.get("interval"),
                "sleep_duration": group.get("sleep_duration"),
                "median_mmsg_s": float(group["median"]),
                "high_mode_count": int(group["high_mode_count"]),
                "rounds": len(group.get("throughputs", [])),
            }
            for group in data
        ],
    }
    if environment is not None:
        result["client"] = analyze_by_client(data, environment)
    return result


def print_result(result):
    """按验收要求打印简洁结果。"""
    print("=== High-Frequency Stability Analysis ===\n")
    print(
        f"Total high-mode rounds: {result['high_mode_count']}/{result['rounds']} "
        f"({result['high_mode_rate']:.1f}%)\n"
    )
    print("High-mode rate by sleep interval:")
    for name, _, _ in INTERVALS:
        stats = result["by_interval"][name]
        print(
            f"  {name:15} {stats['high_mode_rate']:5.1f}% "
            f"({stats['high_count']:3}/{stats['total']})"
        )

    transitions = result["transitions"]
    print()
    print(f"Average high-mode run length: {transitions['avg_high_run']:.1f} rounds")
    print(f"Average low-mode run length:  {transitions['avg_low_run']:.1f} rounds")
    print(f"Longest high/low run:         {transitions['max_high_run']}/"
          f"{transitions['max_low_run']} rounds")
    print(f"Total state transitions:      {transitions['total_transitions']}")
    print()
    print(f"First-round high-mode rate:   "
          f"{result['group_starts']['first_round_high_rate']:.1f}%")
    print(f"Later-round high-mode rate:   "
          f"{result['group_starts']['later_rounds_high_rate']:.1f}%")
    print()
    print(f"Median throughput:            {result['median_mmsg_s']:.3f} Mmsg/s")
    print(f"Mean throughput:              {result['mean_mmsg_s']:.3f} Mmsg/s")
    print(f"Range:                        {result['min_mmsg_s']:.3f}.."
          f"{result['max_mmsg_s']:.3f} Mmsg/s")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    output = args.output.resolve()
    environment_path = output / "environment.json"
    environment = (
        json.loads(environment_path.read_text())
        if environment_path.is_file()
        else None
    )
    summary = load_summary(output)
    result = summarize(summary, environment)
    print_result(result)
    if not result["complete"]:
        print(
            "\nINCOMPLETE: analysis is diagnostic only; do not use these "
            "values for the 400-round acceptance decision."
        )
    path = output / "high-frequency-analysis.json"
    path.write_text(json.dumps(result, indent=2) + "\n")
    print(f"\nwrote {path}")


if __name__ == "__main__":
    main()
