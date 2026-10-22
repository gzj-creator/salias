#!/usr/bin/env python3
"""Compare the measured throughput of the S.2.2 L0-L4 binaries."""

import argparse
import json
from pathlib import Path
import re
from statistics import median


HIGH_MIN_MMSG_S = 15.0
LEVEL_DESCRIPTIONS = {
    0: "完全未插桩",
    1: "只计数成功",
    2: "+背压分支",
    3: "+完整计数器",
    4: "+计时",
}


def infer_level(directory, records):
    match = re.search(r"instrumentation-L([0-4])(?:-|$)", directory.name)
    if match:
        return int(match.group(1))
    levels = {int(record["instrumentation_level"]) for record in records
              if "instrumentation_level" in record}
    if len(levels) != 1:
        raise ValueError(f"Cannot infer one instrumentation level: {directory}")
    return levels.pop()


def load_group(directory):
    path = directory / "results.jsonl"
    if not path.is_file():
        raise ValueError(f"Missing results.jsonl: {directory}")
    records = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    measured = [record for record in records if record.get("phase") == "measured"]
    if not measured:
        raise ValueError(f"No measured records: {directory}")
    level = infer_level(directory, records)
    if level not in range(5):
        raise ValueError(f"Unexpected instrumentation level {level}: {directory}")
    values = []
    for record in measured:
        result = record.get("result", {})
        try:
            throughput = float(result["publish_msg_per_sec"]) / 1_000_000.0
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError(f"Invalid throughput in {directory}: {record}") from error
        values.append(throughput)
    return {
        "level": level,
        "directory": str(directory.resolve()),
        "rounds": len(values),
        "high_rounds": sum(value >= HIGH_MIN_MMSG_S for value in values),
        "median_mmsg_s": median(values),
        "min_mmsg_s": min(values),
        "max_mmsg_s": max(values),
        "values": values,
    }


def compare(directories):
    groups = [load_group(Path(directory)) for directory in directories]
    by_level = {}
    for group in groups:
        if group["level"] in by_level:
            raise ValueError(f"Duplicate level L{group['level']}")
        by_level[group["level"]] = group
    missing = sorted(set(range(5)) - set(by_level))
    if missing:
        raise ValueError("Missing levels: " + ", ".join(f"L{level}" for level in missing))

    ordered = [by_level[level] for level in range(5)]
    for index, group in enumerate(ordered):
        previous = ordered[index - 1] if index else None
        if previous is None or previous["median_mmsg_s"] == 0:
            group["jump_percent"] = None
        else:
            group["jump_percent"] = (
                group["median_mmsg_s"] / previous["median_mmsg_s"] - 1.0
            ) * 100.0

    transitions = []
    for previous, current in zip(ordered, ordered[1:]):
        transitions.append({
            "from": previous["level"],
            "to": current["level"],
            "median_change_percent": current["jump_percent"],
            "high_round_change": current["high_rounds"] - previous["high_rounds"],
        })

    crash = None
    for transition in transitions:
        previous = ordered[transition["from"]]
        current = ordered[transition["to"]]
        if (
            previous["high_rounds"] >= 15
            and current["high_rounds"] <= 2
        ) or (
            transition["high_round_change"] <= -5
            and (transition["median_change_percent"] or 0.0) <= -15.0
        ):
            crash = transition["to"]
            break
    if crash is None:
        negative = [
            transition for transition in transitions
            if transition["median_change_percent"] is not None
            and transition["median_change_percent"] < 0
        ]
        if negative:
            crash = min(negative, key=lambda item: item["median_change_percent"])["to"]

    return {
        "threshold_mmsg_s": HIGH_MIN_MMSG_S,
        "groups": ordered,
        "transitions": transitions,
        "state_crash_level": crash,
    }


def format_percent(value):
    return "-" if value is None else f"{value:+.1f}%"


def print_report(report):
    print("| 级别 | 描述 | 高模式轮数 | 中位数 | 跳变 |")
    print("|---|---|---:|---:|---:|")
    for group in report["groups"]:
        print(
            f"| L{group['level']} | {LEVEL_DESCRIPTIONS[group['level']]} | "
            f"{group['high_rounds']}/{group['rounds']} | "
            f"{group['median_mmsg_s']:.3f} Mmsg/s | "
            f"{format_percent(group['jump_percent'])} |"
        )
    print()
    crash = report["state_crash_level"]
    if crash is None:
        print("状态崩溃点：未检测到明确的级别跳变。")
    else:
        previous = report["groups"][crash - 1]
        current = report["groups"][crash]
        change = current["jump_percent"]
        print(
            f"状态崩溃点：L{crash}（L{crash - 1} -> L{crash}，"
            f"高模式 {previous['high_rounds']}/{previous['rounds']} -> "
            f"{current['high_rounds']}/{current['rounds']}，"
            f"中位数 {format_percent(change)}）。"
        )
        if crash == 1:
            print("解释：L1 即崩溃，说明成功计数带来的额外指令已足以破坏高模式。")
        elif crash == 2:
            print("解释：L2 崩溃，背压计数/分支是主要候选瓶颈。")
        elif crash == 3:
            print("解释：L3 崩溃，poll 详细计数的内存访问是主要候选瓶颈。")
        elif crash == 4:
            print("解释：L4 崩溃，计时读取开销是主要候选瓶颈。")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directories", nargs="+", type=Path)
    parser.add_argument("--json", dest="json_path", type=Path,
                        help="also write the machine-readable comparison")
    args = parser.parse_args()
    report = compare(args.directories)
    print_report(report)
    if args.json_path:
        args.json_path.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
