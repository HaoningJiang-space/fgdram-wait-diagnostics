#!/usr/bin/env python3
"""Run validation before the bounded first-round diagnostic; stdlib only."""
import argparse
import csv
from dataclasses import asdict, replace
import hashlib
import json
from pathlib import Path
import platform
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from fgdram.audit import check_trace
from fgdram.model import Config, Request, Simulator
from fgdram.workloads import balanced, stream


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")


def write_jsonl(path, values):
    path.write_text("".join(json.dumps(v, ensure_ascii=False) + "\n" for v in values))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--output", default="results/first_round")
    ap.add_argument("--records", type=int, default=1024)
    args = ap.parse_args()
    out = ROOT / args.output
    out.mkdir(parents=True, exist_ok=True)
    (out / "traces").mkdir(exist_ok=True)
    (out / "micro_traces").mkdir(exist_ok=True)
    tests = subprocess.run([sys.executable, "-m", "unittest", "discover", "-s", "tests", "-v"],
                           cwd=ROOT, capture_output=True, text=True)
    (out / "validation.txt").write_text(tests.stdout + tests.stderr)
    if tests.returncode:
        raise SystemExit("Validation failed; performance experiments were not started.")
    suite = []
    receipts = {}

    def run(name, reqs, config, micro=False):
        sim = Simulator(config, reqs)
        result = sim.run()
        result["name"] = name
        result["independent_trace_audit"] = check_trace(sim.trace, config, reqs)
        logical = json.dumps([asdict(r) for r in reqs], sort_keys=True).encode()
        result["logical_input_sha256"] = hashlib.sha256(logical).hexdigest()
        trace_path = out / ("micro_traces" if micro else "traces") / f"{name}.jsonl"
        write_jsonl(trace_path, sim.trace)
        receipts[str(trace_path.relative_to(ROOT))] = digest(trace_path)
        # One example per blocker combination, never a normalized time breakdown.
        masks = {}
        for w in sim.wait_samples:
            masks.setdefault("+".join(w["blockers"]), w)
        result["representative_wait_masks"] = list(masks.values())
        suite.append(result)
        print(name, f"{result['useful_GB_s']:.4f} GB/s", "audit PASS", flush=True)
        return sim, result

    base = Config()
    cases = [
        ("same_row", stream(8), base),
        ("same_pb_new_rows", [Request(i, 0, 0, 0, i, 32) for i in range(8)], base),
        ("two_pb_overlap", balanced(64, 8, grains=1), base),
        ("same_sa_conflict", [Request(0, 0, 0, 0, 0, 64), Request(1, 1, 1, 0, 1, 32)], base),
        ("all_grains_act", balanced(32, 256, grains=128),
         replace(base, row_lanes=2, outstanding_records=256)),
        ("output_backpressure", balanced(64, 8, grains=1),
         replace(base, initial_output_stall_ns=160)),
    ]
    excerpts = []
    for name, reqs, c in cases:
        sim, result = run(name, reqs, c, micro=True)
        excerpts.extend({"case": name, **e} for e in sim.trace[:160])
    write_jsonl(out / "short_commands.jsonl", excerpts)

    main_results = []
    for size in (32, 64):
        reqs = balanced(size, args.records)
        for variant, changes in [("base", {}), ("C", {"row_lanes": 2}),
                                 ("D", {"flexible_output": True}),
                                 ("CD", {"row_lanes": 2, "flexible_output": True})]:
            _, r = run(f"q{size}_{variant}", reqs, replace(base, **changes))
            main_results.append(r)
    # Conclusion-sensitive unknowns: no fitting to published or desired throughput.
    sensitivities = []
    for size in (32, 64):
        for setting, changes in [
            ("act6", {"act_command_ns": 6}),
            ("buffer2", {"return_slots_per_grain": 2}),
            ("explicit_pre", {"auto_precharge": False}),
            ("trtp24", {"trtp_ns": 24}),
            ("rrd_die", {"rrd_scope": "die"}),
            ("power_group", {"power_scope": "group"}),
        ]:
            _, r = run(f"q{size}_{setting}", balanced(size, args.records), replace(base, **changes))
            sensitivities.append(r)
    # Negative controls: one serial chain and sequential row reuse, C/D combinations.
    controls = []
    for name, reqs in [("stream", stream(256)), ("serial", balanced(32, 64, serial=True))]:
        for variant, changes in [("base", {}), ("CD", {"row_lanes": 2, "flexible_output": True})]:
            _, r = run(f"control_{name}_{variant}", reqs, replace(base, **changes))
            controls.append(r)

    write_json(out / "runs.json", suite)
    columns = ["name", "records", "elapsed_ns", "lookups_per_s", "useful_GB_s",
               "middle_half_GB_s", "ACTs_per_lookup", "useful_bytes_per_ACT",
               "row_bus_utilization", "column_bus_utilization", "output_utilization"]
    with (out / "summary.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore", lineterminator="\n")
        w.writeheader()
        w.writerows(suite)
    source = ROOT / "sources/FGDRAM-MICRO2017.pdf"
    manifest = {
        "kind": "conditional command/resource diagnostic; not FGDRAM reproduction or application performance",
        "python": platform.python_version(), "command": sys.argv,
        "source_url": "https://www.cs.utexas.edu/~skeckler/pubs/MICRO_2017_Fine_Grained_DRAM.pdf",
        "source_pdf_sha256": digest(source) if source.exists() else None,
        "code_sha256": {str(p.relative_to(ROOT)): digest(p) for folder in ("fgdram", "scripts", "tests")
                        for p in sorted((ROOT / folder).glob("*.py"))},
        "trace_sha256": receipts,
        "A_status": "not defined: additional physical independence/circuit modifications not established",
        "power_boundary": "128-grain die retained; outside first 8 grains idle in throughput cases; full-die pressure tested separately",
        "validation_exit_code": tests.returncode,
        "runs": len(suite), "randomness": "none; each deterministic case run once",
    }
    write_json(out / "manifest.json", manifest)

    lines = [
        "# FGDRAM 最小等待诊断：第一轮结果", "",
        "这是一套新写的、显式列出假设的命令级诊断模型。当前目录原先为空，未找到已有 FGDRAM 模拟器。",
        "本轮完成的是模型内部一致性与约束校验，不是原作者模拟器复现、晶体管验证或应用加速评估。",
        "A 尚无可核验的额外物理独立域，故不运行 A/CA/AD/CAD。C、D、CD 是有成本未估的诊断干预。", "",
        "## 1. 32 B / 64 B 条件结果", "",
        "每条记录一次 ACT；32 B 为一次 READ，64 B 为两次 READ。8 个 grain 轮流分配，两个 pseudobank 交替，避开人为物理冲突。",
        "输入开始地址/编号顺序在两种长度之间固定。各结构重新生成命令；同长度各结构的逻辑输入哈希相同。",
        "配置：ACT 传输假设 4 ns，READ/PRE 传输假设 2 ns，64 条记录窗口，256 个原子请求容量，4 个返回槽/grain。",
        "下面是整个 8-grain 组的十进制 GB/s，不是单 grain，也不是整堆栈。中段值使用中间一半返回事件，不能当无限时间稳态证明。", "",
        "| 记录 | 配置 | 含启动/排空 GB/s | 中段 GB/s | ACT/记录 | READ/ACT 分布 | 行接口利用率 |",
        "|---|---|---:|---:|---:|---|---:|",
    ]
    for r in main_results:
        lines.append(f"| {r['name'].split('_')[0]} | {r['name'].split('_')[1]} | {r['useful_GB_s']:.3f} | {r['middle_half_GB_s']:.3f} | {r['ACTs_per_lookup']:.2f} | {r['reads_per_ACT_histogram']} | {r['row_bus_utilization']:.1%} |")
    lines += ["", "C 增加第二条行命令通路；行列两类接口仍分开，所有阵列时序和激活规则不变。",
              "D 仅在相邻两个 grain 的输出缓冲之后允许选择两个既有出口，仍保留每 grain 16 ns/atom 的源端能力、有限缓冲及总出口带宽；没有给 MAT 增加端口。",
              "A 未定义，不能据此断言增加合法行状态有效或无效。", "",
              "## 2. 缺失参数的敏感性", "",
              "这些都是显式替代假设，不能择优选取后宣称复现。每个变化相对于 base，其他配置固定。", "",
              "| 配置 | 含启动/排空 GB/s | 中段 GB/s |", "|---|---:|---:|"]
    for r in sensitivities:
        lines.append(f"| {r['name']} | {r['useful_GB_s']:.3f} | {r['middle_half_GB_s']:.3f} |")
    lines += ["", "act6：ACT 命令 6 ns；buffer2：返回槽位 2；explicit_pre：显式 PRE；trtp24：假设 read-hold 24 ns；rrd_die：tRRD 扩展到全 die；power_group：激活窗口按命令组计账。",
              "tRRD/激活窗口范围在只有一个命令组有流量时可能无法区分，不能据此确定真实作用范围；all_grains_act 短测试会驱动全部 16 组，并检查全 die 32 ACT/12 ns。", "",
              "## 3. 负面对照", "", "| 输入 | 配置 | GB/s |", "|---|---|---:|"]
    for r in controls:
        lines.append(f"| {r['name']} | 见配置名 | {r['useful_GB_s']:.3f} |")
    lines += ["", "## 4. 校验与证据", "",
              f"unittest 校验通过；全部 {len(suite)} 次运行逐条经独立区间/命令检查器审核。检查器不调用模拟器的 readiness 或 scheduler 函数。",
              "六类短测试及完整命令记录在 micro_traces/；short_commands.jsonl 是摘要，summary.csv / runs.json 为汇总和非互斥等待计数。",
              "traces/ 保存本地全量诊断命令，未加入 Git；每条轨迹与输入的 SHA-256 记录在 manifest.json / runs.json。",
              "等待单位是 candidate-ns：多个请求、多个原因可以重叠，绝不相加为 100%，也不直接换算为可加速比例。只截取前 4000 条等待样本选代表；全程汇总不截断。", "",
              "## 5. 证据边界与后续 Gate", "",
              "本轮可以检验命令带宽、行循环及源端读出约束如何竞争，不能识别真实产品的最优组织，也不能证明新 3D 结构的收益。",
              "需要补全 ACT 编码、PRE/read-hold、激活限制作用范围及更完整物理图后，才能称论文校验配置通过外部校准。",
              "由于该 Gate 尚未通过，暂不扩展 12 输入/288 次扫描，不接真实推荐模型，不进行 RTL、热仿真或 3D PPA 声称。",
              "资源和参数来源见 ../../docs/01-resource-contract.md；命令片段解释见 ../../docs/02-command-walkthrough.md。", "",
              "论文：[FGDRAM, MICRO 2017](https://www.cs.utexas.edu/~skeckler/pubs/MICRO_2017_Fine_Grained_DRAM.pdf)，§3.2–3.4、Figures 6–7、Table 2。", ""]
    (out / "REPORT.md").write_text("\n".join(lines))


if __name__ == "__main__":
    main()
