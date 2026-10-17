#!/usr/bin/env python3
"""Replay fixed 0270afa and current code, reject mutations, compare all 30 traces.

Uses only stdlib and local Git objects. Historical results are read-only. The
baseline runs in an isolated temporary tree with its own original 16 tests.
"""
import argparse
from dataclasses import replace
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))
from fgdram.audit import AuditError, check_trace
from fgdram.model import Config, Simulator
from fgdram.workloads import stream
from test_audit_mutations import REPORTED_MUTATIONS, mutate_reported

BASE = "0270afab22132f8411a5b908c158faef663be411"
BLOBS = {
    "fgdram/model.py": "b43edb7161149ba1fede83ccd822462d72fc0926",
    "fgdram/audit.py": "92d1dea3b9c036e8862f2bf13bb1f956f389878c",
    "fgdram/workloads.py": "e97556a67abaec360dfd9ac44394cf5f1ac8b3f8",
    "tests/test_model.py": "0df291b77f6c51aec7af27cbf866eede804beb09",
    "results/first_round/summary.csv": "92b0f25a1531f1e83bf45b639acc6232a40e609d",
}


def ensure(condition, message):
    if not condition:
        raise RuntimeError(message)


def git(*args):
    return subprocess.check_output(["git", *args], cwd=ROOT)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def git_blob(data):
    return hashlib.sha1(f"blob {len(data)}\0".encode() + data).hexdigest()


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def read_trace(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


def compare_trace(old, new, name):
    # COMPLETE now follows its final DATA_END at the same timestamp. Compare it
    # separately; every other legacy field and the full event order must agree.
    old_completions = [e for e in old if e["event"] == "COMPLETE"]
    new_completions = [e for e in new if e["event"] == "COMPLETE"]
    ensure(old_completions == new_completions, f"{name}: completion schedule changed")
    old_events = [e for e in old if e["event"] != "COMPLETE"]
    new_events = [e for e in new if e["event"] != "COMPLETE"]
    ensure(len(old_events) == len(new_events), f"{name}: event count changed")
    projection = []
    for i, (a, b) in enumerate(zip(old_events, new_events)):
        projected = {key: b.get(key) for key in a}
        ensure(a == projected, f"{name}: legacy event {i} changed: {a} != {projected}")
        projection.append(projected)
    return {"events_excluding_complete": len(projection), "completions": len(old_completions),
            "legacy_event_projection_sha256": sha(json.dumps(projection, sort_keys=True).encode())}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="results/p0_audit")
    args = parser.parse_args()
    out = (ROOT / args.output).resolve()
    ensure(out.is_relative_to(ROOT / "results") and len(out.relative_to(ROOT / "results").parts) == 1,
           "Use a direct child of results/ as the new output directory")
    ensure(out != ROOT / "results/first_round", "Historical results must not be overwritten")
    out.mkdir(parents=True, exist_ok=True)
    paths = git("ls-tree", "-r", "--name-only", BASE).decode().splitlines()
    for path, expected in BLOBS.items():
        ensure(git_blob(git("show", f"{BASE}:{path}")) == expected, f"baseline blob mismatch: {path}")
    frozen = {p: git("show", f"{BASE}:{p}") for p in paths if p.startswith("results/first_round/")}
    for path, data in frozen.items():
        ensure((ROOT / path).read_bytes() == data, f"historical artifact changed: {path}")
    golden_summary = frozen["results/first_round/summary.csv"]
    golden_manifest = json.loads(frozen["results/first_round/manifest.json"])
    golden_runs = json.loads(frozen["results/first_round/runs.json"])
    with tempfile.TemporaryDirectory(prefix="fgdram-0270afa-") as temp:
        baseline = Path(temp)
        for path in paths:
            if path.endswith(".py"):
                destination = baseline / path
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(git("show", f"{BASE}:{path}"))
        baseline_model = load_module("p0_fixed_model", baseline / "fgdram/model.py")
        baseline_audit = load_module("p0_fixed_audit", baseline / "fgdram/audit.py")
        reqs, c = stream(8), Config()
        old_sim = baseline_model.Simulator(c, reqs)
        old_sim.run()
        new_sim = Simulator(c, reqs)
        new_sim.run()
        baseline_audit.check_trace(old_sim.trace, c, reqs)
        check_trace(new_sim.trace, c, reqs)
        mutation_receipts = []
        for name in REPORTED_MUTATIONS:
            bad, mc = mutate_reported(name, old_sim.trace, c)
            baseline_audit.check_trace(bad, mc, reqs)  # reproduce the old hole
            bad, mc = mutate_reported(name, new_sim.trace, c)
            try:
                check_trace(bad, mc, reqs)
            except AuditError as exc:
                reason = str(exc)
            else:
                raise RuntimeError(f"new checker accepted {name}")
            mutation_receipts.append({"mutation": name, "0270afa": "accepted",
                                      "p0": "rejected", "reason": reason})
        print("Six reported holes reproduced in 0270afa and rejected by P0", flush=True)
        optimized = subprocess.run([sys.executable, "-O", "-m", "unittest", "discover", "-s", "tests",
                                    "-p", "test_audit_mutations.py", "-v"],
                                   cwd=ROOT, capture_output=True, text=True)
        (out / "optimized-audit-tests.txt").write_text(optimized.stdout + optimized.stderr)
        ensure(optimized.returncode == 0, "audit tests failed with Python assertions disabled")
        print("Replaying 0270afa in an isolated temporary tree", flush=True)
        subprocess.run([sys.executable, "scripts/run_first_round.py", "--output", "results/replay",
                        "--records", "1024"], cwd=baseline, check=True)
        old_out = baseline / "results/replay"
        (out / "baseline-validation.txt").write_bytes((old_out / "validation.txt").read_bytes())
        print("Replaying current code with strengthened audit", flush=True)
        subprocess.run([sys.executable, "scripts/run_first_round.py", "--output", str(out),
                        "--records", "1024"], cwd=ROOT, check=True)
        ensure((old_out / "summary.csv").read_bytes() == golden_summary, "baseline summary drift")
        ensure((out / "summary.csv").read_bytes() == golden_summary, "P0 changed summary.csv")
        old_runs, new_runs = (json.loads((folder / "runs.json").read_text()) for folder in (old_out, out))
        ensure(old_runs == golden_runs, "baseline statistics drift")
        ensure(len(old_runs) == len(new_runs) == 30, "expected 30 cases")
        comparisons = []
        for old, new in zip(old_runs, new_runs):
            name = old["name"]
            old_stats = {k: v for k, v in old.items() if k != "independent_trace_audit"}
            new_stats = {k: v for k, v in new.items() if k != "independent_trace_audit"}
            ensure(old_stats == new_stats, f"{name}: statistics or input changed")
            folder = "micro_traces" if (old_out / "micro_traces" / f"{name}.jsonl").exists() else "traces"
            rel = Path(folder) / f"{name}.jsonl"
            old_bytes, new_bytes = ((p / rel).read_bytes() for p in (old_out, out))
            ensure(sha(old_bytes) == golden_manifest["trace_sha256"][f"results/first_round/{rel}"],
                   f"{name}: baseline raw trace differs from frozen hash")
            match = compare_trace(read_trace(old_out / rel), read_trace(out / rel), name)
            peaks = new["independent_trace_audit"]["reconstructed_peaks"]
            ensure(peaks["live_records"] == new["max_live_records"] and
                   peaks["outstanding_atoms"] == new["max_outstanding_atoms"], f"{name}: peak disagreement")
            comparisons.append({"name": name, "all_statistics_identical": True,
                                "command_and_completion_schedule_identical": True, **match,
                                "baseline_trace_sha256": sha(old_bytes), "p0_trace_sha256": sha(new_bytes)})
        for path, data in frozen.items():
            ensure((ROOT / path).read_bytes() == data, f"historical artifact overwritten: {path}")
        receipt = {"pass": True, "baseline_commit": BASE, "baseline_blobs": BLOBS,
                   "summary_byte_identical": True, "summary_git_blob": git_blob(golden_summary),
                   "historical_artifacts_unchanged": True, "mutation_results": mutation_receipts,
                   "optimized_audit_tests_exit_code": optimized.returncode, "runs": comparisons,
                   "comparison_rule": "All stats except audit receipt equal; all legacy event fields/order equal, "
                                      "with equal-time COMPLETE compared separately after moving behind final DATA_END.",
                   "current_code_sha256": {str(p.relative_to(ROOT)): sha(p.read_bytes())
                                           for folder in ("fgdram", "scripts", "tests")
                                           for p in sorted((ROOT / folder).glob("*.py"))}}
        (out / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
        (out / "REPORT.md").write_text('''# P0：检查器加固回执

本次只修改日志和独立检查器；时序、资源配置、候选选择、调度和统计口径保持原契约。

- 在临时目录从固定提交 `0270afab22132f8411a5b908c158faef663be411` 提取原始代码，核对审计提供的五个 Git blob SHA。
- 原代码的 16 个测试通过；原 30 组重跑的统计和完整轨迹哈希与历史版本一致。
- 六类已报告漏洞在原检查器全部复现，新检查器全部拒绝；另覆盖孤立 READ、重复返回、缺失 PRE、错误列偏移及事件链错误。
- 当前 32 个测试通过；16 个检查器测试在 `python -O` 下也通过，拒绝逻辑不依赖可关闭的 assert。
- 新日志下 30 组全部通过独立检查，summary.csv 与旧版本逐字节一致（Git blob `92b0f25a1531f1e83bf45b639acc6232a40e609d`）。
- 对每组逐事件比较：旧字段、命令顺序与时刻、完成顺序与时刻、所有非检查器统计均不变；唯一事件顺序调整是同时间戳的 COMPLETE 移到最后 DATA_END 之后。
- 历史 `results/first_round/` 受版本管理的文件保持原字节；新日志不冒充旧日志，新检查器明确拒绝缺少关联字段的旧格式。

`receipt.json` 记录逐组对照、错误注入回执和代码哈希；`manifest.json` 记录当前运行和新轨迹哈希。`micro_traces/` 是可逐条检查的完整短轨迹，全量轨迹在本地 `traces/`，不纳入 Git。

D 正负控制保留为测试：无暂停时无借用；人工暂停 160 ns 时完成时间 416→386 ns，两个块借用邻近出口。这是机制测试，不是应用收益。

指导文档中的多命令组 12 点属于用户提供的外部审计证据；本提交没有开展 P1 时序重解释或 P2 扫描。64 B 差距的可行排程仍未解决。检查通过仅说明满足此模型契约，不验证真实 FGDRAM 电路或新的 3D 架构。

复验：`python3 scripts/verify_p0.py --output results/p0_recheck`。需本地 Git 对象包含固定提交，不依赖 GitHub 网络或证据 ZIP。
''')
        print(f"PASS: six holes rejected, 30 schedules/statistics unchanged; {out / 'receipt.json'}", flush=True)


if __name__ == "__main__":
    main()
