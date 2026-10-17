# FGDRAM 等待原因诊断

目的：解释短记录读取中每个操作为何等待，再用受限组合对照检验因果。不是新架构效果演示。

当前是从空目录新建的条件命令模型，使用 FGDRAM 原文公开结构/时序并显式列出缺失参数。**内部校验通过不等于原论文复现通过。** A 尚未建立额外独立电路契约，不运行；已实现 base/C/D/CD。

## 1. 审计与当前状态

已完成 P0：命令编号、发送/生效对应、行上下文、逐块返回和完成条件，以及独立重建请求/返回额度。六类原检查器漏检均有错误注入回归；原 30 组命令安排、完成时间和统计保持一致。

- [0270afa 审计结论与 P0/P1/P2 指导](docs/03-audit-next-stage-0270afa.md)
- [P0 本地复验报告](results/p0_audit/REPORT.md)、[逐组回执](results/p0_audit/receipt.json)
- [P0 日志格式与检查边界](docs/p0-trace-contract.md)

P1 已完成模型事件解释、三个分段/额度示例、有限排程搜索和周期首尾核查；真实内部传播/关行/额度参数仍待外部校准。P2 已完成 56 个条件运行和 70 组成对干预，39 个测试通过。旧结果保留在 `results/first_round/`，P0 证据在 `results/p0_audit/`。

- [P1 事件契约及 64 B 相位约束](docs/04-event-contract.md)
- [P1 排程与完整短轨迹](results/p1_contract/REPORT.md)
- [P2 主要发现：逐事件解释等待](docs/05-stage2-findings.md)
- [P2 完整表格与图](results/stage2/REPORT.md)、[汇总 CSV](results/stage2/summary.csv)、[核验回执](results/stage2/verification.json)

### 首轮历史结果

- [资源共享、来源和假设](docs/01-resource-contract.md)
- [短命令记录的逐步解释](docs/02-command-walkthrough.md)
- [32 B / 64 B 组合对照报告](results/first_round/REPORT.md)
- [汇总 CSV](results/first_round/summary.csv)、[完整统计](results/first_round/runs.json)、[来源与哈希](results/first_round/manifest.json)
- [六类短测试的原始轨迹](results/first_round/micro_traces/)、[自动校验记录](results/first_round/validation.txt)

## 2. 复现

Python 3.12，模拟、检查器和统计只用标准库，不需要 GPU 或数据集。交互图使用可选的 matplotlib；缺少它时仍输出完整数值表。在本目录运行：

```bash
python3 -m unittest discover -s tests -v
python3 scripts/verify_p0.py --output results/p0_recheck
python3 scripts/run_timing_contract.py --output results/p1_recheck
python3 scripts/run_stage2_interactions.py --output results/stage2_recheck
python3 scripts/verify_stage2_results.py --output results/stage2_recheck
# 仅运行当前版本的首轮输入（输出到新目录）：
python3 scripts/run_first_round.py --output results/current_replay --records 1024
```

`verify_p0.py` 从本地 Git 对象提取固定提交到临时目录，对照旧检查器漏洞、原轨迹哈希和新日志的逐事件投影；需要完整历史中的 `0270afa`，不需要网络。脚本先运行测试，失败就不启动诊断运行；每次运行再执行独立轨迹检查。输入确定性生成，每个配置只跑一次，不求多种子平均。结果目录是指定的生成目标，再运行会更新其中的同名文件；可指定新的目录保留历史。

本地保留全量性能轨迹 `results/first_round/traces/`，Git 排除它们和论文 PDF/提取全文。小规模校验轨迹、统计和 SHA-256 收据纳入版本管理。PDF 来源：[FGDRAM MICRO 2017](https://www.cs.utexas.edu/~skeckler/pubs/MICRO_2017_Fine_Grained_DRAM.pdf)。没有 PDF 也能运行，manifest 会明确记录源 PDF 哈希为空。

## 3. 代码

- `fgdram/model.py`：分离行列命令接口、物理行状态、源端泵读、有限返回槽、出口、跨组激活规则。
- `fgdram/audit.py`：从输出轨迹独立检查资源区间、时序、冲突、激活窗口及返回守恒，不调用调度器判定函数。
- `tests/test_audit_mutations.py`：六类已报告漏洞、额外事件链破坏和 D 正负对照。
- `scripts/verify_p0.py`：固定提交对照、30 组回归和历史文件保护。
- `fgdram/evidence.py`：分段守恒示例、时间窗口、空闲/预约区间和完整候选记录。
- `fgdram/scheduling.py`：单独命名的离线优先级搜索和周期见证，不替代默认调度。
- `scripts/run_stage2_interactions.py`：32 点交互、12 点多组压力、长度与 D 控制；所有候选完整记录。
- `scripts/verify_stage2_results.py`：按事件、输入和哈希检查 P2 结果，不重复测量。
- `fgdram/workloads.py`：固定物理起点的受控输入及负面对照。
- `tests/test_model.py`：手工时序预期、非法重叠、并行重叠、供电窗口、反压、依赖和损坏轨迹拒绝。

未进行实际应用或硬件加速测量；未实现现代 3D 目标配置、RTL、热模型或 PPA。
