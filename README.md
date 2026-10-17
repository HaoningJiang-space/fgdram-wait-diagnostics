# FGDRAM 等待原因诊断

目的：解释短记录读取中每个操作为何等待，再用受限组合对照检验因果。不是新架构效果演示。

当前是从空目录新建的条件命令模型，使用 FGDRAM 原文公开结构/时序并显式列出缺失参数。**内部校验通过不等于原论文复现通过。** A 尚未建立额外独立电路契约，不运行；已实现 base/C/D/CD。

## 1. 第一份结果

- [资源共享、来源和假设](docs/01-resource-contract.md)
- [短命令记录的逐步解释](docs/02-command-walkthrough.md)
- [32 B / 64 B 组合对照报告](results/first_round/REPORT.md)
- [汇总 CSV](results/first_round/summary.csv)、[完整统计](results/first_round/runs.json)、[来源与哈希](results/first_round/manifest.json)
- [六类短测试的原始轨迹](results/first_round/micro_traces/)、[自动校验记录](results/first_round/validation.txt)

## 2. 复现

Python 3.12，只有标准库，不需要 GPU、下载数据集或安装依赖。在本目录运行：

```bash
python3 -m unittest discover -s tests -v
python3 scripts/run_first_round.py --output results/first_round --records 1024
```

脚本先运行测试，失败就不启动诊断运行；每次运行再执行独立轨迹检查。输入确定性生成，每个配置只跑一次，不求多种子平均。结果目录是指定的生成目标，再运行会更新其中的同名文件；可指定新的目录保留历史。

本地保留全量性能轨迹 `results/first_round/traces/`，Git 排除它们和论文 PDF/提取全文。小规模校验轨迹、统计和 SHA-256 收据纳入版本管理。PDF 来源：[FGDRAM MICRO 2017](https://www.cs.utexas.edu/~skeckler/pubs/MICRO_2017_Fine_Grained_DRAM.pdf)。没有 PDF 也能运行，manifest 会明确记录源 PDF 哈希为空。

## 3. 代码

- `fgdram/model.py`：分离行列命令接口、物理行状态、源端泵读、有限返回槽、出口、跨组激活规则。
- `fgdram/audit.py`：从输出轨迹独立检查资源区间、时序、冲突、激活窗口及返回守恒，不调用调度器判定函数。
- `fgdram/workloads.py`：固定物理起点的受控输入及负面对照。
- `tests/test_model.py`：手工时序预期、非法重叠、并行重叠、供电窗口、反压、依赖和损坏轨迹拒绝。

未进行实际应用或硬件加速测量；未实现现代 3D 目标配置、RTL、热模型或 PPA。
