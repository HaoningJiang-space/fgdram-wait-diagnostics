#!/usr/bin/env python3
"""P1 event examples and bounded offline schedule search; stdlib only."""
import argparse
from dataclasses import asdict, replace
import hashlib
import json
from pathlib import Path
import platform
import subprocess
import sys
import time
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from fgdram.audit import check_trace
from fgdram.evidence import annotate_atoms, check_annotations, json_hash, LEGACY_CONTRACT
from fgdram.model import Config, Simulator, Request
from fgdram.workloads import balanced, stream
from fgdram.scheduling import PrioritySimulator, PeriodicWitness, finite_lower_bound, periodic_boundary_certificate


def save(path, value):
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n')


def trace_file(path, trace):
    path.write_text(''.join(json.dumps(e,sort_keys=True)+'\n' for e in trace))
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    ap=argparse.ArgumentParser();ap.add_argument('--output',default='results/p1_contract')
    ap.add_argument('--search-seconds',type=float,default=15)
    args=ap.parse_args();out=ROOT/args.output;out.mkdir(parents=True,exist_ok=True)
    if out.resolve() in ((ROOT/'results/first_round').resolve(),(ROOT/'results/p0_audit').resolve()):
        raise SystemExit('use a new output directory')
    tests=subprocess.run([sys.executable,'-m','unittest','discover','-s','tests','-v'],cwd=ROOT,capture_output=True,text=True)
    (out/'validation.txt').write_text(tests.stdout+tests.stderr)
    if tests.returncode:raise SystemExit('tests failed')
    hashes={};examples=[];schedules=[]
    for name,reqs in [('new_row_32',[Request(i,0,0,0,i,32) for i in range(2)]),
                      ('new_row_64',[Request(i,0,0,0,i,64) for i in range(2)]),
                      ('row_hits',stream(8))]:
        c=Config();s=Simulator(c,reqs);r=s.run();a=annotate_atoms(s.trace,c,reqs)
        hashes[f'{name}.jsonl']=trace_file(out/f'{name}.jsonl',s.trace)
        save(out/f'{name}-atoms.json',a)
        examples.append(dict(name=name,config=asdict(c),input_sha256=json_hash([asdict(x) for x in reqs]),
                             elapsed_ns=r['elapsed_ns'],audit=check_trace(s.trace,c,reqs),
                             annotation_audit=check_annotations(a,c)))
    # Four finite problems; known complete input is permitted only in this diagnostic.
    for grains,per_pb in [(2,2),(2,4),(8,2),(8,4)]:
        reqs=balanced(64,grains*2*per_pb,grains=grains)
        c=replace(Config(),max_ns=4000)
        baseline=Simulator(c,reqs);br=baseline.run();best,best_r,best_name=baseline,br,'legacy'
        trials=[];begin=time.monotonic();exhausted=False
        # Fixed, bounded priority family, not a randomized performance average.
        orders=[]
        for rotation in range(grains):
            order=list(range(rotation,grains))+list(range(rotation))
            for seq in (order,list(reversed(order))):
                if seq not in orders:orders.append(seq)
        for mode in ('grain','round','deadline'):
            for order in orders:
                if time.monotonic()-begin>args.search_seconds:
                    exhausted=True;break
                sim=PrioritySimulator(c,reqs,grain_order=order,mode=mode)
                result=sim.run();audit=check_trace(sim.trace,c,reqs)
                trials.append(dict(mode=mode,grain_order=order,elapsed_ns=result['elapsed_ns'],audit_pass=audit['pass']))
                if result['elapsed_ns']<best_r['elapsed_ns']:
                    best,best_r,best_name=sim,result,f'{mode}:{order}'
            if exhausted:break
        name=f'g{grains}_perpb{per_pb}'
        hashes[name+'-baseline.jsonl']=trace_file(out/(name+'-baseline.jsonl'),baseline.trace)
        hashes[name+'-best.jsonl']=trace_file(out/(name+'-best.jsonl'),best.trace)
        row=dict(name=name,grains=grains,records_per_pb=per_pb,config=asdict(c),
                 input_sha256=json_hash([asdict(x) for x in reqs]),
                 lower_bound_ns=finite_lower_bound(c,reqs),baseline_elapsed_ns=br['elapsed_ns'],
                 best_elapsed_ns=best_r['elapsed_ns'],best_policy=best_name,trials=trials,
                 search_seconds=time.monotonic()-begin,time_limit_seconds=args.search_seconds,
                 status='time_limit' if exhausted else 'fixed_policy_family_exhausted',
                 optimality='unresolved; heuristic search failure is not an infeasibility proof',
                 independent_audit=check_trace(best.trace,c,reqs))
        # Analytic schedule, same finite input; C only when eight grains are active.
        pc=replace(c,row_lanes=1 if grains==2 else 2)
        witness=PeriodicWitness(pc,reqs,phases={g:g*(4 if grains==2 else 2) for g in range(grains)})
        wr=witness.run();wa=check_trace(witness.trace,pc,reqs)
        wb=Simulator(pc,reqs);wbr=wb.run()
        hashes[name+'-periodic.jsonl']=trace_file(out/(name+'-periodic.jsonl'),witness.trace)
        row['periodic_witness']=dict(row_lanes=pc.row_lanes,elapsed_ns=wr['elapsed_ns'],
                                    same_hardware_greedy_elapsed_ns=wbr['elapsed_ns'],audit=wa)
        if per_pb==4:
            cert=periodic_boundary_certificate(witness.trace,records_per_period=2*grains)
            if not(cert['event_streams_equal'] and cert['carried_resource_state_equal']):
                raise RuntimeError('periodic boundary mismatch')
            row['periodic_witness']['boundary_certificate']=cert
            busy=periodic_boundary_certificate(witness.trace,start=80,records_per_period=2*grains)
            if not(busy['event_streams_equal'] and busy['carried_resource_state_equal']):
                raise RuntimeError('busy periodic boundary mismatch')
            row['periodic_witness']['busy_boundary_certificate']=busy
        schedules.append(row)
        print(name,br['elapsed_ns'],'->',best_r['elapsed_ns'],'ns;',len(trials),'legal alternatives',flush=True)
    save(out/'results.json',dict(timing_contract=LEGACY_CONTRACT,examples=examples,schedules=schedules))
    source=json.loads((ROOT/'sources/manifest.json').read_text())
    code={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest()
          for folder in ('fgdram','scripts','tests') for p in sorted((ROOT/folder).glob('*.py'))}
    save(out/'manifest.json',dict(python=platform.python_version(),command=sys.argv,timing_contract=LEGACY_CONTRACT,
                                source=source,code_sha256=code,trace_sha256=hashes,
                                parent_commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
                                hardware_calibration='not established; paper arrival/credit timings remain unknown'))
    lines=['# P1：事件契约和合法排程','',
           '数值仍使用 `legacy_0270afa`，没有缩短延迟。分段事件属于明确标注的 `legacy_atom_annotation_v1` 字节守恒示例；真实内部首段/末段时刻保留未知。',
           '', '## 有限输入排程对照','',
           '| 活动 grain | 每 pseudobank 记录数 | 必要下界 ns | 原调度 ns | 有限搜索最佳 ns | 分析排程 ns / 行通路数 |',
           '|---:|---:|---:|---:|---:|---:|']
    for r in schedules:
        w=r['periodic_witness']
        lines.append(f"| {r['grains']} | {r['records_per_pb']} | {r['lower_bound_ns']} | {r['baseline_elapsed_ns']} | {r['best_elapsed_ns']} | {w['elapsed_ns']} / {w['row_lanes']} |")
    lines += ['', '搜索只枚举固定优先级族，不证明全局最优。每份输出都通过独立检查器；完整命令轨迹、输入哈希、逐次搜索记录见本目录。',
              '', '## 周期边界与解释', '',
              '2-grain 单通路和 8-grain 双通路的分析排程在 128/192 以及 80/144 ns 边界检查了连续两个 64 ns 周期的事件，以及跨界命令、译码器、源端、出口、行占用、返回预约和激活窗口历史。它是旧模型下的硬件排程见证，有限输入队列本身不是周期状态。',
              '8-grain 单通路的“精确、永久无空隙 16 GB/s”受相位约束；推导及限定见 [事件契约](../../docs/04-event-contract.md)。未求得基线最优吞吐，不把有限排程差值归于硬件或调度器单一原因。',
              '', '## 下一阶段入口', '',
              'P1 的模型事件解释、守恒检查与合法排程交付完成；真实 FGDRAM 的未公开时序仍未知。P2 仅获准在明确的旧契约下做条件交互诊断，不能据此通过硬件外部校准 Gate。','']
    (out/'REPORT.md').write_text('\n'.join(lines))


if __name__=='__main__':main()
