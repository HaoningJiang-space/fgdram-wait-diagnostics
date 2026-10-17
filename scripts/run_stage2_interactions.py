#!/usr/bin/env python3
"""P2: fixed 32-point factorial + 12 scope + 8 new length + 4 D controls.

All runs use legacy_0270afa; no new timing assumptions or physical resources.
Each deterministic case is run once. The four 1024-record length points are
reused from the factorial, and both old event-index and new time windows appear.
"""
import argparse
from collections import Counter, deque
from dataclasses import asdict, replace
import csv
import gzip
import hashlib
import itertools
import json
from pathlib import Path
import platform
import subprocess
import sys
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from fgdram.audit import check_trace
from fgdram.evidence import CandidateRecorder, LEGACY_CONTRACT, json_hash, metrics
from fgdram.model import Config, Simulator
from fgdram.workloads import balanced


def save(path,value):
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n')


def digest(path):return hashlib.sha256(path.read_bytes()).hexdigest()


def cases():
    base=Config();specs=[]
    for q,a,s,h,c in itertools.product((32,64),(4,6),(2,4),(16,24),(1,2)):
        specs.append(dict(name=f'f_q{q}_a{a}_s{s}_h{h}_C{c}',kind='factorial',size=q,records=1024,grains=8,
                          config=replace(base,act_command_ns=a,return_slots_per_grain=s,trtp_ns=h,row_lanes=c)))
    for groups,q,c in itertools.product((1,4,16),(32,64),(1,2)):
        specs.append(dict(name=f'scope_g{groups}_q{q}_C{c}',kind='scope',size=q,records=1024,grains=groups*8,
                          config=replace(base,row_lanes=c,outstanding_records=256,atom_queue_capacity=512)))
    for n,q,c in itertools.product((256,4096),(32,64),(1,2)):
        specs.append(dict(name=f'len_n{n}_q{q}_C{c}',kind='length',size=q,records=n,grains=8,
                          config=replace(base,row_lanes=c)))
    for stall,flex in itertools.product((0,160),(False,True)):
        specs.append(dict(name=f'd_stall{stall}_flex{int(flex)}',kind='D_control',size=64,records=8,grains=1,
                          config=replace(base,initial_output_stall_ns=stall,flexible_output=flex)))
    return specs


def command_map(path):
    result={}
    with gzip.open(path,'rt') as f:
        for line in f:
            e=json.loads(line)
            if e['event']=='COMMAND':
                result[(e['command'],e['rid'],e.get('atom',-1))]=e
    return result


def intervention(a,b,out,reason):
    old=command_map(out/'traces'/f"{a['name']}.jsonl.gz")
    new=command_map(out/'traces'/f"{b['name']}.jsonl.gz")
    counts=Counter();advanced=[]
    for key,e in new.items():
        if key not in old:counts['new_command']+=1;continue
        before=old[key]
        delta=e['accept_ns']-before['accept_ns']
        label='earlier' if delta<0 else 'later' if delta>0 else 'same_accept_time'
        counts[label]+=1
        if delta<0:
            advanced.append(dict(command=e['command'],rid=e['rid'],atom=e.get('atom'),
                                 grain=e['grain'],pb=e['pb'],baseline_dispatch_ns=before['time_ns'],
                                 intervention_dispatch_ns=e['time_ns'],baseline_accept_ns=before['accept_ns'],
                                 intervention_accept_ns=e['accept_ns']))
    advanced.sort(key=lambda x:(x['intervention_accept_ns'],x['rid']))
    examples=advanced[:4]
    wanted={(e['intervention_dispatch_ns'],e['command'],e['grain'],e['pb'],e['rid']):e for e in examples}
    if wanted:
        with gzip.open(out/'traces'/f"{a['name']}-candidates.jsonl.gz",'rt') as f:
            for line in f:
                row=json.loads(line);key=(row['time_ns'],*row['candidate'])
                if key in wanted:
                    wanted[key]['baseline_mask_at_intervention_dispatch']=row['blockers']
                    wanted[key]['baseline_issued_at_that_tick']=row['issued']
                if row['time_ns']>max(k[0] for k in wanted):break
    for e in examples:
        if 'baseline_mask_at_intervention_dispatch' not in e:
            e['baseline_mask_at_intervention_dispatch']=None
            e['mask_note']='candidate not visible in baseline at this tick; prior schedule already differs'
    return dict(baseline=a['name'],intervention=b['name'],changed_rule=reason,
                logical_input_equal=a['logical_input_sha256']==b['logical_input_sha256'],
                elapsed_delta_ns=b['elapsed_ns']-a['elapsed_ns'],
                middle_time_GB_s_delta=b['middle_time_GB_s']-a['middle_time_GB_s'],
                command_accept_comparison=dict(counts),first_advanced_operations=examples,
                interpretation='paired intervention, not an additive attribution; differing histories retained')


def make_pairs(runs,out):
    lookup={r['name']:r for r in runs};pairs=[]
    axes=('act_command_ns','return_slots_per_grain','trtp_ns','row_lanes')
    for a in runs:
        if a['kind']!='factorial':continue
        for field,old,new in zip(axes,(6,2,24,1),(4,4,16,2)):
            if a['config'][field]!=old:continue
            cfg={**a['config'],field:new}
            b=next((r for r in runs if r['kind']=='factorial' and r['size']==a['size'] and r['config']==cfg),None)
            if b:pairs.append(intervention(a,b,out,f'{field}: {old} -> {new}'))
    for a in runs:
        if a['kind']=='scope' and a['config']['row_lanes']==1:
            b=lookup.get(a['name'][:-1]+'2')
            if b:pairs.append(intervention(a,b,out,'row_lanes: 1 -> 2; shared die activation budget unchanged'))
    return pairs


def make_figure(out,runs):
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
    except ImportError:
        return 'matplotlib unavailable; all numerical tables remain available'
    lookup={r['name']:r for r in runs}
    needed=[f'f_q{q}_a{a}_s{s}_h{h}_C{c}' for q,a,s,h,c in itertools.product((32,64),(4,6),(2,4),(16,24),(1,2))]
    if not all(n in lookup for n in needed):return 'partial run; figure omitted'
    fig,axes=plt.subplots(1,2,figsize=(12,5),layout='constrained')
    for ax,q in zip(axes,(32,64)):
        values=[[lookup[f'f_q{q}_a{a}_s{s}_h{h}_C{c}']['middle_time_GB_s']
                 for a,h in itertools.product((4,6),(16,24))] for s,c in itertools.product((2,4),(1,2))]
        im=ax.imshow(values,vmin=0,vmax=16,cmap='viridis')
        for i,row in enumerate(values):
            for j,v in enumerate(row):ax.text(j,i,f'{v:.3f}',ha='center',va='center',color='black' if v>10 else 'white')
        ax.set_title(f'{q} B per record')
        ax.set_xticks(range(4),[f'ACT {a}\nhold {h}' for a,h in itertools.product((4,6),(16,24))])
        ax.set_yticks(range(4),[f'slots {s}, lanes {c}' for s,c in itertools.product((2,4),(1,2))])
    fig.colorbar(im,ax=axes,label='Useful GB/s in [T/4, 3T/4)')
    fig.suptitle('Conditional legacy model: 8 active grains, 1024 records; no refresh')
    fig.savefig(out/'interaction-bandwidth.svg');fig.savefig(out/'interaction-bandwidth.png',dpi=160)
    plt.close(fig)
    return 'generated from summary values; matplotlib '+matplotlib.__version__


def report(out,runs,pairs):
    lookup={r['name']:r for r in runs}
    lines=['# P2：有界交互与共享范围诊断','',
           f'本轮完成 {len(runs)} 个确定性运行，每个配置一次。时序契约为 `legacy_0270afa`；不改内部/外部延迟，不加入 A。所有运行先通过测试，再通过独立命令检查器。',
           '', '中段改为预先声明的完成时间中间一半 `[T/4,3T/4)`；同时间返回一起计入或排除。旧的返回事件序号指标单列，供外部审计复验，不能混称为稳态。',
           '', '## 1. 32 点交互', '',
           '| 字节 | ACT ns | 槽位 | hold ns | 行通路 | 完成 ns | 时间中段 GB/s | 旧事件中段 GB/s |',
           '|---:|---:|---:|---:|---:|---:|---:|---:|']
    for r in runs:
        if r['kind']=='factorial':
            c=r['config'];lines.append(f"| {r['size']} | {c['act_command_ns']} | {c['return_slots_per_grain']} | {c['trtp_ns']} | {c['row_lanes']} | {r['elapsed_ns']} | {r['middle_time_GB_s']:.3f} | {r['legacy_middle_half_GB_s']:.3f} |")
    lines += ['', *(['![Conditional bandwidth](interaction-bandwidth.svg)'] if (out/'interaction-bandwidth.svg').exists() else []), '', '## 2. 多命令组压力', '',
              '| 活动组 | 字节 | 行通路 | 完成 ns | 时间中段 GB/s | 旧事件中段 GB/s | 最大 12 ns 激活数 |',
              '|---:|---:|---:|---:|---:|---:|---:|']
    for r in runs:
        if r['kind']=='scope':
            lines.append(f"| {r['grains']//8} | {r['size']} | {r['config']['row_lanes']} | {r['elapsed_ns']} | {r['middle_time_GB_s']:.3f} | {r['legacy_middle_half_GB_s']:.3f} | {r['max_die_ACTs_in_12ns']} |")
    lines += ['', '多组统一窗口 256、原子额度 512；一组交互系列窗口 64、原子额度 256。不同系列不能无说明混比。所有 scope 配置仍为 die 范围 32 ACT/12 ns，未把组数增加当成额外激活额度。',
              '若 16 组时 C 的旧事件中段值相同，只说明当前共同额度会遮挡局部命令收益；完整完成时间和新时间窗口值分别报告。作用范围是假设，不能推出真实产品应采用同一范围。',
              '', '## 3. 长度稳定性', '',
              '| 字节 | 行通路 | 记录数 | 完成 ns | 时间中段 GB/s | 含启动/排空 GB/s |',
              '|---:|---:|---:|---:|---:|---:|']
    for q,c,n in itertools.product((32,64),(1,2),(256,1024,4096)):
        name=f'f_q{q}_a4_s4_h16_C{c}' if n==1024 else f'len_n{n}_q{q}_C{c}'
        if name in lookup:
            r=lookup[name];lines.append(f"| {q} | {c} | {n} | {r['elapsed_ns']} | {r['middle_time_GB_s']:.3f} | {r['useful_GB_s']:.3f} |")
    lines += ['', '1024 条的四点复用交互运行；其余八点另跑，未对随机种子求平均。长度检查只能说明这个无刷新、固定输入窗口的观测变化，不能证明长期稳态。',
              '', '## 4. D 正负控制', '', '| 暂停 ns | D | 完成 ns | 借用块数 |','|---:|---:|---:|---:|']
    for r in runs:
        if r['kind']=='D_control':lines.append(f"| {r['config']['initial_output_stall_ns']} | {r['config']['flexible_output']} | {r['elapsed_ns']} | {r['borrowed_atoms']} |")
    lines += ['', 'D 只保留机制控制，没有扩大应用扫描。人为暂停后的积压收益不是实际应用收益。',
              '', '## 5. 事件干预证据与复验', '',
              f'`interventions.json` 保存 {len(pairs)} 组成对干预：逻辑输入哈希、完成时间和中段变化、提前/延后命令数量、最早改变操作及原轨迹当时的候选掩码。状态历史已经变化时明确留空，不伪造一个直接等待原因。',
              '`key-events.json` 保存返回预约限制、read-hold 限制、命令与共同激活限制的短片段。`runs.json` 给出每次 ACT 的读取分布、有效/传输载荷、所有候选掩码汇总、每 grain 的源端/出口空闲总量和完整区间文件指针。',
              '全量命令、每个时刻每个候选的多原因掩码、每 grain 空闲区间和每个返回预约生命周期均在本地 `traces/` 压缩文件中；不截断，SHA-256 记录在 manifest。Git 仅保留短证据与统计，脚本可重新生成全部文件。',
              '命令/等待轨迹哈希针对解压后的确定性内容另行记录，gzip 文件本身也记录哈希。candidate-ns 不相加为 100%，不用作可加速比例。线载荷不含尚未建模的 ECC/CRC，也不包含反量化扩展。',
              '', '复验：`python3 scripts/run_stage2_interactions.py --output results/stage2_recheck`；单例可加 `--case f_q64_a4_s2_h16_C1`。配置和输入哈希随每个结果保存。',
              '', '## 6. 研究停止点', '',
              '本轮只定位旧模型中的条件瓶颈及其相互遮挡，没有建立真实 FGDRAM 的完整时序/电流契约，也没有面积、能量或新 3D 组织证据。接真实索引之前仍需地址拆分、缓存/合并、依赖和硬件时序校准。P0/P1/P2 的交付完成不等于可直接提出新阵列架构。','']
    (out/'REPORT.md').write_text('\n'.join(lines))


def main():
    ap=argparse.ArgumentParser();ap.add_argument('--output',default='results/stage2');ap.add_argument('--case')
    args=ap.parse_args();out=(ROOT/args.output).resolve()
    if not out.is_relative_to(ROOT/'results') or out.name in ('first_round','p0_audit','p1_contract'):
        raise SystemExit('choose a new results directory')
    specs=cases()
    if args.case:
        specs=[s for s in specs if s['name']==args.case]
        if not specs:raise SystemExit('unknown case')
    out.mkdir(parents=True,exist_ok=True);(out/'traces').mkdir(exist_ok=True)
    tests=subprocess.run([sys.executable,'-m','unittest','discover','-s','tests','-v'],cwd=ROOT,capture_output=True,text=True)
    (out/'validation.txt').write_text(tests.stdout+tests.stderr)
    if tests.returncode:raise SystemExit('tests failed')
    artifacts={};runs=[];key_events={}
    for spec in specs:
        c=spec['config'];name=spec['name'];reqs=balanced(spec['size'],spec['records'],grains=spec['grains'])
        candidate_path=out/'traces'/f'{name}-candidates.jsonl.gz'
        recorder=CandidateRecorder(candidate_path,c.clock_ns)
        sim=Simulator(c,reqs,candidate_observer=recorder)
        try:result=sim.run()
        finally:observations=recorder.close()
        m=metrics(sim.trace,c,reqs)
        intervals=out/'traces'/f'{name}-resources.json.gz'
        with gzip.open(intervals,'wt') as f:json.dump(dict(per_grain_idle=m.pop('per_grain_idle'),return_reservations=m.pop('return_reservations')),f)
        with gzip.open(intervals,'rt') as f:resource=json.load(f)
        idle_summary={g:{k:sum(b-a for a,b in spans) for k,spans in categories.items()}
                      for g,categories in resource['per_grain_idle'].items()}
        trace_path=out/'traces'/f'{name}.jsonl.gz'
        with gzip.open(trace_path,'wt') as f:
            for e in sim.trace:f.write(json.dumps(e,sort_keys=True,separators=(',',':'))+'\n')
        for path in (candidate_path,intervals,trace_path):
            with gzip.open(path,'rb') as f:
                h=hashlib.sha256()
                for block in iter(lambda:f.read(1024*1024),b''):h.update(block)
            artifacts[str(path.relative_to(out))]=dict(file_sha256=digest(path),content_sha256=h.hexdigest())
        acts=[e['time_ns'] for e in sim.trace if e['event']=='ACT_ACCEPT'];window=deque();peak=0
        for t in acts:
            while window and window[0]<=t-12:window.popleft()
            window.append(t);peak=max(peak,len(window))
        row={**{k:v for k,v in spec.items() if k!='config'},'config':asdict(c),**m,
             'logical_input_sha256':json_hash([asdict(r) for r in reqs]),
             'useful_GB_s':result['useful_GB_s'],'legacy_middle_half_GB_s':result['middle_half_GB_s'],
             'lookups_per_s':result['lookups_per_s'],'max_live_records':result['max_live_records'],
             'max_outstanding_atoms':result['max_outstanding_atoms'],'stats':result['stats'],
             'candidate_evidence':observations,'per_grain_idle_ns':idle_summary,
             'resource_intervals_file':str(intervals.relative_to(out)),
             'borrowed_atoms':sum(e['port']!=e['grain'] for e in sim.trace if e['event']=='DATA_START'),
             'max_die_ACTs_in_12ns':peak}
        runs.append(row)
        if name in ('f_q32_a4_s4_h16_C1','f_q32_a4_s4_h16_C2','f_q64_a4_s4_h16_C1',
                     'f_q64_a4_s2_h16_C1','f_q64_a4_s4_h24_C1','scope_g16_q32_C1','scope_g16_q32_C2'):
            example=observations['examples'].get('activation_window');center=example['time_ns'] if example else 0
            key_events[name]=dict(grain0_first_200ns=[e for e in sim.trace if e['time_ns']<=200 and e.get('grain')==0],
                                 selected_waits=observations['examples'],
                                 shared_ACT_near_first_window_wait=[e for e in sim.trace if e['event']=='ACT_ACCEPT' and center-12<e['time_ns']<=center+12] if example else [])
        print(f"{len(runs)}/{len(specs)} {name}: {row['elapsed_ns']} ns; time-window {row['middle_time_GB_s']:.6f} GB/s; audit PASS",flush=True)
    pairs=make_pairs(runs,out)
    save(out/'runs.json',runs);save(out/'interventions.json',pairs);save(out/'key-events.json',key_events)
    fields=['name','kind','size','records','grains','act_command_ns','return_slots_per_grain','trtp_ns','row_lanes',
            'elapsed_ns','middle_time_GB_s','legacy_middle_half_GB_s','useful_GB_s','lookups_per_s',
            'ACTs_per_lookup','useful_bytes','wire_payload_bytes','max_live_records','max_outstanding_atoms']
    with (out/'summary.csv').open('w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=fields,extrasaction='ignore',lineterminator='\n');w.writeheader()
        for r in runs:w.writerow({**r,**r['config']})
    plot=make_figure(out,runs);report(out,runs,pairs)
    code={str(p.relative_to(ROOT)):digest(p) for folder in ('fgdram','scripts','tests') for p in sorted((ROOT/folder).glob('*.py'))}
    save(out/'manifest.json',dict(command=sys.argv,python=platform.python_version(),timing_contract=LEGACY_CONTRACT,
        parent_commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
        code_sha256=code,source=json.loads((ROOT/'sources/manifest.json').read_text()),artifacts=artifacts,
        summary_sha256=digest(out/'summary.csv'),run_counts=dict(Counter(r['kind'] for r in runs)),
        length_1024_reused=[f'f_q{q}_a4_s4_h16_C{c}' for q,c in itertools.product((32,64),(1,2))],
        validation_exit_code=tests.returncode,plot=plot,randomness='none; one execution per fixed configuration',
        time_window='[elapsed/4,3*elapsed/4); rational comparison 1*T <= 4*t < 3*T; same-time ties indivisible',
        claims='conditional legacy-model diagnostics only; no physical validation or application/3D speedup'))


if __name__=='__main__':main()
