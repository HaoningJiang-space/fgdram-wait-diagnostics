#!/usr/bin/env python3
"""Bounded P3 matrix: 36 main runs and two cache-fit controls, no timing sweep."""
import argparse
from collections import Counter
import csv
from dataclasses import asdict, replace
import gzip
import hashlib
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from fgdram.evidence import CandidateRecorder, json_hash, metrics
from fgdram.frontend import AddressMap, Gather, Lookup, Producer
from fgdram.frontend_audit import check_gather
from fgdram.index_input import inputs, provenance
from fgdram.model import Config


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def write_json(path, data):
    Path(path).write_text(json.dumps(data,indent=2,sort_keys=True)+'\n')


def gz_json(path, data):
    raw = (json.dumps(data,sort_keys=True,separators=(',',':'))+'\n').encode()
    Path(path).write_bytes(gzip.compress(raw,mtime=0))
    return dict(path=str(Path(path).relative_to(ROOT)), sha256=sha(path),
                uncompressed_sha256=hashlib.sha256(raw).hexdigest())


def cases():
    result=[]
    for source in ('movielens','uniform'):
        for mapping in ('linear','xor'):
            for scenario,groups in (('single_bounded',1),('die_bounded',16),('die_supply',16)):
                for hardware in ('base','C','credit3'):
                    result.append(dict(source=source,mapping=mapping,scenario=scenario,
                                       groups=groups,hardware=hardware))
        result.append(dict(source=source,mapping='linear',scenario='cache_fit',groups=1,hardware='base'))
    return result


def settings(case):
    c=replace(Config(),outstanding_records=512,atom_queue_capacity=512,
              row_lanes=2 if case['hardware']=='C' else 1,
              return_slots_per_grain=3 if case['hardware']=='credit3' else 4)
    p=Producer(mshr_entries=512)
    if case['scenario']=='die_supply':
        p=replace(p,window=256,records_per_tick=16,accesses_per_tick=64,
                  batch_size=1024,batch_gap_ns=0)
    if case['scenario']=='cache_fit':
        p=replace(p,cache_blocks=8192,batch_size=1024)
    return c,p


def logical_example(gather, lid):
    """Share timing/relative coverage, not reconstructible dataset indices."""
    r=next(x for x in gather.lookups if x.lid==lid)
    events=[e for e in gather.timeline if e['event'].startswith('L_') and e.get('lid')==lid]
    accesses=[e for e in events if e['event']=='L_ACCESS']
    block_labels={e['block']:i for i,e in enumerate(accesses)}
    rid_labels={e['rid']:i for i,e in enumerate(accesses)}
    safe=[]
    for e in events:
        row=dict(event=e['event'],time_ns=e['time_ns'])
        if 'block' in e:
            row.update(fragment=block_labels[e['block']],lo=e['lo'],hi=e['hi'],
                       physical_reference=rid_labels[e['rid']])
        if 'route' in e:
            row['route']=e['route']
        safe.append(row)
    physical=[]
    for e in gather.memory.trace:
        if e.get('rid') in rid_labels and e['event'] in (
                'COMMAND','ACT_ACCEPT','READ_ACCEPT','DATA_READY','DATA_START','DATA_END','COMPLETE'):
            physical.append({k:v for k,v in e.items() if k in (
                'time_ns','event','command','accept_ns','duration_ns','end_ns','ready_ns')}
                | dict(physical_reference=rid_labels[e['rid']]))
    return dict(lid=lid,size=r.size,start_offset_in_atom=r.address%32,
                cross_row=(r.address//256!=(r.address+r.size-1)//256),
                logical_events=safe,physical_events=physical,
                note='Absolute addresses and movie IDs omitted; full local timeline has exact provenance.')


def run_case(case, records, out):
    name='_'.join(case[k] for k in ('source','mapping','scenario','hardware'))
    c,p=settings(case)
    if case['scenario']=='cache_fit':
        records=records+[replace(r,lid=r.lid+len(records)) for r in records]
    recorder=CandidateRecorder(out/'traces'/f'{name}.candidates.jsonl.gz',c.clock_ns)
    group_observed=Counter();group_pending=Counter();ticks=0
    gather=Gather(c,records,AddressMap(case['mapping']),p,case['groups'])

    def observe(t,snapshot,issued):
        nonlocal ticks
        recorder(t,snapshot,issued)
        counts=Counter(x.request.grain//8 for x in gather.memory.queued())
        ticks+=1
        for group,count in counts.items():
            group_observed[group]+=1
            group_pending[group]+=count

    gather.memory.candidate_observer=observe
    raw=gather.run()
    candidates=recorder.close()
    receipt=check_gather(gather.timeline,c,gather.memory.requests,records,gather.mapping,p,case['groups'])
    physical=metrics(gather.memory.trace,c,gather.memory.requests)
    act=receipt['physical']['activations']; reads=len(gather.memory.requests)
    load=Counter(r.grain//8 for r in gather.memory.requests)
    act_load=Counter(e['grain']//8 for e in gather.memory.trace if e['event']=='ACT_ACCEPT')
    completion=max(gather.done.values())
    ends=[e for e in gather.memory.trace if e['event']=='DATA_END']
    middle=[e for e in ends if completion<=4*e['time_ns']<3*completion]
    row=dict(name=name,**case,logical_lookups=len(records),elapsed_ns=completion,
             lookups_per_s=len(records)*1e9/completion,ACTs=act,READs=reads,
             ACTs_per_logical_lookup=act/len(records),READs_per_ACT=reads/act,
             single_READ_ACT_fraction=int(physical['reads_per_ACT_histogram'].get(1,0))/act,
             logical_requested_bytes=receipt['logical_requested_bytes'],
             useful_dram_bytes=receipt['useful_dram_bytes'],wire_payload_bytes=receipt['wire_payload_bytes'],
             useful_dram_bytes_per_ACT=receipt['useful_dram_bytes']/act,
             middle_time_wire_GB_s=len(middle)*32/(completion/2),
             hit_fragments=receipt['routes'].get('hit',0),merge_fragments=receipt['routes'].get('merge',0),
             active_groups=len(load),peak_logical_live=receipt['reconstructed_peaks']['logical_live'],
             peak_mshrs=receipt['reconstructed_peaks']['mshrs'])
    cache_control=None
    if case['scenario']=='cache_fit':
        start=next(e['time_ns'] for e in gather.timeline if e['event']=='L_ADMIT' and e['lid']==1024)
        tail=[e for e in gather.timeline if e['time_ns']>=start]
        cache_control=dict(second_pass_start_ns=start,second_pass_elapsed_ns=completion-start,
                           second_pass_ACTs=sum(e['event']=='ACT_ACCEPT' for e in tail),
                           second_pass_READs=sum(e['event']=='READ_ACCEPT' for e in tail),
                           second_pass_lookups=1024)
    # Aggregate candidate examples omit request IDs; original full masks stay local.
    candidates.pop('examples')
    resources=dict(per_grain_idle=physical['per_grain_idle'],
                   return_reservations=physical['return_reservations'])
    artifacts=[gz_json(out/'traces'/f'{name}.timeline.json.gz',gather.timeline),
               gz_json(out/'traces'/f'{name}.inputs.json.gz',dict(logical=[asdict(r) for r in records],
                    physical=[asdict(r) for r in gather.memory.requests])),
               gz_json(out/'traces'/f'{name}.resources.json.gz',resources)]
    path=out/'traces'/f'{name}.candidates.jsonl.gz'
    artifacts.append(dict(path=str(path.relative_to(ROOT)),sha256=sha(path),
                          uncompressed_sha256=hashlib.sha256(gzip.decompress(path.read_bytes())).hexdigest()))
    first_cross=next(r.lid for r in records if r.address//256!=(r.address+r.size-1)//256)
    detail=dict(summary=row,config=asdict(c),producer=asdict(p),mapping=asdict(gather.mapping),
                logical_input_sha256=json_hash([asdict(r) for r in records]),
                physical_input_sha256=json_hash([asdict(r) for r in gather.memory.requests]),
                config_sha256=json_hash(dict(config=asdict(c),producer=asdict(p),mapping=asdict(gather.mapping),
                                               groups=case['groups'])),
                audit=receipt,reads_per_ACT_histogram=physical['reads_per_ACT_histogram'],
                per_group_READs={str(g):load[g] for g in range(16)},
                per_group_ACTs={str(g):act_load[g] for g in range(16)},
                group_supply={str(g):dict(ticks_with_queued_READs=group_observed[g],ticks=ticks,
                    mean_queued_READs=group_pending[g]/ticks) for g in range(16)},
                candidates=candidates,producer_stats=raw['stats'],cache_control=cache_control,
                actual_cross_row_example=logical_example(gather,first_cross),artifacts=artifacts)
    return name,detail,gather


def paired(base, other):
    differences=[lid for lid in base.done if base.done[lid]!=other.done[lid]]
    lid=differences[0] if differences else base.lookups[0].lid
    return dict(changed_lookups=len(differences),elapsed_base_ns=max(base.done.values()),
                elapsed_other_ns=max(other.done.values()),base=logical_example(base,lid),
                other=logical_example(other,lid),
                note='Same logical lookup; physical IDs and cache/miss streams may diverge. '
                     'Earlier completion is not automatically attributed to an exclusive wait counter.')


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--output',default='results/stage3')
    parser.add_argument('--download',action='store_true')
    args=parser.parse_args()
    out=(ROOT/args.output).resolve()
    if not out.is_relative_to(ROOT/'results') or out.exists():
        raise SystemExit('Use a new output directory under results/; never overwrite evidence.')
    data=inputs(ROOT/'data/.local/ml-100k-u.data',download=args.download)
    (out/'traces').mkdir(parents=True)
    code={str(p.relative_to(ROOT)):sha(p) for p in sorted((ROOT/'fgdram').glob('*.py'))}
    code[str(Path(__file__).relative_to(ROOT))]=sha(__file__)
    details={};comparisons={};family={}
    for case in cases():
        name,detail,gather=run_case(case,data[case['source']],out)
        details[name]=detail
        key=tuple(case[k] for k in ('source','mapping','scenario'))
        if case['hardware']=='base':
            family[key]=gather
        else:
            comparisons[name]=paired(family[key],gather)
        if case['hardware']=='credit3' or case['scenario']=='cache_fit':
            family.pop(key,None)
        write_json(out/'runs.json',details)
        print(name,detail['summary']['elapsed_ns'],'ns',f"ACT/lookup={detail['summary']['ACTs_per_logical_lookup']:.3f}",flush=True)
    with (out/'summary.csv').open('w') as f:
        writer=csv.DictWriter(f,fieldnames=next(iter(details.values()))['summary'].keys())
        writer.writeheader();writer.writerows(x['summary'] for x in details.values())
    write_json(out/'comparisons.json',comparisons)
    write_json(out/'manifest.json',dict(contract='online-gather-v1',timing='legacy_0270afa',
        git_parent=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
        source=provenance(),code_sha256=code,cases=cases(),runs=len(details),
        selection='Declared 36-case input/mapping/supply/hardware factorial, plus two cache-fit controls.',
        result_sha256={name:sha(out/name) for name in ('runs.json','summary.csv','comparisons.json')},
        limits=['No calibrated production timing or full-model speedup.',
                'die_supply is stronger finite supply, not a saturation proof.',
                'Power scope remains the same uncertain die-wide 32 ACT/12 ns contract.',
                'Raw real indices/address traces are local, not redistributed.']))


if __name__=='__main__':
    main()
