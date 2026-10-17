#!/usr/bin/env python3
"""Verify P3 saved evidence, without rerunning the simulation or using its state."""
import argparse
from collections import Counter
from dataclasses import asdict, replace
import gzip
import hashlib
import json
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from fgdram.evidence import json_hash, idle_intervals, credit_intervals
from fgdram.frontend import AddressMap, Lookup, Producer
from fgdram.frontend_audit import check_gather
from fgdram.model import Config, Request
from fgdram.index_input import DATA_SHA256, inputs as source_inputs


def require(condition,message):
    if not condition:
        raise RuntimeError(message)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--output',default='results/stage3')
    args=parser.parse_args()
    out=(ROOT/args.output).resolve()
    manifest=json.loads((out/'manifest.json').read_text())
    for path,expected in manifest['code_sha256'].items():
        require(sha((ROOT/path).read_bytes())==expected,f'code changed: {path}')
    require(sha((ROOT/'data/.local/ml-100k-u.data').read_bytes())==DATA_SHA256,'source mismatch')
    for name,expected in manifest['result_sha256'].items():
        require(sha((out/name).read_bytes())==expected,f'public artifact changed: {name}')
    runs=json.loads((out/'runs.json').read_text())
    expected_inputs=source_inputs(ROOT/'data/.local/ml-100k-u.data')
    receipts={};count=0;diagnostics={}
    for name,run in runs.items():
        files={}
        for artifact in run['artifacts']:
            path=ROOT/artifact['path'];compressed=path.read_bytes()
            require(sha(compressed)==artifact['sha256'],f'compressed hash: {path}')
            raw=gzip.decompress(compressed)
            require(sha(raw)==artifact['uncompressed_sha256'],f'raw hash: {path}')
            files[path.name]=raw;count+=1
        timeline=json.loads(files[name+'.timeline.json.gz'])
        inputs=json.loads(files[name+'.inputs.json.gz'])
        logical=[Lookup(**(r|{'depends':tuple(r['depends'])})) for r in inputs['logical']]
        physical=[Request(**(r|{'depends':tuple(r['depends'])})) for r in inputs['physical']]
        expected_logical=expected_inputs[run['summary']['source']]
        if run['summary']['scenario']=='cache_fit':
            expected_logical=expected_logical+[replace(r,lid=r.lid+len(expected_logical))
                                              for r in expected_logical]
        require(logical==expected_logical,'input does not match declared source/extraction')
        require(json_hash(inputs['logical'])==run['logical_input_sha256'],'logical input hash')
        require(json_hash(inputs['physical'])==run['physical_input_sha256'],'physical input hash')
        require(json_hash(dict(config=run['config'],producer=run['producer'],mapping=run['mapping'],
                               groups=run['summary']['groups']))==run['config_sha256'],'config hash')
        c=Config(**run['config']);p=Producer(**run['producer']);m=AddressMap(**run['mapping'])
        receipt=check_gather(timeline,c,physical,logical,m,p,run['summary']['groups'])
        require(receipt==run['audit'],f'audit replay mismatch: {name}')
        row=run['summary']
        complete={e['lid']:e['time_ns'] for e in timeline if e['event']=='L_COMPLETE'}
        require(max(complete.values())==row['elapsed_ns'] and len(complete)==row['logical_lookups'],
                'logical completions')
        acts={e['activation_id']:0 for e in timeline if e['event']=='ACT_ACCEPT'}
        for e in timeline:
            if e['event']=='READ_ACCEPT':
                acts[e['activation_id']]+=1
        hist={str(k):v for k,v in Counter(acts.values()).items()}
        require(hist==run['reads_per_ACT_histogram'],'READ/ACT distribution')
        require(row['ACTs']==len(acts) and row['READs']==len(physical),'ACT/READ totals')
        require(row['ACTs_per_logical_lookup']==len(acts)/len(logical),'ACT/lookup denominator')
        for field in ('useful_dram_bytes','logical_requested_bytes','wire_payload_bytes'):
            require(receipt[field]==row[field],field)
        loads=Counter(r.grain//8 for r in physical)
        require(run['per_group_READs']=={str(g):loads[g] for g in range(16)},'group loads')
        masks,reasons=Counter(),Counter();observations=0
        commands={(e['time_ns'],e['command'],e['grain'],e['pb'],e['rid'])
                  for e in timeline if e['event']=='COMMAND'}
        witnessed=set()
        for line in files[name+'.candidates.jsonl.gz'].splitlines():
            e=json.loads(line);observations+=1
            masks['+'.join(e['blockers']) or 'eligible_and_issued']+=c.clock_ns
            for reason in e['blockers']:
                reasons[reason]+=c.clock_ns
            if e['issued']:
                key=(e['time_ns'],*e['candidate'])
                require(key in commands,'candidate issue without command')
                witnessed.add(key)
        require(commands==witnessed,'command without candidate witness')
        require(observations==run['candidates']['candidate_observations'] and
                dict(masks)==run['candidates']['all_candidate_masks_ns'] and
                dict(reasons)==run['candidates']['wait_candidate_ns'],'candidate aggregation')
        if run['cache_control']:
            start=run['cache_control']['second_pass_start_ns']
            tail=[e for e in timeline if e['time_ns']>=start]
            require(not any(e['event']=='READ_ACCEPT' for e in tail),'cache-fit second pass READ')
            require(sum(e['event']=='L_COMPLETE' for e in tail)==1024,'cache-fit lookup count')
        horizon=row['elapsed_ns']
        source_spans={g:[] for g in range(c.grains)}
        output_spans={g:[] for g in range(c.grains)}
        row_ns=Counter();col_ns=Counter()
        for e in timeline:
            if e['event']=='READ_ACCEPT':
                source_spans[e['grain']].append((e['time_ns'],e['time_ns']+c.source_ns))
            elif e['event']=='DATA_START':
                output_spans[e['port']].append((e['time_ns'],e['end_ns']))
            elif e['event']=='COMMAND':
                (col_ns if e['command']=='RD' else row_ns)[e['group']]+=e['duration_ns']
        # The legacy metrics helper ends at the final physical response. This
        # derivative explicitly includes the complete logical horizon, including
        # a cache-only tail, rather than silently omitting its idle intervals.
        per_grain={str(g):dict(source_idle_ns=sum(b-a for a,b in idle_intervals(source_spans[g],horizon)),
                              output_idle_ns=sum(b-a for a,b in idle_intervals(output_spans[g],horizon)))
                   for g in range(c.grains)}
        diagnostics[name]=dict(logical_horizon_ns=horizon,per_grain=per_grain,
            per_group_command_utilization={str(g):dict(row=row_ns[g]/(horizon*c.row_lanes),
                                                       column=col_ns[g]/horizon) for g in range(16)},
            observed_shared_control_candidate_ns=reasons['shared_row_control'],
            observed_sense_conflict_candidate_ns=reasons['shared_sense_amplifier'],
            note='Observed masks can overlap and are not exclusive delay fractions. '
                 'Source/output idle complements cover the entire logical interval.')
        resources=json.loads(files[name+'.resources.json.gz'])
        physical_trace=[e for e in timeline if not e['event'].startswith('L_')]
        require(resources['return_reservations']==credit_intervals(physical_trace),'credit interval evidence')
        physical_end=max(e['time_ns'] for e in physical_trace if e['event']=='COMPLETE')
        expected_idle={str(g):dict(source=idle_intervals(source_spans[g],physical_end),
                                  output=idle_intervals(output_spans[g],physical_end))
                       for g in range(c.grains)}
        require(resources['per_grain_idle']==expected_idle,'legacy physical-horizon idle evidence')
        receipts[name]=dict(pass_=True,logical_lookups=len(logical),physical_reads=len(physical))
        print(name,'PASS',flush=True)
    result=dict(pass_=True,runs=len(receipts),artifacts=count,receipts=receipts,
                verifier_sha256=sha(Path(__file__).read_bytes()),
                scope='Saved source/config/input/hash, logical and physical independent replay; '
                      'candidate-command correspondence and reported aggregate recomputation.')
    (out/'diagnostics.json').write_text(json.dumps(diagnostics,indent=2,sort_keys=True)+'\n')
    result['diagnostics_sha256']=sha((out/'diagnostics.json').read_bytes())
    (out/'verification.json').write_text(json.dumps(result,indent=2,sort_keys=True)+'\n')


if __name__=='__main__':
    main()
