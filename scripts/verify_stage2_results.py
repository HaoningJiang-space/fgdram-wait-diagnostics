#!/usr/bin/env python3
"""Verify P2 coverage, frozen baselines, ties, artifact hashes and audit controls.

Does not rerun experiments or replace the per-run independent command audit.
"""
import argparse
from dataclasses import asdict
import csv
import gzip
import hashlib
import json
from pathlib import Path
import subprocess
import sys
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from fgdram.evidence import json_hash
from fgdram.workloads import balanced
from scripts.run_stage2_interactions import cases


def check(ok,message):
    if not ok:raise RuntimeError(message)


def digest(path):return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    ap=argparse.ArgumentParser();ap.add_argument('--output',default='results/stage2');args=ap.parse_args()
    out=ROOT/args.output
    runs=json.loads((out/'runs.json').read_text());manifest=json.loads((out/'manifest.json').read_text())
    by_name={r['name']:r for r in runs};specs=cases()
    check(len(runs)==56 and set(by_name)=={s['name'] for s in specs},'complete 56-case design')
    for s in specs:
        r=by_name[s['name']]
        check(r['config']==asdict(s['config']),'configuration '+s['name'])
        reqs=balanced(s['size'],s['records'],grains=s['grains'])
        check(r['logical_input_sha256']==json_hash([asdict(x) for x in reqs]),'input hash')
        check(r['independent_trace_audit']['pass'],'trace audit')
        check(r['useful_bytes']==r['wire_payload_bytes']==s['size']*s['records'],'payload')
        check(r['ACTs_per_lookup']==1,'balanced input ACT count')
        check(r['max_die_ACTs_in_12ns']<=32,'shared die activation budget')
        with gzip.open(out/'traces'/f"{s['name']}.jsonl.gz",'rt') as f:
            returned=[e for line in f if (e:=json.loads(line))['event']=='DATA_END']
        # Whole timestamp groups, not event-index slicing.
        counts={}
        for e in returned:counts[e['time_ns']]=counts.get(e['time_ns'],0)+1
        t=r['elapsed_ns'];n=sum(count for time,count in counts.items() if t<=4*time<3*t)
        check(n==r['time_window']['returns'] and n*64/t==r['middle_time_GB_s'],'time-window ties')
        with gzip.open(out/'traces'/f"{s['name']}-resources.json.gz",'rt') as f:resources=json.load(f)
        check(len(resources['return_reservations'])==len(returned),'reservation coverage')
        check(len(resources['per_grain_idle'])==128,'all physical grains accounted for')
        for g,values in resources['per_grain_idle'].items():
            for kind,spans in values.items():
                check(sum(b-a for a,b in spans)==r['per_grain_idle_ns'][g][kind],'idle summary')
        check(sum(r['candidate_evidence']['all_candidate_masks_ns'].values())==
              2*r['candidate_evidence']['candidate_observations'],'all candidate coverage')
    expected={(1,32):(8,10.667),(4,32):(32,42.667),(16,32):(85.333,85.333),
              (1,64):(15.604,16),(4,64):(62.534,64),(16,64):(170.667,170.667)}
    scope=[]
    for (groups,q),values in expected.items():
        for c,v in enumerate(values,1):
            r=by_name[f'scope_g{groups}_q{q}_C{c}']
            check(abs(r['legacy_middle_half_GB_s']-v)<0.00051,'external scope reference')
            scope.append(dict(name=r['name'],reference_GB_s=v,observed_GB_s=r['legacy_middle_half_GB_s']))
    for q,times in ((32,(428,426)),(64,(462,454))):
        for c,t in enumerate(times,1):check(by_name[f'scope_g16_q{q}_C{c}']['elapsed_ns']==t,'scope finite completion')
    old={r['name']:r for r in json.loads((ROOT/'results/first_round/runs.json').read_text())}
    baseline_checks=[]
    for q in (32,64):
        for c,name in ((1,'base'),(2,'C')):
            r=by_name[f'f_q{q}_a4_s4_h16_C{c}'];reference=old[f'q{q}_{name}']
            check(r['elapsed_ns']==reference['elapsed_ns'] and
                  r['legacy_middle_half_GB_s']==reference['middle_half_GB_s'],'unchanged first-round point')
            baseline_checks.append(r['name'])
    left=by_name['f_q64_a4_s2_h16_C1'];right=by_name['f_q64_a4_s4_h24_C1']
    check(left['elapsed_ns']==right['elapsed_ns']==4670,'equal-throughput distinct-cause reference')
    check(left['candidate_evidence']['wait_candidate_ns'].get('return_buffer',0)>0,'return credit wait evidence')
    events=json.loads((out/'key-events.json').read_text())
    for name,start,end in [('f_q64_a4_s2_h16_C1',52,68),('f_q64_a4_s4_h24_C1',60,76)]:
        seq=events[name]['grain0_first_200ns']
        first_start=next(e['time_ns'] for e in seq if e['event']=='PRE_START')
        first_end=next(e['time_ns'] for e in seq if e['event']=='PRE_END')
        check((first_start,first_end)==(start,end),'row hold PRE event evidence')
    for flex,t,borrowed in ((0,416,0),(1,386,2)):
        r=by_name[f'd_stall160_flex{flex}']
        check((r['elapsed_ns'],r['borrowed_atoms'])==(t,borrowed),'D positive control')
    check(by_name['d_stall0_flex0']['elapsed_ns']==by_name['d_stall0_flex1']['elapsed_ns'],'D negative control')
    pairs=json.loads((out/'interventions.json').read_text())
    check(len(pairs)==70 and all(p['logical_input_equal'] for p in pairs),'paired intervention coverage')
    for p in pairs:
        check(p['elapsed_delta_ns']==by_name[p['intervention']]['elapsed_ns']-by_name[p['baseline']]['elapsed_ns'],
              'paired completion delta')
    check(digest(out/'summary.csv')==manifest['summary_sha256'],'summary integrity')
    with (out/'summary.csv').open() as f:check(len(list(csv.DictReader(f)))==56,'CSV rows')
    for path,hashes in manifest['artifacts'].items():
        check(digest(out/path)==hashes['file_sha256'],'artifact '+path)
        with gzip.open(out/path,'rb') as f:
            h=hashlib.sha256()
            for block in iter(lambda:f.read(1024*1024),b''):h.update(block)
        check(h.hexdigest()==hashes['content_sha256'],'uncompressed artifact '+path)
    for path,h in manifest['code_sha256'].items():
        if path=='scripts/verify_stage2_results.py':
            # This result verifier is not imported by simulation or unit tests.
            # Its post-run revision and both hashes are explicit in the manifest.
            h=manifest.get('post_run_verifier',{}).get('current_sha256',h)
        check(digest(ROOT/path)==h,'code snapshot '+path)
    frozen=subprocess.check_output(['git','diff','a496398','--','results/first_round','results/p0_audit'],cwd=ROOT)
    check(not frozen,'historical result protection')
    receipt=dict(pass_=True,cases=56,paired_interventions=70,scope_reference=scope,
                 first_round_points_identical=baseline_checks,artifact_files_checked=len(manifest['artifacts']),
                 candidate_and_resource_coverage='all candidates, all 128 grains, all returned atom reservations',
                 scope_reference_provenance='rounded values and finite times from user-provided 0270afa audit',
                 historical_results_unchanged=True,summary_sha256=digest(out/'summary.csv'),
                 verifier_sha256=digest(Path(__file__)))
    (out/'verification.json').write_text(json.dumps(receipt,indent=2)+'\n')
    print('PASS: 56 cases, 70 pairs, 12 scope reference points, all artifact hashes and legacy boundaries')


if __name__=='__main__':main()
