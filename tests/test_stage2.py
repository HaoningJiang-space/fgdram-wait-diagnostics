import gzip
import json
from pathlib import Path
import tempfile
import unittest
from dataclasses import replace

from fgdram.evidence import CandidateRecorder, json_hash, metrics
from fgdram.model import Config, Simulator
from fgdram.workloads import balanced
from scripts.run_stage2_interactions import cases, intervention, make_pairs


class Stage2Tests(unittest.TestCase):
    def test_declared_scope_and_reused_length_points(self):
        specs=cases()
        self.assertEqual(len({s['name'] for s in specs}),56)
        self.assertEqual(sum(s['kind']=='factorial' for s in specs),32)
        self.assertEqual(sum(s['kind']=='scope' for s in specs),12)
        self.assertEqual(sum(s['kind']=='length' for s in specs),8)
        self.assertEqual(sum(s['kind']=='D_control' for s in specs),4)
        for s in specs:
            self.assertEqual(s['config'].power_scope,'die')
            self.assertEqual(s['config'].faw_limit,32)
            if s['kind']=='scope':
                self.assertEqual((s['config'].outstanding_records,s['config'].atom_queue_capacity),(256,512))

    def test_intervention_is_joined_by_operation_not_command_id(self):
        # A changed schedule renumbers commands. Compare logical operations and
        # retain the original candidate mask instead of treating IDs as stable.
        from dataclasses import asdict
        with tempfile.TemporaryDirectory() as temp:
            out=Path(temp);(out/'traces').mkdir()
            rows=[]
            req=balanced(32,32)
            for lanes in (1,2):
                name=f'case{lanes}';c=replace(Config(),row_lanes=lanes)
                rec=CandidateRecorder(out/'traces'/f'{name}-candidates.jsonl.gz',2)
                s=Simulator(c,req,candidate_observer=rec);s.run();rec.close()
                with gzip.open(out/'traces'/f'{name}.jsonl.gz','wt') as f:
                    for e in s.trace:f.write(json.dumps(e)+'\n')
                m=metrics(s.trace,c,req)
                rows.append(dict(name=name,kind='factorial',size=32,config=asdict(c),**m,
                                 logical_input_sha256=json_hash([asdict(r) for r in req])))
            p=intervention(*rows,out,'row_lanes')
            self.assertTrue(p['logical_input_equal'])
            self.assertGreater(p['command_accept_comparison']['earlier'],0)
            self.assertTrue(p['first_advanced_operations'])
            self.assertLess(p['elapsed_delta_ns'],0)
            self.assertEqual(len(make_pairs(rows,out)),1)

    def test_all_physical_outputs_accounted_for_in_d_control(self):
        c=replace(Config(),initial_output_stall_ns=160,flexible_output=True)
        req=balanced(64,8,grains=1);s=Simulator(c,req);s.run()
        m=metrics(s.trace,c,req)
        self.assertEqual(len(m['per_grain_idle']),128)
        self.assertLess(sum(b-a for a,b in m['per_grain_idle']['1']['output']),m['elapsed_ns'])
        self.assertEqual(m['per_grain_idle']['127']['source'],[[0,m['elapsed_ns']]])


if __name__=='__main__':unittest.main()
