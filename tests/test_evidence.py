import copy
import gzip
import json
from dataclasses import replace
from pathlib import Path
import tempfile
import unittest

from fgdram.audit import AuditError, check_trace
from fgdram.evidence import annotate_atoms, check_annotations, metrics, idle_intervals, CandidateRecorder
from fgdram.model import Config, Simulator, Request
from fgdram.scheduling import PeriodicWitness, finite_lower_bound, periodic_boundary_certificate
from fgdram.workloads import balanced, stream


class EvidenceTests(unittest.TestCase):
    def test_byte_and_credit_witness_for_row_hit_and_two_read_rows(self):
        for reqs in (stream(8), balanced(32, 8), balanced(64, 8)):
            sim=Simulator(Config(),reqs); sim.run()
            a=annotate_atoms(sim.trace,Config(),reqs)
            self.assertEqual(len(a),sum(r.size//32 for r in reqs))
            self.assertTrue(all(x['physical_internal_first_arrival_ns'] is None for x in a))
            bad=copy.deepcopy(a); bad[0]['pre_start_ns']=bad[0]['last_column_dependency_end_ns']-1
            with self.assertRaisesRegex(AuditError,'early close'):
                check_annotations(bad,Config())
            bad=copy.deepcopy(a); bad[0]['credit_release_ns']-=2
            with self.assertRaisesRegex(AuditError,'credit lifetime'):
                check_annotations(bad,Config())

    def test_time_window_ties_and_idle_intervals(self):
        # Synthetic complement checks exercise clipping, overlap and equal endpoints.
        self.assertEqual(idle_intervals([[2,4],[4,6],[3,5],[8,12]],10),[[0,2],[6,8]])
        reqs=balanced(64,32)
        sim=Simulator(Config(),reqs);sim.run()
        m=metrics(sim.trace,Config(),reqs)
        t=m['elapsed_ns']
        times=[e['time_ns'] for e in sim.trace if e['event']=='DATA_END']
        counts={x:times.count(x) for x in set(times)}
        self.assertEqual(m['time_window']['returns'],sum(n for x,n in counts.items() if t/4<=x<3*t/4))
        self.assertEqual(m['useful_bytes'],m['wire_payload_bytes'])

    def test_full_candidate_observer_changes_no_behavior(self):
        reqs=balanced(64,32)
        a=Simulator(Config(),reqs); ra=a.run()
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp)/'candidates.jsonl.gz'
            recorder=CandidateRecorder(path,2)
            b=Simulator(Config(),reqs,candidate_observer=recorder);rb=b.run()
            receipt=recorder.close()
            with gzip.open(path,'rt') as f: rows=[json.loads(s) for s in f]
            self.assertEqual(len(rows),receipt['candidate_observations'])
            self.assertTrue(any(x['blockers'] for x in rows))
            self.assertTrue(any(x['issued'] for x in rows))
        self.assertEqual(ra,rb);self.assertEqual(a.trace,b.trace)

    def test_periodic_witness_and_illegal_one_lane_rejected(self):
        reqs=balanced(64,64)
        config=replace(Config(),row_lanes=2,max_ns=1000)
        s=PeriodicWitness(config,reqs,phases={g:2*g for g in range(8)})
        result=s.run();check_trace(s.trace,config,reqs)
        cert=periodic_boundary_certificate(s.trace)
        self.assertTrue(cert['event_streams_equal'])
        self.assertTrue(cert['carried_resource_state_equal'])
        tail=periodic_boundary_certificate(s.trace,start=144)
        self.assertFalse(tail['event_streams_equal'])  # finite input drains inside second period
        busy=periodic_boundary_certificate(s.trace,start=80)
        self.assertTrue(busy['event_streams_equal'] and busy['carried_resource_state_equal'])
        self.assertTrue(busy['first_state']['activation_window_history'])
        self.assertLessEqual(finite_lower_bound(config,reqs),result['elapsed_ns'])
        with self.assertRaisesRegex(AuditError,'command lane|double booking'):
            check_trace(s.trace,replace(config,row_lanes=1),reqs)
        bad=copy.deepcopy(s.trace)
        next(e for e in bad if e['event']=='DATA_START' and 128<=e['time_ns']<192)['end_ns']+=2
        cert=periodic_boundary_certificate(bad)
        self.assertFalse(cert['event_streams_equal'] and cert['carried_resource_state_equal'])


if __name__=='__main__':
    unittest.main()
