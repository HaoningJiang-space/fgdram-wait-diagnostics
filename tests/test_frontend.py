import copy
from dataclasses import replace
import random
import unittest

from fgdram.audit import AuditError
from fgdram.frontend import AddressMap, Gather, Lookup, Producer
from fgdram.frontend_audit import check_gather
from fgdram.model import Config, Simulator
from fgdram.workloads import balanced


class FrontendTests(unittest.TestCase):
    def run_gather(self, lookups, *, config=None, **kwargs):
        s = Gather(config or Config(), lookups, **kwargs)
        result = s.run()
        receipt = self.audit(s)
        return s, result, receipt

    def audit(self, s, timeline=None, producer=None):
        return check_gather(s.timeline if timeline is None else timeline, s.memory.c,
                            s.memory.requests, s.lookups, s.mapping, producer or s.p, s.groups)

    def test_mapping_roundtrip_capacity_and_no_aliases(self):
        rng = random.Random(13)
        for variant in ("linear", "xor"):
            m = AddressMap(variant)
            addresses = [0, 31, 32, 255, 256, m.capacity-1]
            addresses += [rng.randrange(m.capacity) for _ in range(500)]
            self.assertEqual([m.encode(*m.decode(a)) for a in addresses], addresses)
            for groups in (1, 16):
                placed = [m.place(i*32, groups) for i in range(4096)]
                self.assertEqual(len(set(placed)), len(placed))
            with self.assertRaises(ValueError):
                m.decode(m.capacity)
            with self.assertRaises(ValueError):
                m.encode(128, 0, 0, 0, 0)

    def test_unaligned_cross_block_row_grain_and_pseudobank(self):
        for address, size, groups in ((31, 34, 1), (250, 40, 1),
                                       (2040, 40, 1), (32760, 40, 16)):
            with self.subTest(address=address, groups=groups):
                s, r, a = self.run_gather([Lookup(0, address, size)], groups=groups)
                self.assertEqual(a['logical_lookups'], 1)
                self.assertEqual(a['useful_dram_bytes'], size)
                self.assertEqual(r['physical_requests'], (address % 32+size+31)//32)
                self.assertEqual(sum(e['event']=='L_COMPLETE' for e in s.timeline), 1)

    def test_coalesced_overlap_counts_union_not_consumer_copies(self):
        s, r, a = self.run_gather([Lookup(0, 3, 10), Lookup(1, 8, 14), Lookup(2, 3, 10)])
        self.assertEqual(r['physical_requests'], 1)
        self.assertEqual(a['useful_dram_bytes'], 19)
        self.assertEqual(a['logical_requested_bytes'], 34)
        self.assertEqual(a['routes'], dict(miss=1, merge=2))

    def test_multiple_logical_records_share_activation(self):
        s, r, a = self.run_gather([Lookup(i, i*32, 32) for i in range(8)])
        self.assertEqual(a['physical']['activations'], 1)
        self.assertEqual(r['logical_lookups'], 8)

    def test_cache_hit_dependency_and_batch_gap(self):
        p = Producer(batch_size=1, batch_gap_ns=10)
        s, r, a = self.run_gather([Lookup(0, 0, 32), Lookup(1, 0, 32, depends=(0,))], producer=p)
        self.assertEqual(a['routes'], dict(miss=1, hit=1))
        times = {(e['event'], e.get('lid')):e['time_ns'] for e in s.timeline if e['event'].startswith('L_')}
        self.assertGreaterEqual(times['L_ADMIT', 1], times['L_COMPLETE', 0]+10)
        self.assertEqual(times['L_COMPLETE', 1], times['L_ACCESS', 1]+2)

    def test_lru_eviction_zero_cache_and_finite_windows(self):
        records = [Lookup(i, address, 32) for i,address in enumerate([0,32,0])]
        for cache in (0,1):
            s, r, a = self.run_gather(records, producer=Producer(cache_blocks=cache, window=1,
                                                               mshr_entries=1, batch_size=1))
            self.assertEqual(r['physical_requests'], 3)
            self.assertEqual(a['reconstructed_peaks']['logical_live'], 1)

    def test_large_record_progresses_with_one_physical_credit(self):
        s, r, a = self.run_gather([Lookup(0, 17, 600)],
                config=replace(Config(), outstanding_records=1, atom_queue_capacity=1),
                producer=Producer(mshr_entries=1))
        self.assertEqual(a['useful_dram_bytes'], 600)
        self.assertEqual(a['physical']['reconstructed_peaks']['outstanding_atoms'], 1)

    def test_wrong_mapping_is_rejected(self):
        s, _, _ = self.run_gather([Lookup(0, 16*256, 120)], mapping=AddressMap('xor'))
        with self.assertRaises(AuditError):
            check_gather(s.timeline,s.memory.c,s.memory.requests,s.lookups,AddressMap('linear'),s.p,1)

    def test_mutations_completion_coverage_merge_fill_and_duplicate(self):
        s, _, _ = self.run_gather([Lookup(0, 250, 40), Lookup(1, 250, 40)])
        mutations = []
        x=copy.deepcopy(s.timeline);x.insert(0,dict(time_ns=0,event='L_COMPLETE',lid=0));mutations.append(x)
        x=copy.deepcopy(s.timeline);next(e for e in x if e['event']=='L_ACCESS')['lo']+=1;mutations.append(x)
        x=copy.deepcopy(s.timeline);next(e for e in x if e.get('route')=='merge')['rid']=999;mutations.append(x)
        x=copy.deepcopy(s.timeline);x.insert(0,dict(time_ns=0,event='L_FILL',block=224,rid=0));mutations.append(x)
        x=copy.deepcopy(s.timeline);i=next(i for i,e in enumerate(x) if e['event']=='L_PART');x.insert(i,copy.deepcopy(x[i]));mutations.append(x)
        x=[e for e in copy.deepcopy(s.timeline) if not(e['event']=='L_PART' and e['lid']==0)];mutations.append(x)
        for n, x in enumerate(mutations):
            with self.subTest(mutation=n), self.assertRaises(AuditError):
                self.audit(s,x)

    def test_replay_rejects_underdeclared_window_cache_and_issue_limits(self):
        s, _, _ = self.run_gather([Lookup(i, i*32, 32) for i in range(8)])
        for p in (replace(s.p,window=1),replace(s.p,records_per_tick=1),
                  replace(s.p,accesses_per_tick=1),replace(s.p,mshr_entries=1)):
            with self.assertRaises(AuditError):
                self.audit(s,producer=p)
        s, _, _ = self.run_gather([Lookup(0,0,32),Lookup(1,0,32,depends=(0,))])
        with self.assertRaises(AuditError):
            self.audit(s,producer=replace(s.p,cache_blocks=0))

    def test_returns_recompute_producer_timing_per_machine(self):
        p=Producer(batch_size=1,window=1)
        records=[Lookup(i,i*256,32) for i in range(3)]
        a,_,_=self.run_gather(records,producer=p)
        b,_,_=self.run_gather(records,producer=p,config=replace(Config(),initial_output_stall_ns=160))
        at=[e['time_ns'] for e in a.timeline if e['event']=='L_ADMIT']
        bt=[e['time_ns'] for e in b.timeline if e['event']=='L_ADMIT']
        self.assertGreater(bt[1],at[1])

    def test_step_hook_matches_legacy_run_exactly(self):
        requests=balanced(64,32)
        a=Simulator(Config(),requests);a.run()
        b=Simulator(Config(),requests)
        while len(b.completed)<len(requests):
            b.step()
        while b.events:
            b.t=b.events[0][0];b.process_events()
        self.assertEqual(a.trace,b.trace)


if __name__ == '__main__':
    unittest.main()
