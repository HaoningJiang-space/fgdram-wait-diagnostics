import copy
import unittest
from dataclasses import replace

from fgdram.audit import check_trace
from fgdram.model import Config, Request, Simulator
from fgdram.workloads import balanced, stream


class ResourceTests(unittest.TestCase):
    def run_case(self, reqs, **changes):
        c = replace(Config(), **changes)
        s = Simulator(c, reqs)
        result = s.run()
        check_trace(s.trace, c, reqs)
        return s, result

    def test_same_row_stream_preserves_one_activation(self):
        s, r = self.run_case(stream(8))
        self.assertEqual(r["stats"]["ACT"], 1)
        self.assertEqual(r["reads_per_ACT_histogram"], {8: 1})
        starts = [e["time_ns"] for e in s.trace if e["event"] == "DATA_START"]
        self.assertEqual([b - a for a, b in zip(starts, starts[1:])], [16] * 7)

    def test_same_pseudobank_new_rows_obey_timing(self):
        reqs = [Request(i, 0, 0, 0, i, 32) for i in range(8)]
        s, r = self.run_case(reqs)
        self.assertEqual(r["stats"]["ACT"], 8)
        acts = [e["time_ns"] for e in s.trace if e["event"] == "ACT_ACCEPT"]
        self.assertTrue(all(b - a >= 45 for a, b in zip(acts, acts[1:])))

    def test_two_pseudobanks_overlap(self):
        s, _ = self.run_case(balanced(64, 8, grains=1))
        pre = [e for e in s.trace if e["event"] == "PRE_START"]
        reads = [e for e in s.trace if e["event"] == "READ_ACCEPT"]
        self.assertTrue(any(p["pb"] != r["pb"] and
                            r["time_ns"] <= p["time_ns"] < r["time_ns"] + 16
                            for p in pre for r in reads))

    def test_command_transport_can_overlap_tail_of_precharge(self):
        reqs = [Request(i, 0, 0, 0, i, 32) for i in range(4)]
        s, _ = self.run_case(reqs)
        closes = [e["time_ns"] for e in s.trace if e["event"] == "PRE_END"]
        commands = [e for e in s.trace if e["event"] == "COMMAND" and e["command"] == "ACT"]
        self.assertTrue(any(e["time_ns"] < end <= e["accept_ns"]
                            for end in closes for e in commands))
        acts = [e["time_ns"] for e in s.trace if e["event"] == "ACT_ACCEPT"]
        # Hand schedule: RCD 16 + read pumps 16 + RP 16 = 48 ns, vs tRC 45.
        self.assertEqual([b - a for a, b in zip(acts, acts[1:])], [48] * 3)

    def test_command_and_source_analytical_limits(self):
        _, base = self.run_case(balanced(32, 128))
        _, c = self.run_case(balanced(32, 128), row_lanes=2)
        self.assertAlmostEqual(base["middle_half_GB_s"], 8.0)
        self.assertAlmostEqual(c["middle_half_GB_s"], 8 * 2 * 32 / 48)

    def test_same_subarray_different_rows_conflict_across_grains(self):
        reqs = [Request(0, 0, 0, 0, 0, 64), Request(1, 1, 1, 0, 1, 32)]
        s, r = self.run_case(reqs, row_lanes=2)
        self.assertGreater(r["wait_candidate_ns"]["shared_row_control_candidate_ns"], 0)
        acts = [e for e in s.trace if e["event"] == "ACT_ACCEPT"]
        pre_end = next(e["time_ns"] for e in s.trace if e["event"] == "PRE_END")
        self.assertGreaterEqual(acts[1]["time_ns"], pre_end)

    def test_same_subarray_same_row_is_permitted(self):
        reqs = [Request(0, 0, 0, 0, 1, 64), Request(1, 1, 1, 0, 1, 64)]
        s, _ = self.run_case(reqs)
        acts = [e["time_ns"] for e in s.trace if e["event"] == "ACT_ACCEPT"]
        first_close = next(e["time_ns"] for e in s.trace if e["event"] == "PRE_START")
        self.assertLess(acts[1], first_close)

    def test_all_die_activation_window_is_retained(self):
        _, r = self.run_case(balanced(32, 256, grains=128),
                             outstanding_records=256, atom_queue_capacity=256, row_lanes=2)
        self.assertGreater(r["wait_candidate_ns"]["activation_window_candidate_ns"], 0)

    def test_output_backpressure_and_finite_return_buffers(self):
        _, r = self.run_case(balanced(64, 8, grains=1), initial_output_stall_ns=160)
        self.assertGreater(r["wait_candidate_ns"]["return_buffer_candidate_ns"], 0)
        self.assertGreater(r["wait_candidate_ns"]["data_output_path_candidate_ns"], 0)

    def test_simultaneous_row_and_column_commands(self):
        s, _ = self.run_case(balanced(64, 64))
        row = {e["time_ns"] for e in s.trace if e["event"] == "COMMAND" and e["command"] == "ACT"}
        col = {e["time_ns"] for e in s.trace if e["event"] == "COMMAND" and e["command"] == "RD"}
        self.assertTrue(row & col)

    def test_multi_cause_masks_and_no_head_only_scheduling(self):
        s, r = self.run_case(balanced(32, 64))
        self.assertTrue(any("+" in key for key in r["overlapping_wait_masks_candidate_ns"]))
        # Another grain can advance while an older record is still awaiting data.
        second_act = [e for e in s.trace if e["event"] == "ACT_ACCEPT"][1]
        first_complete = next(e for e in s.trace if e["event"] == "COMPLETE")
        self.assertLess(second_act["time_ns"], first_complete["time_ns"])

    def test_explicit_precharge_and_automatic_precharge(self):
        _, auto = self.run_case(balanced(32, 32))
        _, explicit = self.run_case(balanced(32, 32), auto_precharge=False)
        self.assertEqual(auto["stats"].get("PRE", 0), 0)
        self.assertGreater(explicit["stats"]["PRE"], 0)

    def test_d_preserves_mat_rate_and_aggregate_outputs(self):
        _, base = self.run_case(stream(32))
        _, flexible = self.run_case(stream(32), flexible_output=True)
        self.assertEqual(base["elapsed_ns"], flexible["elapsed_ns"])

    def test_dependencies_and_bounded_issue_window(self):
        _, r = self.run_case(balanced(32, 16, serial=True), outstanding_records=8)
        self.assertEqual(r["max_live_records"], 1)

    def test_invalid_cross_row_record_fails_instead_of_silent_split(self):
        with self.assertRaises(AssertionError):
            Simulator(Config(), [Request(0, 0, 0, 0, 0, 64, 224)])

    def test_independent_auditor_rejects_corrupted_trace(self):
        reqs = stream(8)
        s, _ = self.run_case(reqs)
        bad = copy.deepcopy(s.trace)
        ports = [e for e in bad if e["event"] == "DATA_START"]
        ports[1]["time_ns"] = ports[0]["time_ns"]
        ports[1]["end_ns"] = ports[0]["end_ns"]
        with self.assertRaises(AssertionError):
            check_trace(bad, Config(), reqs)


if __name__ == "__main__":
    unittest.main()
