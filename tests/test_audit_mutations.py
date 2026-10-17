"""Adversarial trace inputs, independent of scheduler implementation choices."""
import copy
from dataclasses import replace
import unittest

from fgdram.audit import AuditError, check_trace
from fgdram.model import Config, Request, Simulator
from fgdram.workloads import balanced, stream


# Shared with the fixed-commit regression script, so the six reported holes have
# machine-readable old/new outcomes using identical mutations.
REPORTED_MUTATIONS = (
    "missing_act_dispatch", "wrong_act_duration", "unknown_activation",
    "early_complete", "record_window", "atom_capacity",
)


def mutate_reported(name, trace, config):
    bad = copy.deepcopy(trace)
    if name == "missing_act_dispatch":
        bad = [e for e in bad if not (e["event"] == "COMMAND" and e["command"] == "ACT")]
    elif name == "wrong_act_duration":
        e = next(e for e in bad if e["event"] == "COMMAND" and e["command"] == "ACT")
        e["duration_ns"] = 2
        e["accept_ns"] = e["time_ns"] + 2
    elif name == "unknown_activation":
        next(e for e in bad if e["event"] == "READ_ACCEPT")["activation_id"] = 99999
    elif name == "early_complete":
        e = [e for e in bad if e["event"] == "COMPLETE"][-1]
        e["time_ns"] = 0
        # Keep chronological order so rejection tests completion, not log order.
        bad.sort(key=lambda e: e["time_ns"])
    elif name == "record_window":
        config = replace(config, outstanding_records=1)
    elif name == "atom_capacity":
        config = replace(config, atom_queue_capacity=1)
    else:
        raise ValueError(name)
    return bad, config


class AuditMutationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = Config()
        cls.requests = stream(8)
        sim = Simulator(cls.config, cls.requests)
        sim.run()
        cls.trace = sim.trace

    def reject(self, trace, message, config=None, requests=None):
        with self.assertRaisesRegex(AuditError, message):
            check_trace(trace, config or self.config, requests or self.requests)

    def test_six_reported_holes_are_rejected(self):
        messages = ("missing_command", "command_duration", "activation_context",
                    "incomplete_record", "record_window", "atom_capacity")
        for name, message in zip(REPORTED_MUTATIONS, messages):
            with self.subTest(name=name):
                bad, c = mutate_reported(name, self.trace, self.config)
                self.reject(bad, message, c)

    def test_isolated_read_accept(self):
        bad = copy.deepcopy(self.trace)
        cid = next(e for e in bad if e["event"] == "READ_ACCEPT")["command_id"]
        bad = [e for e in bad if not (e["event"] == "COMMAND" and e["command_id"] == cid)]
        self.reject(bad, "missing_command")

    def test_duplicate_return(self):
        bad = copy.deepcopy(self.trace)
        i = next(i for i, e in enumerate(bad) if e["event"] == "DATA_END")
        bad.insert(i + 1, copy.deepcopy(bad[i]))
        self.reject(bad, "duplicate return")

    def test_missing_auto_pre_start_or_end(self):
        for kind in ("PRE_START", "PRE_END"):
            with self.subTest(kind=kind):
                self.reject([e for e in self.trace if e["event"] != kind], "PRE")
        self.reject([e for e in self.trace if e["event"] not in ("PRE_START", "PRE_END")],
                    "missing PRE lifecycle")

    def test_missing_explicit_pre_dispatch(self):
        c = replace(self.config, auto_precharge=False)
        reqs = balanced(32, 8, grains=1)
        sim = Simulator(c, reqs)
        sim.run()
        check_trace(sim.trace, c, reqs)
        cid = next(e for e in sim.trace if e["event"] == "COMMAND" and
                   e["command"] == "PRE")["command_id"]
        bad = [e for e in sim.trace if not (e["event"] == "COMMAND" and e["command_id"] == cid)]
        self.reject(bad, "unlinked PRE|command after close intent", c, reqs)

    def test_wrong_column_offset_even_if_entire_chain_agrees(self):
        bad = copy.deepcopy(self.trace)
        for e in bad:
            if e.get("rid") == 1 and "column_offset" in e:
                e["column_offset"] = 0  # rid 1 should read bytes 32..63
        self.reject(bad, "column_offset")

    def test_data_events_must_match_read_and_output_port(self):
        for kind, field, value in (("DATA_READY", "atom", 9),
                                   ("DATA_START", "column_offset", 64),
                                   ("DATA_END", "port", 1),
                                   ("PRE_END", "activation_id", 99999)):
            with self.subTest(kind=kind, field=field):
                bad = copy.deepcopy(self.trace)
                next(e for e in bad if e["event"] == kind)[field] = value
                self.reject(bad, "context|target")

    def test_missing_and_duplicate_data_ready(self):
        bad = copy.deepcopy(self.trace)
        i = next(i for i, e in enumerate(bad) if e["event"] == "DATA_READY")
        removed = bad.pop(i)
        self.reject(bad, "without DATA_READY")
        bad.insert(i, removed)
        bad.insert(i, copy.deepcopy(removed))
        self.reject(bad, "duplicate DATA_READY")

    def test_command_effect_time_lane_and_duplicate_identity(self):
        for field, value, reason in (("time_ns", 2, "command effect time"),
                                      ("grain", 1, "command effect target")):
            bad = copy.deepcopy(self.trace)
            next(e for e in bad if e["event"] == "ACT_ACCEPT")[field] = value
            self.reject(bad, reason)
        bad = copy.deepcopy(self.trace)
        next(e for e in bad if e["event"] == "COMMAND")["lane"] = -1
        self.reject(bad, "command lane")
        bad = copy.deepcopy(self.trace)
        i = next(i for i, e in enumerate(bad) if e["event"] == "COMMAND")
        bad.insert(i + 1, copy.deepcopy(bad[i]))
        self.reject(bad, "command identity")

    def test_auto_pre_requires_closing_read_intent(self):
        bad = copy.deepcopy(self.trace)
        cid = next(e for e in bad if e["event"] == "PRE_START")["command_id"]
        next(e for e in bad if e["event"] == "COMMAND" and
             e["command_id"] == cid)["auto_precharge"] = False
        self.reject(bad, "unlinked PRE")

    def test_independent_credit_peaks_and_return_credit_overflow(self):
        receipt = check_trace(self.trace, self.config, self.requests)
        self.assertEqual(receipt["reconstructed_peaks"]["live_records"], 8)
        self.assertEqual(receipt["reconstructed_peaks"]["outstanding_atoms"], 8)
        self.assertEqual(receipt["reconstructed_peaks"]["return_slots_per_grain"], 3)
        self.reject(self.trace, "return credits", replace(self.config, return_slots_per_grain=1))

    def test_record_completion_requires_every_atom_not_just_one(self):
        reqs = [Request(0, 0, 0, 0, 0, 64)]
        sim = Simulator(self.config, reqs)
        sim.run()
        bad = copy.deepcopy(sim.trace)
        completion = next(e for e in bad if e["event"] == "COMPLETE")
        bad.remove(completion)
        i = next(i for i, e in enumerate(bad) if e["event"] == "DATA_END")
        completion["time_ns"] = bad[i]["time_ns"]
        completion["latency_ns"] = completion["time_ns"]
        bad.insert(i + 1, completion)
        self.reject(bad, "incomplete_record", requests=reqs)

    def test_dependencies_and_release_rebuilt_from_admission(self):
        reqs = list(self.requests)
        reqs[1] = replace(reqs[1], depends=(0,))
        bad = copy.deepcopy(self.trace)
        next(e for e in bad if e["event"] == "ADMIT" and e["rid"] == 1)["depends"] = [0]
        self.reject(bad, "before release/dependencies", requests=reqs)
        reqs = list(self.requests)
        reqs[0] = replace(reqs[0], release_ns=2)
        bad = copy.deepcopy(self.trace)
        next(e for e in bad if e["event"] == "ADMIT")["release_ns"] = 2
        self.reject(bad, "before release/dependencies", requests=reqs)

    def test_unknown_event_and_legacy_log_are_not_silently_accepted(self):
        self.reject(self.trace + [{"time_ns": self.trace[-1]["time_ns"], "event": "TYPO"}],
                    "unknown event")
        bad = copy.deepcopy(self.trace)
        next(e for e in bad if e["event"] == "COMMAND").pop("command_id")
        self.reject(bad, "malformed")

    def test_equal_time_final_return_then_completion_is_legal(self):
        receipt = check_trace(self.trace, self.config, self.requests)
        self.assertTrue(receipt["pass"])
        for i, e in enumerate(self.trace):
            if e["event"] == "COMPLETE":
                self.assertEqual(self.trace[i - 1]["event"], "DATA_END")
                self.assertEqual(self.trace[i - 1]["time_ns"], e["time_ns"])

    def test_positive_and_negative_output_borrowing_controls(self):
        reqs = balanced(64, 8, grains=1)
        elapsed = {}
        for stall in (0, 160):
            for flexible in (False, True):
                c = replace(self.config, initial_output_stall_ns=stall, flexible_output=flexible)
                sim = Simulator(c, reqs)
                result = sim.run()
                check_trace(sim.trace, c, reqs)
                elapsed[stall, flexible] = result["elapsed_ns"]
                borrowed = sum(e["port"] != e["grain"] for e in sim.trace if e["event"] == "DATA_START")
                self.assertEqual(borrowed, 2 if stall and flexible else 0)
        self.assertEqual(elapsed[0, False], elapsed[0, True])
        self.assertEqual((elapsed[160, False], elapsed[160, True]), (416, 386))


if __name__ == "__main__":
    unittest.main()
