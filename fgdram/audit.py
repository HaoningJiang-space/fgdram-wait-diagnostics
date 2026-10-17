"""Post-run interval/command checker; does not invoke scheduler or readiness code."""
from collections import Counter, defaultdict


def check_trace(trace, config, requests):
    c = config
    expected = {r.rid: r for r in requests}
    active = {}
    activation_times = defaultdict(list)
    last_reads = {}
    act_reads = Counter()
    completed = set()
    admitted = set()
    read_atoms = set()
    returned = set()
    ready = {}
    departures = {}
    buffers = Counter()
    intervals = defaultdict(list)
    power = defaultdict(list)
    rrd = defaultdict(list)
    read_times = defaultdict(list)
    act_ids = set()
    end = 0
    for e in trace:
        t, kind = e["time_ns"], e["event"]
        end = max(end, t)
        if kind == "ADMIT":
            rid = e["rid"]
            assert rid not in admitted
            assert t >= expected[rid].release_ns
            assert set(expected[rid].depends) <= completed
            admitted.add(rid)
        elif kind == "COMMAND":
            cmd, g = e["command"], e["grain"]
            assert t % c.clock_ns == 0
            assert e["accept_ns"] == t + e["duration_ns"]
            assert e["group"] == g // c.grains_per_group
            family = "column" if cmd == "RD" else "row"
            assert e["lane"] < (1 if family == "column" else c.row_lanes)
            intervals[(family, e["group"], e["lane"])].append((t, e["accept_ns"]))
            if family == "row":
                intervals[("bank_row_decoder", g // 2)].append(
                    (e["accept_ns"], e["accept_ns"] + c.clock_ns))
            if cmd == "RD":
                buffers[g] += 1
                assert buffers[g] <= c.return_slots_per_grain, "finite return buffer"
        elif kind == "ACT_ACCEPT":
            g, p, sa, row = (e[k] for k in ("grain", "pb", "sa", "row"))
            assert (g, p) not in active, "ACT before prior PRE_END"
            for (other_g, _), v in active.items():
                if other_g // 2 == g // 2 and v["sa"] == sa:
                    assert v["row"] == row, "shared subarray row conflict"
            prior = activation_times[g, p]
            if prior:
                assert t - prior[-1] >= c.trc_ns, "tRC"
            prior.append(t)
            active[g, p] = dict(e, pre=False)
            act_ids.add(e["activation_id"])
            ps = 0 if c.power_scope == "die" else g // c.grains_per_group
            rs = 0 if c.rrd_scope == "die" else g // c.grains_per_group
            power[ps].append(t)
            rrd[rs].append(t)
        elif kind == "READ_ACCEPT":
            g, p, rid, atom = (e[k] for k in ("grain", "pb", "rid", "atom"))
            a = active[g, p]
            assert not a["pre"], "read during PRE"
            assert t >= a["time_ns"] + c.trcd_ns, "tRCD"
            assert (a["sa"], a["row"]) == (e["sa"], e["row"])
            r = expected[rid]
            assert rid in admitted and (r.grain, r.pb, r.sa, r.row) == (g, p, e["sa"], e["row"])
            assert 0 <= atom < r.size // c.atom_bytes
            assert (rid, atom) not in read_atoms, "duplicate read"
            read_atoms.add((rid, atom))
            last_reads[g, p] = t
            act_reads[e["activation_id"]] += 1
            read_times[g].append(t)
            intervals[("source", g)].append((t, t + c.source_ns))
            ready[rid, atom] = t + max(c.tcl_ns, c.source_ns)
        elif kind == "PRE_START":
            key = (e["grain"], e["pb"])
            a = active[key]
            assert not a["pre"]
            assert t >= a["time_ns"] + c.tras_ns, "tRAS"
            if key in last_reads:
                assert t >= last_reads[key] + max(c.trtp_ns, c.source_ns), "read hold / pumps"
            a["pre"] = True
            a["pre_time"] = t
        elif kind == "PRE_END":
            key = (e["grain"], e["pb"])
            a = active.pop(key)
            assert a["pre"] and t - a["pre_time"] >= c.trp_ns, "tRP"
            last_reads.pop(key, None)
        elif kind == "DATA_START":
            g, port = e["grain"], e["port"]
            key = (e["rid"], e["atom"])
            assert t >= ready[key] and t >= c.initial_output_stall_ns
            assert port == g or (c.flexible_output and port // 2 == g // 2)
            assert e["end_ns"] - t == c.burst_ns
            assert key not in departures
            departures[key] = e["end_ns"]
            intervals[("port", port)].append((t, e["end_ns"]))
        elif kind == "DATA_END":
            key = (e["rid"], e["atom"])
            assert key in read_atoms and key not in returned
            assert departures[key] == t, "return without matching full output burst"
            returned.add(key)
            buffers[e["grain"]] -= 1
            assert buffers[e["grain"]] >= 0
        elif kind == "COMPLETE":
            assert e["rid"] not in completed
            # COMPLETE is logged immediately before its final DATA_END.
            completed.add(e["rid"])
    for name, spans in intervals.items():
        spans.sort()
        for prev, nxt in zip(spans, spans[1:]):
            assert nxt[0] >= prev[1], f"resource double booking: {name}"
    for times in power.values():
        times.sort()
        for i in range(c.faw_limit, len(times)):
            assert times[i] - times[i - c.faw_limit] >= c.tfaw_ns, "activation window"
    for times in rrd.values():
        times.sort()
        assert all(b - a >= c.trrd_ns for a, b in zip(times, times[1:])), "tRRD"
    assert len(returned) == sum(r.size // c.atom_bytes for r in requests)
    assert set(departures) == returned
    assert completed == set(expected)
    assert all(v == 0 for v in buffers.values())
    return {"pass": True, "commands": sum(e["event"] == "COMMAND" for e in trace),
            "activations": len(act_ids), "returned_atoms": len(returned),
            "resources_checked": len(intervals), "trace_end_ns": end}
