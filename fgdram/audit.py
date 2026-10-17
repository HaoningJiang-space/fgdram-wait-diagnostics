"""Independent replay of the P0 trace contract; no scheduler/readiness calls.

Version 2 requires command identities and explicit data/close provenance. Legacy
0270afa traces lack that evidence and are deliberately not silently upgraded.
Resource intervals are half-open; equal-time events follow their recorded order.
"""
from collections import Counter, defaultdict
from dataclasses import asdict


class AuditError(AssertionError):
    """A trace violates the declared diagnostic contract (also under python -O)."""


def require(condition, reason):
    if not condition:
        raise AuditError(reason)


ROW_FIELDS = ("grain", "pb", "sa", "row", "activation_id")
ATOM_FIELDS = ROW_FIELDS + ("rid", "atom", "column_offset", "bytes", "command_id")


def same(left, right, fields, reason):
    require(all(left[k] == right[k] for k in fields), reason)


def check_trace(trace, config, requests):
    """Reconstruct admissions, commands, row state, atoms and credits from events."""
    try:
        return _check_trace(trace, config, requests)
    except (KeyError, TypeError, ValueError, IndexError) as exc:
        raise AuditError(f"malformed or unlinked trace: {exc}") from exc


def _check_trace(trace, c, requests):
    expected = {r.rid: r for r in requests}
    require(bool(expected) and len(expected) == len(requests), "request IDs")
    earlier = set()
    for r in requests:
        require(0 <= r.grain < c.grains and 0 <= r.pb < c.pseudobanks and
                0 <= r.sa < c.subarrays and r.row >= 0, "request address")
        require(r.size > 0 and r.size % c.atom_bytes == 0 and r.offset >= 0 and
                r.offset % c.atom_bytes == 0 and r.offset + r.size <= c.row_bytes,
                "request column extent")
        require(set(r.depends) <= earlier, "request dependencies")
        earlier.add(r.rid)
    expected_atoms = {(r.rid, i) for r in requests for i in range(r.size // c.atom_bytes)}
    commands, effects, allocated_ids, pending_acts = {}, set(), set(), set()
    active, closing, pre_starts, pre_ends = {}, {}, {}, set()
    activation_times = defaultdict(list)
    admitted, live, completed = {}, set(), set()
    issued, reads, ready, departures, returned = {}, set(), {}, {}, set()
    returns_per_record, last_return = Counter(), {}
    buffers, peaks = Counter(), Counter()
    atom_credits = 0
    intervals, power, rrd = defaultdict(list), defaultdict(list), defaultdict(list)
    end = -1

    def row_context(e):
        key = (e["grain"], e["pb"])
        require(key in active, "activation_context: no open row")
        a = active[key]
        same(e, a, ROW_FIELDS, "activation_context: wrong activation/row")
        return key, a

    def command_for(e, kind, fields):
        cid = e["command_id"]
        require(cid in commands, "missing_command: effect without dispatch")
        cmd = commands[cid]
        require(cmd["command"] == kind, "command effect type")
        same(e, cmd, fields, "command effect target")
        return cmd

    def accept(e, kind, fields):
        cmd = command_for(e, kind, fields)
        cid = e["command_id"]
        require(cid not in effects, "duplicate command effect")
        require(e["time_ns"] == cmd["accept_ns"], "command effect time")
        effects.add(cid)
        return cmd

    def read_command(e):
        cmd = command_for(e, "RD", ATOM_FIELDS)
        require(e["command_id"] in effects, "data without accepted READ")
        return cmd

    for index, e in enumerate(trace):
        t, kind = e["time_ns"], e["event"]
        require(type(t) is int and t >= 0 and t >= end,
                f"event_order: event {index} is not chronological")
        end = t
        if kind == "ADMIT":
            rid = e["rid"]
            require(rid in expected and rid not in admitted, "admission identity")
            r = expected[rid]
            same(e, asdict(r), ("rid", "grain", "pb", "sa", "row", "size", "offset", "release_ns"),
                 "admission request mismatch")
            require(tuple(e["depends"]) == tuple(r.depends), "admission dependencies")
            require(t >= r.release_ns and set(r.depends) <= completed,
                    "admission before release/dependencies")
            require(len(live) < c.outstanding_records, "record_window exceeded")
            atoms = r.size // c.atom_bytes
            require(atom_credits + atoms <= c.atom_queue_capacity, "atom_capacity exceeded")
            admitted[rid] = t
            live.add(rid)
            atom_credits += atoms
            peaks["live_records"] = max(peaks["live_records"], len(live))
            peaks["outstanding_atoms"] = max(peaks["outstanding_atoms"], atom_credits)
        elif kind == "COMMAND":
            cid, cmd, g, p = (e[k] for k in ("command_id", "command", "grain", "pb"))
            require(type(cid) is int and cid > 0 and cid not in commands, "command identity")
            require(cmd in ("ACT", "PRE", "RD"), "unknown command")
            require(0 <= g < c.grains and 0 <= p < c.pseudobanks and
                    0 <= e["sa"] < c.subarrays and e["row"] >= 0, "command address")
            require(type(e["activation_id"]) is int and e["activation_id"] > 0,
                    "activation identity")
            require(e["rid"] in live, "command for non-live request")
            duration = {"ACT": c.act_command_ns, "PRE": c.pre_command_ns,
                        "RD": c.read_command_ns}[cmd]
            require(e["duration_ns"] == duration, "command_duration differs from config")
            require(t % c.clock_ns == 0 and e["accept_ns"] == t + duration, "command timing")
            require(e["group"] == g // c.grains_per_group, "command group")
            family = "column" if cmd == "RD" else "row"
            require(type(e["lane"]) is int and 0 <= e["lane"] <
                    (1 if family == "column" else c.row_lanes), "command lane")
            require(type(e["auto_precharge"]) is bool and
                    (not e["auto_precharge"] or (cmd == "RD" and c.auto_precharge)),
                    "automatic PRE intent")
            intervals[family, e["group"], e["lane"]].append((t, e["accept_ns"]))
            if family == "row":
                intervals["bank_row_decoder", g // 2].append(
                    (e["accept_ns"], e["accept_ns"] + c.clock_ns))
            key = (g, p)
            if cmd == "ACT":
                r = expected[e["rid"]]
                same(e, asdict(r), ("grain", "pb", "sa", "row"), "ACT request target")
                require(e["activation_id"] not in allocated_ids, "duplicate activation identity")
                require(key not in pending_acts, "multiple pending ACTs")
                allocated_ids.add(e["activation_id"])
                pending_acts.add(key)
            else:
                _, a = row_context(e)
                require(not a["pre"] and key not in closing, "command after close intent")
                if cmd == "RD":
                    r = expected[e["rid"]]
                    same(e, asdict(r), ("grain", "pb", "sa", "row"), "READ request target")
                    atom = (e["rid"], e["atom"])
                    require(atom in expected_atoms and atom not in issued, "READ atom identity")
                    require(e["column_offset"] == r.offset + e["atom"] * c.atom_bytes and
                            e["bytes"] == c.atom_bytes, "column_offset/width mismatch")
                    issued[atom] = cid
                    buffers[g] += 1
                    require(buffers[g] <= c.return_slots_per_grain, "return credits exceeded")
                    peaks["return_slots_per_grain"] = max(peaks["return_slots_per_grain"], buffers[g])
                if cmd == "PRE" or e["auto_precharge"]:
                    closing[key] = cid
            commands[cid] = e
        elif kind == "ACT_ACCEPT":
            accept(e, "ACT", ROW_FIELDS + ("rid",))
            g, p, sa, row = (e[k] for k in ("grain", "pb", "sa", "row"))
            key = (g, p)
            require(key not in active, "ACT before prior PRE_END")
            for (other_g, _), v in active.items():
                if other_g // 2 == g // 2 and v["sa"] == sa:
                    require(v["row"] == row, "shared subarray row conflict")
            prior = activation_times[key]
            require(not prior or t - prior[-1] >= c.trc_ns, "tRC")
            prior.append(t)
            active[key] = dict(e, pre=False, last_read=None)
            pending_acts.remove(key)
            power[0 if c.power_scope == "die" else g // c.grains_per_group].append(t)
            rrd[0 if c.rrd_scope == "die" else g // c.grains_per_group].append(t)
        elif kind == "READ_ACCEPT":
            # Check the current activation before checking dispatch metadata so an
            # invented activation gets a specific diagnostic.
            _, a = row_context(e)
            accept(e, "RD", ATOM_FIELDS)
            require(not a["pre"], "read during PRE")
            require(t >= a["time_ns"] + c.trcd_ns, "tRCD")
            atom = (e["rid"], e["atom"])
            require(atom not in reads, "duplicate read")
            reads.add(atom)
            a["last_read"] = t
            intervals["source", e["grain"]].append((t, t + c.source_ns))
        elif kind == "PRE_START":
            key, a = row_context(e)
            require(type(e["auto"]) is bool, "PRE mode")
            require(key in closing and closing[key] == e["command_id"], "unlinked PRE")
            if e["auto"]:
                cmd = command_for(e, "RD", ROW_FIELDS)
                require(cmd["auto_precharge"] and e["command_id"] in effects,
                        "automatic PRE without accepted closing READ")
            else:
                accept(e, "PRE", ROW_FIELDS)
            require(not a["pre"], "duplicate PRE_START")
            require(t >= a["time_ns"] + c.tras_ns, "tRAS")
            if a["last_read"] is not None:
                require(t >= a["last_read"] + max(c.trtp_ns, c.source_ns), "read hold / pumps")
            require(e["command_id"] not in pre_starts, "duplicate close")
            pre_starts[e["command_id"]] = e
            a["pre"] = True
        elif kind == "PRE_END":
            key, a = row_context(e)
            cid = e["command_id"]
            require(cid in pre_starts and cid not in pre_ends, "missing/duplicate PRE_START")
            start = pre_starts[cid]
            same(e, start, ROW_FIELDS + ("auto", "command_id"), "PRE_END context")
            require(a["pre"] and t == start["time_ns"] + c.trp_ns, "tRP")
            pre_ends.add(cid)
            del active[key]
            del closing[key]
        elif kind == "DATA_READY":
            cmd = read_command(e)
            atom = (e["rid"], e["atom"])
            require(atom not in ready, "duplicate DATA_READY")
            require(t == cmd["accept_ns"] + max(c.tcl_ns, c.source_ns), "data readiness time")
            ready[atom] = t
        elif kind == "DATA_START":
            read_command(e)
            atom = (e["rid"], e["atom"])
            require(atom in ready, "DATA_START without DATA_READY")
            require(t >= ready[atom] and e["ready_ns"] == ready[atom] and
                    t >= c.initial_output_stall_ns, "data output before ready")
            g, port = e["grain"], e["port"]
            require(type(port) is int and 0 <= port < c.grains and
                    (port == g or (c.flexible_output and port // 2 == g // 2)), "output port")
            require(e["end_ns"] == t + c.burst_ns, "output burst duration")
            require(atom not in departures, "duplicate DATA_START")
            departures[atom] = e
            intervals["port", port].append((t, e["end_ns"]))
        elif kind == "DATA_END":
            read_command(e)
            atom = (e["rid"], e["atom"])
            require(atom in departures and atom not in returned, "missing/duplicate return")
            start = departures[atom]
            same(e, start, ATOM_FIELDS + ("port",), "DATA_END context")
            require(t == start["end_ns"], "return without matching full output burst")
            require(e["rid"] in live, "return for non-live request")
            returned.add(atom)
            returns_per_record[e["rid"]] += 1
            last_return[e["rid"]] = t
            buffers[e["grain"]] -= 1
            atom_credits -= 1
            require(buffers[e["grain"]] >= 0 and atom_credits >= 0, "negative credits")
        elif kind == "COMPLETE":
            rid = e["rid"]
            require(rid in live and rid not in completed, "completion identity")
            require(returns_per_record[rid] == expected[rid].size // c.atom_bytes,
                    "incomplete_record: completion before all data returns")
            require(t == last_return[rid], "completion time differs from final return")
            require(e["admitted_ns"] == admitted[rid] and
                    e["latency_ns"] == t - admitted[rid], "completion latency")
            completed.add(rid)
            live.remove(rid)
        else:
            raise AuditError(f"unknown event {kind!r}")

    require(set(commands) == effects, "missing command effect")
    expected_closes = {cid for cid, cmd in commands.items()
                       if cmd["command"] == "PRE" or cmd["auto_precharge"]}
    require(expected_closes == set(pre_starts) == pre_ends, "missing PRE lifecycle")
    require(not pending_acts and not closing, "unfinished row operation")
    require(set(issued) == reads == set(ready) == set(departures) == returned == expected_atoms,
            "missing atom lifecycle")
    require(set(admitted) == completed == set(expected) and not live, "missing record lifecycle")
    require(atom_credits == 0 and all(v == 0 for v in buffers.values()), "unreleased credits")
    for name, spans in intervals.items():
        spans.sort()
        for prev, nxt in zip(spans, spans[1:]):
            require(nxt[0] >= prev[1], f"resource double booking: {name}")
    for times in power.values():
        times.sort()
        for i in range(c.faw_limit, len(times)):
            require(times[i] - times[i - c.faw_limit] >= c.tfaw_ns, "activation window")
    for times in rrd.values():
        times.sort()
        require(all(b - a >= c.trrd_ns for a, b in zip(times, times[1:])), "tRRD")
    return {"pass": True, "trace_contract": "p0-v2", "commands": len(commands),
            "activations": len(allocated_ids), "returned_atoms": len(returned),
            "resources_checked": len(intervals), "trace_end_ns": end,
            "reconstructed_peaks": dict(peaks), "closed_activations": len(pre_ends)}
