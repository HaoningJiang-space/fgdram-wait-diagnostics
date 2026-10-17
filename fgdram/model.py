from __future__ import annotations

from collections import Counter, deque
from dataclasses import asdict, dataclass, field
import heapq


@dataclass(frozen=True)
class Config:
    grains: int = 128                 # full modeled die; first group is normally driven
    grains_per_group: int = 8
    pseudobanks: int = 2
    subarrays: int = 32              # illustrative address domain, not paper capacity
    row_bytes: int = 256
    atom_bytes: int = 32
    clock_ns: int = 2
    act_command_ns: int = 4          # assumption: paper says only > 2 ns
    pre_command_ns: int = 2          # assumption: minimum command length
    read_command_ns: int = 2         # assumption: minimum command length
    trcd_ns: int = 16
    tras_ns: int = 29
    trp_ns: int = 16
    trc_ns: int = 45
    tcl_ns: int = 16
    source_ns: int = 16              # 8 internal pumps at 500 MHz
    burst_ns: int = 16
    trtp_ns: int = 16                # assumed read-hold; NOT published tRTP
    trrd_ns: int = 2
    tfaw_ns: int = 12
    faw_limit: int = 32
    power_scope: str = "die"        # uncertain in paper; shared die scope retained
    rrd_scope: str = "group"        # uncertain in paper; explicit sensitivity input
    row_lanes: int = 1              # C=2; column capability unchanged
    flexible_output: bool = False   # D: pairs of neighboring outputs, after source
    return_slots_per_grain: int = 4 # assumption; reserves at command dispatch, until departure
    outstanding_records: int = 64
    atom_queue_capacity: int = 256
    auto_precharge: bool = True
    adjacent_sa_exclusion: bool = True  # conservative open-bitline conflict model
    initial_output_stall_ns: int = 0  # test-only finite receiver backpressure
    max_ns: int = 2_000_000

    def __post_init__(self):
        assert self.grains > 0 and self.grains % self.grains_per_group == 0
        assert self.grains % 2 == 0 and self.pseudobanks == 2
        assert self.power_scope in ("die", "group")
        assert self.rrd_scope in ("die", "group")
        assert self.act_command_ns > 2
        for name in ("act_command_ns", "pre_command_ns", "read_command_ns"):
            assert getattr(self, name) % self.clock_ns == 0
        assert self.row_lanes > 0 and self.return_slots_per_grain > 0
        assert self.outstanding_records > 0 and self.atom_queue_capacity > 0
        assert self.trc_ns >= self.tras_ns + self.trp_ns
        assert self.source_ns > 0 and self.burst_ns > 0
        assert self.tfaw_ns > 0 and self.faw_limit > 0


@dataclass(frozen=True)
class Request:
    rid: int
    grain: int
    pb: int
    sa: int
    row: int
    size: int
    offset: int = 0
    release_ns: int = 0
    depends: tuple[int, ...] = ()

    @property
    def key(self):
        return (self.grain, self.pb, self.sa, self.row)


@dataclass
class RowState:
    row: tuple[int, int] | None = None
    phase: str = "closed"
    act: int = -10**9
    ready: int = 0
    next_act: int = 0
    read_hold: int = 0
    close_pending: bool = False
    reads: int = 0
    activation_id: int = -1
    pending_act: bool = False
    pre_end: int = 0


@dataclass
class Live:
    request: Request
    admitted: int
    issued: int = 0
    returned: int = 0


class Simulator:
    """2 ns scheduler, integer-ns events, explicit command transport/acceptance.

    A is intentionally unavailable: the public FGDRAM description is insufficient
    to certify an additional independent row-control domain and its circuit cost.
    """

    def __init__(self, config: Config, requests: list[Request], capture=True,
                 candidate_observer=None, streaming=False):
        self.c = config
        self.requests = requests
        ids = [r.rid for r in requests]
        assert len(ids) == len(set(ids)) and (requests or streaming)
        earlier = set()
        for r in requests:
            assert 0 <= r.grain < config.grains and 0 <= r.pb < 2
            assert 0 <= r.sa < config.subarrays and r.row >= 0
            assert r.size > 0 and r.size % config.atom_bytes == 0
            assert r.offset % config.atom_bytes == 0
            assert r.offset >= 0 and r.offset + r.size <= config.row_bytes
            assert r.size // config.atom_bytes <= config.atom_queue_capacity
            assert set(r.depends) <= earlier, "dependencies must precede request"
            earlier.add(r.rid)
        self.capture = capture
        self.candidate_observer = candidate_observer
        self.trace = []
        self.wait_samples = []
        self.rows = {(g, p): RowState() for g in range(config.grains) for p in range(2)}
        groups = config.grains // config.grains_per_group
        self.row_bus = [[0] * config.row_lanes for _ in range(groups)]
        self.col_bus = [0] * groups
        self.row_decoder_slots = [[] for _ in range(config.grains // 2)]
        self.source_busy = [0] * config.grains
        self.port_busy = [config.initial_output_stall_ns] * config.grains
        self.reserved = [0] * config.grains
        self.ready_atoms = []
        self.events = []
        self.serial = 0
        self.command_serial = 0  # trace identity only; independent of event ordering
        self.t = 0
        self.live = {}
        self.completed = {}
        self.next_request = 0
        self.power_acts = {}
        self.last_act = {}
        self.waits = Counter()
        self.wait_masks = Counter()
        self.stats = Counter()
        self.reads_per_act = Counter()
        self.latencies = []
        self.activations = 0
        self.pending_activations = {}
        self.atoms_reserved = 0
        self.max_inflight = 0
        self.max_atom_queue = 0
        self.completed_atom_events = []

    def log(self, event, **data):
        if self.capture:
            self.trace.append({"time_ns": self.t, "event": event, **data})

    def schedule(self, when, kind, **data):
        self.serial += 1
        heapq.heappush(self.events, (when, self.serial, kind, data))

    def group(self, grain):
        return grain // self.c.grains_per_group

    def scope(self, grain, which):
        return 0 if which == "die" else self.group(grain)

    def queued(self):
        return [x for x in self.live.values()
                if x.issued < x.request.size // self.c.atom_bytes]

    def wants(self, grain, pb, row):
        return any((x.request.grain, x.request.pb) == (grain, pb)
                   and (x.request.sa, x.request.row) == row for x in self.queued())

    def admit(self):
        while self.next_request < len(self.requests):
            r = self.requests[self.next_request]
            if r.release_ns > self.t or not all(d in self.completed for d in r.depends):
                self.stats["input_dependency_or_release_ns"] += self.c.clock_ns
                break
            atoms = r.size // self.c.atom_bytes
            if len(self.live) >= self.c.outstanding_records:
                self.stats["input_record_window_full_ns"] += self.c.clock_ns
                break
            if self.atoms_reserved + atoms > self.c.atom_queue_capacity:
                self.stats["input_atom_queue_full_ns"] += self.c.clock_ns
                break
            self.live[r.rid] = Live(r, self.t)
            self.atoms_reserved += atoms
            self.next_request += 1
            self.log("ADMIT", **asdict(r))
        self.max_inflight = max(self.max_inflight, len(self.live))
        self.max_atom_queue = max(self.max_atom_queue, self.atoms_reserved)

    def conflicts(self, grain, pb, sa, row, at_time=None):
        result = []
        # One physical bank contains two grains/four pseudobanks (§3.2/Fig.7).
        for g in range(grain // 2 * 2, grain // 2 * 2 + 2):
            for p in range(2):
                if (g, p) == (grain, pb):
                    continue
                s = self.rows[g, p]
                exiting = at_time is not None and s.close_pending and s.pre_end <= at_time
                if s.row is not None and not exiting:
                    other_sa, other_row = s.row
                    if other_sa == sa and other_row != row:
                        result.append((g, p, "shared_row_control"))
                pending = self.pending_activations.get((g, p))
                if pending and pending[0] == sa and pending[1] != row:
                    result.append((g, p, "shared_row_control"))
        # Same pseudobank slice: conservatively retain neighboring BLSA conflict.
        s = self.rows[grain, pb]
        exiting = at_time is not None and s.close_pending and s.pre_end <= at_time
        if s.row is not None and self.c.adjacent_sa_exclusion and not exiting:
            if abs(s.row[0] - sa) == 1:
                result.append((grain, pb, "shared_sense_amplifier"))
        return result

    def candidates(self):
        candidates = []
        seen = set()
        for x in self.queued():
            r = x.request
            s = self.rows[r.grain, r.pb]
            wanted = (r.sa, r.row)
            if s.row == wanted and not s.close_pending and s.phase != "precharging":
                candidates.append(("RD", r.grain, r.pb, r.rid))
                continue
            key = ("ACT", r.grain, r.pb, r.sa, r.row)
            if key not in seen:
                candidates.append(("ACT", r.grain, r.pb, r.rid))
                seen.add(key)
            victims = [(r.grain, r.pb)] if s.row is not None else []
            victims += [(g, p) for g, p, _ in self.conflicts(r.grain, r.pb, *wanted)]
            for g, p in victims:
                v = self.rows[g, p]
                if (v.row is not None and not v.close_pending
                        and not self.wants(g, p, v.row) and ("PRE", g, p) not in seen):
                    candidates.append(("PRE", g, p, r.rid))
                    seen.add(("PRE", g, p))
        return candidates

    def blockers(self, candidate):
        kind, g, p, rid = candidate
        c, t = self.c, self.t
        s = self.rows[g, p]
        group = self.group(g)
        reasons = set()
        if kind in ("ACT", "PRE"):
            if min(self.row_bus[group]) > t:
                reasons.add("row_command_bus")
            duration = c.act_command_ns if kind == "ACT" else c.pre_command_ns
            accept = t + duration
            if any(a < accept + c.clock_ns and accept < b
                   for a, b in self.row_decoder_slots[g // 2]):
                reasons.add("bank_row_decoder")
        if kind == "ACT":
            r = self.live[rid].request
            accept = t + c.act_command_ns
            can_arrive = s.phase == "closed" or (s.close_pending and s.pre_end <= accept)
            if not can_arrive or s.pending_act:
                reasons.add("row_state_occupied")
            if accept < s.next_act:
                reasons.add("local_tRC")
            if s.phase == "precharging" and accept < s.pre_end:
                reasons.add("local_tRP")
            reasons.update(reason for _, _, reason in self.conflicts(g, p, r.sa, r.row, accept))
            ps = self.scope(g, c.power_scope)
            rs = self.scope(g, c.rrd_scope)
            if accept - self.last_act.get(rs, -10**9) < c.trrd_ns:
                reasons.add("activation_tRRD")
            acts = self.power_acts.get(ps, ())
            if sum(a > accept - c.tfaw_ns for a in acts) >= c.faw_limit:
                reasons.add("activation_window")
        elif kind == "PRE":
            if s.close_pending or s.row is None:
                reasons.add("row_state_occupied")
            if t + c.pre_command_ns < s.act + c.tras_ns:
                reasons.add("local_tRAS")
            if t + c.pre_command_ns < s.read_hold:
                reasons.add("local_read_hold")
        else:
            r = self.live[rid].request
            accept = t + c.read_command_ns
            if s.row != (r.sa, r.row) or s.close_pending or s.phase == "precharging":
                reasons.add("row_state_occupied")
            if accept < s.ready:
                reasons.add("local_tRCD")
            if self.col_bus[group] > t:
                reasons.add("column_command_bus")
            if accept < self.source_busy[g]:
                reasons.add("local_readout_path")
            if self.reserved[g] >= c.return_slots_per_grain:
                reasons.add("return_buffer")
        return sorted(reasons)

    def issue(self, candidate):
        kind, g, p, rid = candidate
        c, t = self.c, self.t
        s = self.rows[g, p]
        group = self.group(g)
        self.command_serial += 1
        command_id = self.command_serial
        auto_precharge = False
        if kind in ("ACT", "PRE"):
            lane = next(i for i, end in enumerate(self.row_bus[group]) if end <= t)
            duration = c.act_command_ns if kind == "ACT" else c.pre_command_ns
            accept = t + duration
            self.row_bus[group][lane] = accept
            slots = self.row_decoder_slots[g // 2]
            slots[:] = [(a, b) for a, b in slots if b > t]
            slots.append((accept, accept + c.clock_ns))
            self.stats["row_bus_occupied_ns"] += duration
        else:
            lane = 0
            duration = c.read_command_ns
            accept = t + duration
            self.col_bus[group] = accept
            self.stats["column_bus_occupied_ns"] += duration
        if kind == "ACT":
            r = self.live[rid].request
            self.activations += 1
            s.pending_act = True
            self.pending_activations[g, p] = (r.sa, r.row, accept)
            ps, rs = self.scope(g, c.power_scope), self.scope(g, c.rrd_scope)
            acts = self.power_acts.setdefault(ps, deque())
            while acts and acts[0] <= accept - c.tfaw_ns:
                acts.popleft()
            acts.append(accept)
            self.last_act[rs] = accept
            context = dict(sa=r.sa, row=r.row, activation_id=self.activations)
            self.schedule(accept, "ACT_ACCEPT", grain=g, pb=p, rid=rid,
                          command_id=command_id, **context)
        elif kind == "PRE":
            context = dict(sa=s.row[0], row=s.row[1], activation_id=s.activation_id)
            s.close_pending = True
            s.pre_end = accept + c.trp_ns
            self.schedule(accept, "PRE_START", grain=g, pb=p, auto=False,
                          command_id=command_id, **context)
        else:
            x = self.live[rid]
            r = x.request
            atom = x.issued
            x.issued += 1
            s.reads += 1
            self.source_busy[g] = accept + c.source_ns
            s.read_hold = max(s.read_hold, accept + c.source_ns, accept + c.trtp_ns)
            self.reserved[g] += 1
            context = dict(sa=r.sa, row=r.row, activation_id=s.activation_id,
                           atom=atom, column_offset=r.offset + atom * c.atom_bytes,
                           bytes=c.atom_bytes)
            data = dict(grain=g, pb=p, rid=rid, command_id=command_id, **context)
            self.schedule(accept, "READ_ACCEPT", **data)
            # Conservative atom-ready abstraction: no extra per-MAT bandwidth.
            self.schedule(accept + max(c.tcl_ns, c.source_ns), "DATA_READY", **data)
            if c.auto_precharge and not self.wants(g, p, s.row):
                auto_precharge = True
                s.close_pending = True
                pre_start = max(s.act + c.tras_ns, s.read_hold)
                s.pre_end = pre_start + c.trp_ns
                self.schedule(pre_start, "PRE_START",
                              grain=g, pb=p, auto=True, command_id=command_id,
                              sa=r.sa, row=r.row, activation_id=s.activation_id)
        self.stats[kind] += 1
        self.log("COMMAND", command=kind, grain=g, pb=p, rid=rid, group=group,
                 lane=lane, accept_ns=accept, duration_ns=duration,
                 command_id=command_id, auto_precharge=auto_precharge, **context)

    def process_events(self):
        target = self.t
        while self.events and self.events[0][0] <= target:
            time, _, kind, data = heapq.heappop(self.events)
            self.t = time
            completion = None
            if kind == "ACT_ACCEPT":
                key = (data["grain"], data["pb"])
                s = self.rows[key]
                assert s.row is None, "ACT executes only after prior PRE completes"
                s.phase = "open"
                s.row = (data["sa"], data["row"])
                s.act = time
                s.ready = time + self.c.trcd_ns
                s.next_act = time + self.c.trc_ns
                s.read_hold = 0
                s.reads = 0
                s.activation_id = data["activation_id"]
                s.pending_act = False
                del self.pending_activations[key]
            elif kind == "PRE_START":
                s = self.rows[data["grain"], data["pb"]]
                assert s.row is not None
                assert time >= max(s.act + self.c.tras_ns, s.read_hold)
                data.update(sa=s.row[0], row=s.row[1], activation_id=s.activation_id)
                self.reads_per_act[s.reads] += 1
                s.phase = "precharging"
                self.schedule(time + self.c.trp_ns, "PRE_END",
                              **data)
            elif kind == "PRE_END":
                s = self.rows[data["grain"], data["pb"]]
                s.row = None
                s.phase = "closed"
                s.close_pending = False
            elif kind == "DATA_READY":
                self.ready_atoms.append((time, data))
            elif kind == "DATA_END":
                rid, g = data["rid"], data["grain"]
                x = self.live[rid]
                x.returned += 1
                self.reserved[g] -= 1
                self.atoms_reserved -= 1
                self.stats["returned_atoms"] += 1
                self.completed_atom_events.append((time, g))
                if x.returned == x.request.size // self.c.atom_bytes:
                    self.completed[rid] = time
                    self.latencies.append(time - x.admitted)
                    completion = dict(rid=rid, admitted_ns=x.admitted,
                                      latency_ns=time - x.admitted)
                    del self.live[rid]
            self.log(kind, **data)
            if completion is not None:
                self.log("COMPLETE", **completion)
        self.t = target

    def drain_outputs(self):
        remaining = []
        for ready, data in self.ready_atoms:
            g = data["grain"]
            ports = [g] if not self.c.flexible_output else [g // 2 * 2, g // 2 * 2 + 1]
            free = [p for p in ports if self.port_busy[p] <= self.t]
            if not free:
                self.waits["data_output_path_candidate_ns"] += self.c.clock_ns
                remaining.append((ready, data))
                continue
            port = g if g in free else free[0]
            end = self.t + self.c.burst_ns
            self.port_busy[port] = end
            self.stats["output_occupied_ns"] += self.c.burst_ns
            self.log("DATA_START", **data, port=port, ready_ns=ready, end_ns=end)
            self.schedule(end, "DATA_END", **data, port=port)
        self.ready_atoms = remaining

    def order_candidates(self, candidates):
        """Default policy, with an explicit hook for separately labelled diagnostics."""
        return sorted(candidates, key=lambda v: ({"RD": 0, "PRE": 1, "ACT": 2}[v[0]], v[3]))

    def step(self, before_admit=None):
        """One unchanged scheduler tick; an online producer may run after returns.

        The callback can append ready physical requests. Legacy run() has no
        callback and retains its original event and arbitration order.
        """
        c = self.c
        if self.t > c.max_ns:
            raise RuntimeError(f"timeout at {self.t} ns; completed {len(self.completed)}")
        self.process_events()
        if before_admit is not None:
            before_admit()
        self.admit()
        self.drain_outputs()
        candidates = self.candidates()
        snapshot = [(candidate, self.blockers(candidate)) for candidate in candidates]
        if not candidates:
            self.stats["no_pending_command_candidate_ns"] += c.clock_ns
        if candidates and all(reasons for _, reasons in snapshot):
            self.stats["all_command_candidates_blocked_ns"] += c.clock_ns
        for candidate, reasons in snapshot:
            if reasons:
                self.wait_masks["+".join(reasons)] += c.clock_ns
                for reason in reasons:
                    self.waits[reason + "_candidate_ns"] += c.clock_ns
                if self.capture and len(self.wait_samples) < 4000:
                    self.wait_samples.append({"time_ns": self.t,
                                              "candidate": candidate,
                                              "blockers": reasons})
        # Row hits first, then precharges, then oldest activation. No queue-HOL gate.
        candidates = self.order_candidates(candidates)
        issued = set()
        for candidate in candidates:
            if not self.blockers(candidate):
                self.issue(candidate)
                issued.add(candidate)
        if self.candidate_observer is not None:
            self.candidate_observer(self.t, snapshot, issued)
        for candidate, reasons in snapshot:
            self.stats["command_candidate_ns"] += c.clock_ns
            if not reasons and candidate not in issued:
                self.waits["scheduler_arbitration_candidate_ns"] += c.clock_ns
                self.wait_masks["scheduler_arbitration"] += c.clock_ns
                if self.capture and len(self.wait_samples) < 4000:
                    self.wait_samples.append({"time_ns": self.t, "candidate": candidate,
                                              "blockers": ["scheduler_arbitration"]})
        self.t += c.clock_ns

    def run(self):
        c = self.c
        while len(self.completed) < len(self.requests):
            self.step()
        end = max(self.completed.values())
        # Close-page housekeeping may finish after final response; drain only events.
        while self.events:
            self.t = self.events[0][0]
            self.process_events()
        for s in self.rows.values():
            if s.row is not None and s.phase != "precharging":
                self.reads_per_act[s.reads] += 1
        total_bytes = sum(r.size for r in self.requests)
        active_grains = len({r.grain for r in self.requests})
        active_groups = len({self.group(r.grain) for r in self.requests})
        # Middle half of response events: separate sustained observation from cold start.
        events = self.completed_atom_events
        lo, hi = len(events) // 4, len(events) * 3 // 4
        middle_bw = None
        if hi > lo and events[hi][0] > events[lo][0]:
            middle_bw = (hi - lo) * c.atom_bytes / (events[hi][0] - events[lo][0])
        return {
            "config": asdict(c), "records": len(self.requests), "elapsed_ns": end,
            "useful_bytes": total_bytes, "wire_payload_bytes": total_bytes,
            "wire_ecc_crc_bytes": None, "metadata_energy_current": "not modeled",
            "lookups_per_s": len(self.requests) * 1e9 / end,
            "useful_GB_s": total_bytes / end, "middle_half_GB_s": middle_bw,
            "ACTs_per_lookup": self.stats["ACT"] / len(self.requests),
            "useful_bytes_per_ACT": total_bytes / self.stats["ACT"],
            "reads_per_ACT_histogram": dict(sorted(self.reads_per_act.items())),
            "row_bus_utilization": self.stats["row_bus_occupied_ns"] / (end * active_groups * c.row_lanes),
            "column_bus_utilization": self.stats["column_bus_occupied_ns"] / (end * active_groups),
            "output_utilization": self.stats["output_occupied_ns"] / (end * active_grains),
            "max_live_records": self.max_inflight, "max_outstanding_atoms": self.max_atom_queue,
            "mean_record_latency_ns": sum(self.latencies) / len(self.latencies),
            "stats": dict(self.stats), "wait_candidate_ns": dict(self.waits),
            "overlapping_wait_masks_candidate_ns": dict(self.wait_masks),
        }
