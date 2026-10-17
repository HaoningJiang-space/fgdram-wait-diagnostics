"""Bounded offline scheduling diagnostics, never the production policy.

Search compares concrete legal finite schedules; it does not certify optimality.
The periodic witness has an explicit analytic phase pattern and keeps all timing,
queue, decoder and activation rules in the ordinary simulator and trace audit.
"""
from collections import Counter

from .model import Simulator


class PrioritySimulator(Simulator):
    def __init__(self, *args, grain_order, mode='oldest', **kwargs):
        super().__init__(*args, **kwargs)
        self.rank = {g: i for i, g in enumerate(grain_order)}
        self.mode = mode

    def order_candidates(self, candidates):
        def key(v):
            kind, g, p, rid = v
            family = {'RD': 0, 'PRE': 1, 'ACT': 2}[kind]
            if self.mode == 'grain':
                return family, self.rank[g], rid
            if self.mode == 'deadline':
                # Prefer the grain whose previous source operation finishes first.
                return family, self.source_busy[g], self.rank[g], rid
            if self.mode == 'round':
                r = self.live[rid].request
                return family, r.row, r.pb, self.rank[g], rid
            return family, rid
        return sorted(candidates, key=key)


class PeriodicWitness(Simulator):
    """ACT accepts 4+phase[g]+32*pb+64*row; READ accepts ACT+16/+32.

    Balanced 64 B only, 4 ns ACT, legacy timings, auto PRE. Two-grain phases
    (0,4) fit one lane; eight-grain phases (0,2,...14) need two lanes.
    """
    def __init__(self, *args, phases, **kwargs):
        super().__init__(*args, **kwargs)
        self.phases = phases
        c = self.c
        if not (c.act_command_ns == 4 and c.read_command_ns == 2 and c.trcd_ns == 16
                and c.source_ns == c.trtp_ns == c.trp_ns == c.burst_ns == c.tcl_ns == 16
                and c.auto_precharge and all(r.size == 64 for r in self.requests)):
            raise ValueError('periodic witness requires the stated legacy 64 B contract')

    def order_candidates(self, candidates):
        due = []
        for v in candidates:
            kind, g, p, rid = v
            r = self.live[rid].request
            a = 4 + self.phases[g] + 32*p + 64*r.row
            if kind == 'ACT':
                dispatch = a - self.c.act_command_ns
            elif kind == 'RD':
                dispatch = a + 16 + 16*self.live[rid].issued - self.c.read_command_ns
            else:
                continue
            if dispatch == self.t:
                due.append(v)
        return super().order_candidates(due)


def finite_lower_bound(config, requests):
    """Necessary bound only, assumes one new activation per record as in balanced()."""
    c = config
    atoms = Counter()
    groups = Counter()
    for r in requests:
        atoms[r.grain] += r.size // c.atom_bytes
        groups[r.grain//c.grains_per_group] += 1
    first_output = c.act_command_ns + c.trcd_ns + max(c.tcl_ns, c.source_ns)
    output_bound = first_output + max(atoms.values())*c.burst_ns
    # The last ACT cannot finish before all ACT transmission work on its group.
    last_act = max((n+c.row_lanes-1)//c.row_lanes*c.act_command_ns for n in groups.values())
    tail = c.trcd_ns + max(c.tcl_ns, c.source_ns) + c.burst_ns
    return max(output_bound, last_act+tail)


def periodic_boundary_certificate(trace, start=128, period=64, records_per_period=16):
    """Compare two full command/data periods, including carry-over operations.

    Equal event streams alone are insufficient: also compare all intervals
    crossing each boundary with relative start/end and normalized identities.
    Input ADMIT/COMPLETE are finite-driver bookkeeping, not hardware period state.
    """
    kinds = {'COMMAND','ACT_ACCEPT','READ_ACCEPT','DATA_READY','DATA_START','DATA_END','PRE_START','PRE_END'}
    def normalize(e, boundary):
        d = dict(e)
        for key in ('time_ns','accept_ns','ready_ns','end_ns'):
            if key in d:
                d[key] -= boundary
        for key in ('command_id','activation_id','row'):
            d.pop(key, None)
        if 'rid' in d:
            d['rid'] %= records_per_period
        return d
    def events(boundary):
        return [normalize(e, boundary) for e in trace if e['event'] in kinds and
                boundary <= e['time_ns'] < boundary+period]
    commands = {e['command_id']:e for e in trace if e['event']=='COMMAND'}
    acts = {e['activation_id']:e for e in trace if e['event']=='ACT_ACCEPT'}
    spans = []
    for e in trace:
        k = e['event']
        if k == 'COMMAND':
            spans.append(('command',e['grain'],e['lane'],e['time_ns'],e['accept_ns']))
            if e['command'] in ('ACT','PRE'):
                spans.append(('decoder',e['grain']//2,0,e['accept_ns'],e['accept_ns']+2))
        elif k == 'READ_ACCEPT':
            spans.append(('source',e['grain'],0,e['time_ns'],e['time_ns']+16))
        elif k == 'DATA_START':
            spans.append(('port',e['port'],0,e['time_ns'],e['end_ns']))
        elif k == 'PRE_END':
            a=acts[e['activation_id']]
            spans.append(('row',e['grain'],e['pb'],a['time_ns'],e['time_ns']))
        elif k == 'DATA_END':
            d=commands[e['command_id']]
            spans.append(('reservation',e['grain'],0,d['time_ns'],e['time_ns']))
    def state(b):
        crossing = sorted((k,g,p,a-b,z-b) for k,g,p,a,z in spans if a < b < z)
        history = sorted((e['grain'],e['time_ns']-b) for e in acts.values() if b-12 < e['time_ns'] < b)
        # Include ready/accepted data pending output, plus full remaining events of
        # each command already dispatched but not fully retired across boundary.
        return dict(crossing_intervals=crossing, activation_window_history=history)
    return dict(period_ns=period, boundaries=[start,start+period],
                event_streams_equal=events(start)==events(start+period),
                carried_resource_state_equal=state(start)==state(start+period),
                first_state=state(start), second_state=state(start+period),
                scope='hardware period of finite legal witness; input queues are not periodic',
                records_per_period=records_per_period)
