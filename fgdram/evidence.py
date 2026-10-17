"""Trace-derived timing/credit evidence; no change to the legacy simulation.

The optional beat annotation is an explicit uniform serialization witness. It is
not a claim that paper tCL measures the internal first/last-section arrival.
"""
from collections import Counter, defaultdict
import gzip
import hashlib
import json

from .audit import check_trace, require

LEGACY_CONTRACT = "legacy_0270afa"
ANNOTATION_CONTRACT = "legacy_atom_annotation_v1"


def json_hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def credit_intervals(trace):
    commands = {e['command_id']: e for e in trace if e['event'] == 'COMMAND'}
    return [dict(rid=e['rid'], atom=e['atom'], grain=e['grain'], command_id=e['command_id'],
                 reserve_ns=commands[e['command_id']]['time_ns'], release_ns=e['time_ns'])
            for e in trace if e['event'] == 'DATA_END']


def annotate_atoms(trace, config, requests):
    """Produce eight 4-byte section endpoints for an *assumed* atomic witness.

    Sections uniformly complete during [READ, READ+source]. Legacy output still
    waits for the entire atom and tCL. The physical internal arrival is unknown;
    this witness proves byte conservation for this interpretation only.
    """
    check_trace(trace, config, requests)
    c = config
    require(c.atom_bytes == 32 and c.source_ns % 8 == 0 and c.burst_ns % 8 == 0,
            'annotation requires integral eight-section timing')
    commands = {e['command_id']: e for e in trace if e['event'] == 'COMMAND'}
    starts = {e['command_id']: e for e in trace if e['event'] == 'DATA_START'}
    closes = {e['activation_id']: e for e in trace if e['event'] == 'PRE_START'}
    result = []
    for read in (e for e in trace if e['event'] == 'READ_ACCEPT'):
        cid, r = read['command_id'], read['time_ns']
        start, cmd = starts[cid], commands[cid]
        pre = closes.get(read['activation_id'])
        result.append(dict(
            contract=ANNOTATION_CONTRACT, timing_contract=LEGACY_CONTRACT,
            command_id=cid, rid=read['rid'], atom=read['atom'], grain=read['grain'],
            activation_id=read['activation_id'], column_offset=read['column_offset'],
            read_dispatch_ns=cmd['time_ns'], read_accept_ns=r,
            source_interval_ns=[r, r + c.source_ns],
            assumed_internal_section_end_ns=[r + (i + 1) * c.source_ns // 8 for i in range(8)],
            physical_internal_first_arrival_ns=None, physical_internal_last_arrival_ns=None,
            last_column_dependency_end_ns=r + max(c.source_ns, c.trtp_ns),
            atom_eligible_ns=r + max(c.source_ns, c.tcl_ns),
            external_first_start_ns=start['time_ns'],
            external_section_end_ns=[start['time_ns'] + (i + 1) * c.burst_ns // 8 for i in range(8)],
            external_last_end_ns=start['end_ns'], pre_start_ns=pre['time_ns'] if pre else None,
            credit_reserve_ns=cmd['time_ns'], credit_release_ns=start['end_ns'],
            physical_timing_status='unknown; uniform internal arrivals are an annotation assumption'))
    check_annotations(result, c)
    return result


def check_annotations(atoms, config):
    """Byte and reservation conservation; fail even if an annotation is corrupted."""
    occupancy = defaultdict(list)
    credits = defaultdict(list)
    identities = set()
    for a in atoms:
        key = a['rid'], a['atom']
        require(key not in identities, 'duplicate annotated atom')
        identities.add(key)
        internal, external = a['assumed_internal_section_end_ns'], a['external_section_end_ns']
        require(len(internal) == len(external) == 8, 'section count')
        r = a['read_accept_ns']
        require(internal == [r + (i + 1) * config.source_ns // 8 for i in range(8)], 'internal witness times')
        require(external == [a['external_first_start_ns'] + (i + 1) * config.burst_ns // 8
                            for i in range(8)], 'external witness times')
        require(a['external_first_start_ns'] >= a['atom_eligible_ns'] >= internal[-1], 'early output')
        require(a['credit_reserve_ns'] <= internal[0] and
                a['credit_release_ns'] == external[-1] == a['external_last_end_ns'], 'credit lifetime')
        require(a['last_column_dependency_end_ns'] == r + max(config.source_ns, config.trtp_ns),
                'column dependency')
        require(a['pre_start_ns'] is None or a['pre_start_ns'] >= a['last_column_dependency_end_ns'],
                'early close')
        for t in internal:
            occupancy[a['grain']].append((t, 4))
        for t in external:
            occupancy[a['grain']].append((t, -4))
        credits[a['grain']].extend(((a['credit_reserve_ns'], 1), (a['credit_release_ns'], -1)))
    peak_bytes, peak_credits = {}, {}
    for g in occupancy:
        value = peak = 0
        # Equal-time arrival before departure is a conservative section-buffer convention.
        for _, delta in sorted(occupancy[g], key=lambda x: (x[0], -x[1])):
            value += delta
            require(0 <= value <= config.return_slots_per_grain * config.atom_bytes, 'section storage')
            peak = max(peak, value)
        require(value == 0, 'unreturned bytes')
        peak_bytes[g] = peak
        value = peak = 0
        for _, delta in sorted(credits[g]):  # release before reserve, half-open intervals
            value += delta
            require(0 <= value <= config.return_slots_per_grain, 'reservation storage')
            peak = max(peak, value)
        require(value == 0, 'unreleased annotation credits')
        peak_credits[g] = peak
    return dict(pass_=True, peak_assumed_buffer_bytes=peak_bytes, peak_reserved_slots=peak_credits)


def idle_intervals(spans, end):
    """Complement of occupied half-open spans, clipped to [0, end)."""
    idle, cursor = [], 0
    for a, b in sorted(spans):
        a, b = max(0, a), min(end, b)
        if b <= a:
            continue
        if a > cursor:
            idle.append([cursor, a])
        cursor = max(cursor, b)
    if cursor < end:
        idle.append([cursor, end])
    return idle


def metrics(trace, config, requests):
    audit = check_trace(trace, config, requests)
    end = max(e['time_ns'] for e in trace if e['event'] == 'COMPLETE')
    # Predeclared rule: central half of elapsed *time*, [T/4, 3T/4).
    # Integer multiplication avoids rounding and includes/excludes all ties alike.
    ends = [e for e in trace if e['event'] == 'DATA_END']
    middle = [e for e in ends if end <= 4 * e['time_ns'] < 3 * end]
    reads, acts, source, ports = Counter(), {}, defaultdict(list), defaultdict(list)
    for e in trace:
        if e['event'] == 'ACT_ACCEPT':
            acts[e['activation_id']] = e
        elif e['event'] == 'READ_ACCEPT':
            reads[e['activation_id']] += 1
            source[e['grain']].append([e['time_ns'], e['time_ns'] + config.source_ns])
        elif e['event'] == 'DATA_START':
            ports[e['port']].append([e['time_ns'], e['end_ns']])
    histogram = Counter(reads[a] for a in acts)
    live_grains = range(config.grains)  # retain idle and borrowed-output grains too
    idle = {str(g): dict(source=idle_intervals(source[g], end),
                        output=idle_intervals(ports[g], end)) for g in live_grains}
    return dict(timing_contract=LEGACY_CONTRACT, independent_trace_audit=audit,
                elapsed_ns=end, time_window=dict(start_ns=end/4, end_ns=3*end/4,
                convention='[start,end); all equal timestamps treated together', returns=len(middle)),
                middle_time_GB_s=len(middle)*config.atom_bytes/(end/2),
                ACTs_per_lookup=len(acts)/len(requests), reads_per_ACT_histogram=dict(histogram),
                useful_bytes=sum(r.size for r in requests), wire_payload_bytes=len(ends)*config.atom_bytes,
                wire_ecc_crc_bytes=None, per_grain_idle=idle, return_reservations=credit_intervals(trace))


class CandidateRecorder:
    """Stream every candidate/mask/decision to gzip; bounded examples in memory."""
    def __init__(self, path, clock_ns):
        self.file = gzip.open(path, 'wt', encoding='utf-8')
        self.clock = clock_ns
        self.count = 0
        self.masks, self.reasons = Counter(), Counter()
        self.examples = {}

    def __call__(self, time, snapshot, issued):
        for candidate, blockers in snapshot:
            blockers = list(blockers)
            if not blockers and candidate not in issued:
                blockers = ['scheduler_arbitration']
            row = dict(time_ns=time, candidate=candidate, blockers=blockers, issued=candidate in issued)
            self.file.write(json.dumps(row, separators=(',', ':')) + '\n')
            self.count += 1
            mask = '+'.join(blockers) or 'eligible_and_issued'
            self.masks[mask] += self.clock
            for reason in blockers:
                self.reasons[reason] += self.clock
                self.examples.setdefault(reason, row)

    def close(self):
        self.file.close()
        return dict(candidate_observations=self.count, all_candidate_masks_ns=dict(self.masks),
                    wait_candidate_ns=dict(self.reasons), examples=self.examples,
                    trace_format='JSONL gzip; every candidate at every scheduler tick, no truncation')
