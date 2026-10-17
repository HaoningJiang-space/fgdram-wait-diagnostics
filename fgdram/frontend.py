"""Online byte-addressed gather over the unchanged legacy DRAM timing contract.

This is a trace generator, not a processor or a calibrated cache hierarchy.
Logical records, cache blocks, physical READs and activations have separate IDs.
"""
from collections import Counter, OrderedDict
from dataclasses import dataclass
import heapq

from .model import Request, Simulator


@dataclass(frozen=True)
class Lookup:
    lid: int
    address: int
    size: int
    release_ns: int = 0
    depends: tuple[int, ...] = ()


@dataclass(frozen=True)
class AddressMap:
    """A bijection on a fixed 2 GiB illustrative physical address space.

    Low to high: column, grain, pseudobank, subarray, row. XOR changes only
    grain-within-group using high address bits; no data replication or capacity
    change. Placement chooses physical pages separately from this permutation.
    """
    variant: str = "linear"
    grains: int = 128
    subarrays: int = 32
    rows: int = 1024
    row_bytes: int = 256
    atom_bytes: int = 32

    def __post_init__(self):
        if (self.variant not in ("linear", "xor") or self.grains % 8 or
                self.row_bytes % self.atom_bytes or self.rows <= 0):
            raise ValueError("invalid address map")

    @property
    def capacity(self):
        return self.grains * 2 * self.subarrays * self.rows * self.row_bytes

    def decode(self, address):
        if not 0 <= address < self.capacity:
            raise ValueError("address outside modeled capacity")
        n, column = divmod(address, self.row_bytes)
        n, grain = divmod(n, self.grains)
        n, pb = divmod(n, 2)
        row, sa = divmod(n, self.subarrays)
        if self.variant == "xor":
            grain ^= (sa ^ row) & 7
        return grain, pb, sa, row, column

    def encode(self, grain, pb, sa, row, column):
        if not (0 <= grain < self.grains and 0 <= pb < 2 and
                0 <= sa < self.subarrays and 0 <= row < self.rows and
                0 <= column < self.row_bytes):
            raise ValueError("invalid physical address")
        if self.variant == "xor":
            grain ^= (sa ^ row) & 7
        return ((((row * self.subarrays + sa) * 2 + pb) * self.grains + grain)
                * self.row_bytes + column)

    def place(self, byte_address, groups):
        """Page allocation: restrict to group zero, or stripe over the full die.

        Virtual table bytes remain compact. Single-group placement skips pages
        allocated to other groups, without shrinking the physical hardware.
        """
        if groups not in (1, self.grains // 8):
            raise ValueError("placement must use one group or all groups")
        page, column = divmod(byte_address, self.row_bytes)
        if groups == 1:
            page = (page // 8) * self.grains + page % 8
        address = page * self.row_bytes + column
        self.decode(address)
        return address

    def fragments(self, lookup, groups):
        if lookup.address < 0 or lookup.size <= 0:
            raise ValueError("invalid logical range")
        result = []
        end = lookup.address + lookup.size
        for virtual_block in range(lookup.address // self.atom_bytes,
                                   (end - 1) // self.atom_bytes + 1):
            start = virtual_block * self.atom_bytes
            block = self.place(start, groups)
            result.append((block, max(start, lookup.address) - start,
                           min(start + self.atom_bytes, end) - start))
        return result


@dataclass(frozen=True)
class Producer:
    window: int = 32
    records_per_tick: int = 4
    accesses_per_tick: int = 8
    cache_blocks: int = 256       # 8 KiB, cold initially; an explicit assumption
    cache_hit_ns: int = 2
    mshr_entries: int = 128       # includes misses pending DRAM admission
    batch_size: int = 64
    batch_gap_ns: int = 16

    def __post_init__(self):
        if min(self.window, self.records_per_tick, self.accesses_per_tick,
               self.mshr_entries, self.batch_size) <= 0 or min(
                self.cache_blocks, self.cache_hit_ns, self.batch_gap_ns) < 0:
            raise ValueError("invalid producer capacity or latency")


class Gather:
    def __init__(self, config, lookups, mapping=None, producer=None, groups=1,
                 candidate_observer=None):
        self.mapping = mapping or AddressMap()
        self.p = producer or Producer()
        self.lookups = list(lookups)
        self.groups = groups
        if not self.lookups:
            raise ValueError("empty logical input")
        if (config.grains, config.subarrays, config.row_bytes, config.atom_bytes) != (
                self.mapping.grains, self.mapping.subarrays, self.mapping.row_bytes,
                self.mapping.atom_bytes):
            raise ValueError("mapping/topology mismatch")
        if (self.p.cache_hit_ns < config.clock_ns or self.p.cache_hit_ns % config.clock_ns or
                self.p.batch_gap_ns % config.clock_ns):
            raise ValueError("producer timing must be on the scheduler grid")
        earlier = set()
        for r in self.lookups:
            if r.lid in earlier or not set(r.depends) <= earlier or r.release_ns < 0:
                raise ValueError("logical identity/dependencies/release")
            self.mapping.fragments(r, groups)  # validates full address extent
            earlier.add(r.lid)
        self.timeline = []
        timeline = self.timeline

        class Memory(Simulator):
            def log(mem, event, **data):
                super().log(event, **data)
                timeline.append(mem.trace[-1])

        self.memory = Memory(config, [], streaming=True,
                             candidate_observer=candidate_observer)
        self.cache = OrderedDict()  # block -> completed physical request ID
        self.misses = {}            # block -> {rid, waiters}
        self.rid_block = {}
        self.live = {}
        self.done = {}
        self.hit_events = []
        self.next_lookup = 0
        self.trace_cursor = 0
        self.stats = Counter()

    def log(self, event, **fields):
        self.timeline.append(dict(time_ns=self.memory.t, event="L_" + event, **fields))

    def part(self, lid, block, lo, hi, rid):
        state = self.live[lid]
        self.log("PART", lid=lid, block=block, lo=lo, hi=hi, rid=rid)
        state["remaining"] -= 1
        if state["remaining"] == 0:
            self.done[lid] = self.memory.t
            self.log("COMPLETE", lid=lid)
            del self.live[lid]

    def before_admit(self):
        mem, p, t = self.memory, self.p, self.memory.t
        for event in mem.trace[self.trace_cursor:]:
            if event["event"] != "COMPLETE":
                continue
            rid = event["rid"]
            block = self.rid_block[rid]
            miss = self.misses.pop(block)
            self.log("FILL", block=block, rid=rid)
            if p.cache_blocks:
                self.cache[block] = rid
                self.cache.move_to_end(block)
                if len(self.cache) > p.cache_blocks:
                    self.cache.popitem(last=False)
            for lid, lo, hi in miss["waiters"]:
                self.part(lid, block, lo, hi, rid)
        self.trace_cursor = len(mem.trace)
        while self.hit_events and self.hit_events[0][0] <= t:
            _, lid, block, lo, hi, rid = heapq.heappop(self.hit_events)
            self.part(lid, block, lo, hi, rid)

        # A finite FIFO producer. Each next batch waits for the preceding batch
        # and a declared consumer gap; no old-machine timestamps are replayed.
        admitted = 0
        while self.next_lookup < len(self.lookups) and admitted < p.records_per_tick:
            r = self.lookups[self.next_lookup]
            batch = self.next_lookup // p.batch_size
            previous = self.lookups[(batch-1)*p.batch_size:batch*p.batch_size] if batch else []
            if len(self.live) >= p.window:
                self.stats["logical_window_full_ns"] += mem.c.clock_ns
                break
            if (r.release_ns > t or any(d not in self.done for d in r.depends) or
                    any(x.lid not in self.done for x in previous) or
                    (previous and t < max(self.done[x.lid] for x in previous) + p.batch_gap_ns)):
                self.stats["producer_dependency_release_or_gap_ns"] += mem.c.clock_ns
                break
            fragments = self.mapping.fragments(r, self.groups)
            self.live[r.lid] = dict(todo=list(fragments), remaining=len(fragments))
            self.log("ADMIT", lid=r.lid)
            self.next_lookup += 1
            admitted += 1
            self.stats["peak_logical_live"] = max(self.stats["peak_logical_live"], len(self.live))

        accessed = 0
        for lid, state in list(self.live.items()):
            for block, lo, hi in list(state["todo"]):
                if accessed >= p.accesses_per_tick:
                    return
                if block in self.cache:
                    rid = self.cache[block]
                    self.cache.move_to_end(block)
                    route = "hit"
                elif block in self.misses:
                    rid = self.misses[block]["rid"]
                    route = "merge"
                else:
                    if len(self.misses) >= p.mshr_entries:
                        self.stats["mshr_blocked_fragment_ns"] += mem.c.clock_ns
                        continue
                    if (len(mem.requests) - len(mem.completed) >= mem.c.outstanding_records or
                            len(mem.requests) - len(mem.completed) >= mem.c.atom_queue_capacity):
                        self.stats["physical_queue_blocked_fragment_ns"] += mem.c.clock_ns
                        continue
                    rid = len(mem.requests)
                    g, pb, sa, row, col = self.mapping.decode(block)
                    mem.requests.append(Request(rid, g, pb, sa, row, mem.c.atom_bytes,
                                                offset=col, release_ns=t))
                    self.rid_block[rid] = block
                    self.misses[block] = dict(rid=rid, waiters=[])
                    route = "miss"
                state["todo"].remove((block, lo, hi))
                self.log("ACCESS", lid=lid, block=block, lo=lo, hi=hi, rid=rid, route=route)
                self.stats[route + "_fragments"] += 1
                accessed += 1
                if route == "hit":
                    heapq.heappush(self.hit_events, (t+p.cache_hit_ns, lid, block, lo, hi, rid))
                else:
                    self.misses[block]["waiters"].append((lid, lo, hi))
                self.stats["peak_mshrs"] = max(self.stats["peak_mshrs"], len(self.misses))

    def run(self):
        while len(self.done) != len(self.lookups):
            self.memory.step(self.before_admit)
        end = max(self.done.values())
        while self.memory.events:
            self.memory.t = self.memory.events[0][0]
            self.memory.process_events()
        return dict(elapsed_ns=end, logical_lookups=len(self.lookups),
                    physical_requests=len(self.memory.requests),
                    logical_requested_bytes=sum(r.size for r in self.lookups),
                    wire_payload_bytes=len(self.memory.requests)*self.memory.c.atom_bytes,
                    stats=dict(self.stats))
