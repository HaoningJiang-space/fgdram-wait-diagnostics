"""Independent logical lifecycle/cache/coverage replay above the P0 auditor."""
from collections import Counter, OrderedDict, defaultdict

from .audit import AuditError, check_trace, require


def check_gather(timeline, config, requests, lookups, mapping, producer, groups):
    try:
        return _check(timeline, config, requests, lookups, mapping, producer, groups)
    except (KeyError, IndexError, TypeError, ValueError) as exc:
        raise AuditError(f"malformed gather trace: {exc}") from exc


def _check(timeline, c, requests, lookups, m, p, groups):
    physical = [e for e in timeline if not e["event"].startswith("L_")]
    physical_receipt = check_trace(physical, c, requests)
    logical = {r.lid: r for r in lookups}
    require(len(logical) == len(lookups) and logical, "logical input identities")
    physical_by_id = {r.rid: r for r in requests}
    expected = {}
    earlier = set()
    # Build coverage independently, from every requested virtual byte. Do not
    # call the front-end splitter, mapper or its cache/readiness helpers.
    for r in lookups:
        require(r.size > 0 and r.address >= 0 and set(r.depends) <= earlier,
                "logical range/dependencies")
        coverage = defaultdict(set)
        for v in range(r.address, r.address + r.size):
            page, col = divmod(v, c.row_bytes)
            if groups == 1:
                page = (page // 8) * c.grains + page % 8
            else:
                require(groups == c.grains // 8, "placement scope")
            address = page * c.row_bytes + col
            require(address < m.capacity, "coverage outside capacity")
            coverage[address // c.atom_bytes * c.atom_bytes].add(col % c.atom_bytes)
        expected[r.lid] = dict(coverage)
        earlier.add(r.lid)

    cache, misses = OrderedDict(), {}
    admitted, done, live = {}, {}, set()
    accessed, delivered, due = {}, set(), {}
    finished_physical, created = {}, {}
    fill_seen = set()
    waiting = defaultdict(list)
    access_ticks, admit_ticks = Counter(), Counter()
    routes, peaks, useful = Counter(), Counter(), defaultdict(set)
    last_t = -1
    next_index = 0
    for e in timeline:
        t, kind = e["time_ns"], e["event"]
        require(type(t) is int and t >= last_t and t % c.clock_ns == 0,
                "logical event chronology/grid")
        last_t = t
        if kind == "ADMIT":
            require(e["rid"] in created, "physical request before logical miss")
        elif kind == "COMPLETE":
            finished_physical[e["rid"]] = t
        elif not kind.startswith("L_"):
            continue
        elif kind == "L_ADMIT":
            lid = e["lid"]
            require(next_index < len(lookups) and lookups[next_index].lid == lid,
                    "logical producer order/duplicate")
            r = logical[lid]
            require(lid not in admitted and t >= r.release_ns and set(r.depends) <= done.keys(),
                    "logical release/dependencies")
            batch = next_index // p.batch_size
            previous = lookups[(batch-1)*p.batch_size:batch*p.batch_size] if batch else []
            require(all(x.lid in done for x in previous), "producer batch dependency")
            if previous:
                require(t >= max(done[x.lid] for x in previous) + p.batch_gap_ns,
                        "producer consumer gap")
            admitted[lid] = t
            live.add(lid)
            next_index += 1
            admit_ticks[t] += 1
            require(len(live) <= p.window and admit_ticks[t] <= p.records_per_tick,
                    "logical window/issue width")
            peaks["logical_live"] = max(peaks["logical_live"], len(live))
        elif kind == "L_ACCESS":
            lid, block, rid = e["lid"], e["block"], e["rid"]
            key = lid, block
            require(lid in live and key not in accessed, "fragment duplicate/visibility")
            require(block in expected[lid] and set(range(e["lo"], e["hi"])) == expected[lid][block],
                    "fragment byte coverage")
            access_ticks[t] += 1
            require(access_ticks[t] <= p.accesses_per_tick, "cache access issue width")
            route = e["route"]
            if block in cache:
                require(route == "hit" and rid == cache[block], "cache hit provenance")
                cache.move_to_end(block)
                due[key] = t + p.cache_hit_ns
            elif block in misses:
                require(route == "merge" and rid == misses[block], "miss coalescing provenance")
            else:
                require(route == "miss" and rid not in created and rid in physical_by_id,
                        "new miss provenance")
                r = physical_by_id[rid]
                n, col = divmod(block, c.row_bytes)
                n, grain = divmod(n, c.grains)
                n, pb = divmod(n, 2)
                row, sa = divmod(n, c.subarrays)
                if m.variant == "xor":
                    grain ^= (sa ^ row) & 7
                require((r.grain, r.pb, r.sa, r.row, r.offset, r.size, r.release_ns, r.depends) ==
                        (grain, pb, sa, row, col, c.atom_bytes, t, ()),
                        "physical address/extent/release mismatch")
                misses[block] = rid
                created[rid] = block
                require(len(misses) <= p.mshr_entries, "MSHR capacity")
                pending = len(created) - len(finished_physical)
                require(pending <= min(c.outstanding_records, c.atom_queue_capacity),
                        "pending physical request capacity")
                peaks["mshrs"] = max(peaks["mshrs"], len(misses))
            routes[route] += 1
            if route != "hit":
                useful[rid].update(expected[lid][block])
                waiting[rid].append(key)
            accessed[key] = dict(rid=rid, route=route)
        elif kind == "L_FILL":
            block, rid = e["block"], e["rid"]
            require(rid not in fill_seen and misses.get(block) == rid and
                    finished_physical.get(rid) == t, "fill before/mismatched physical return")
            fill_seen.add(rid)
            del misses[block]
            if p.cache_blocks:
                cache[block] = rid
                cache.move_to_end(block)
                if len(cache) > p.cache_blocks:
                    cache.popitem(last=False)
            for key in waiting.pop(rid):
                due[key] = t
        elif kind == "L_PART":
            key = e["lid"], e["block"]
            require(key in accessed and key not in delivered and e["lid"] in live,
                    "missing/duplicate logical fragment")
            require(e["rid"] == accessed[key]["rid"] and due.get(key) == t,
                    "fragment before return/cache latency")
            require(set(range(e["lo"], e["hi"])) == expected[key[0]][key[1]],
                    "returned byte coverage")
            delivered.add(key)
        elif kind == "L_COMPLETE":
            lid = e["lid"]
            require(lid in live and lid not in done, "logical completion identity")
            require(all((lid, block) in delivered for block in expected[lid]),
                    "logical completion before all fragments")
            require(t == max(due[lid, b] for b in expected[lid]), "logical completion time")
            done[lid] = t
            live.remove(lid)
        else:
            raise AuditError(f"unknown logical event {kind}")
    required_parts = {(lid, block) for lid, blocks in expected.items() for block in blocks}
    require(set(accessed) == delivered == required_parts, "missing logical coverage lifecycle")
    require(set(admitted) == set(done) == set(logical) and not live, "missing logical lifecycle")
    require(set(created) == fill_seen == set(physical_by_id) and not misses, "unmatched physical fill")
    return dict(pass_=True, contract="online-gather-v1", physical=physical_receipt,
                logical_lookups=len(done), physical_reads=len(requests),
                useful_dram_bytes=sum(map(len, useful.values())),
                logical_requested_bytes=sum(r.size for r in lookups),
                wire_payload_bytes=len(requests)*c.atom_bytes,
                routes=dict(routes), reconstructed_peaks=dict(peaks))
