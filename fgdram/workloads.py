from .model import Request


def balanced(size, records=1024, grains=8, serial=False):
    """Deterministic new-row records; exact same physical starts for 32/64 B.

    Different pseudobanks use different even subarrays, avoiding the deliberately
    tested physical-bank conflict. No claim of real application distribution.
    """
    result = []
    for i in range(records):
        g = i % grains
        lap = i // grains
        p = lap % 2
        result.append(Request(i, g, p, 2 * (g % 2 * 2 + p), lap // 2,
                              size, depends=(i - 1,) if serial and i else ()))
    return result


def stream(records=32):
    return [Request(i, 0, 0, 0, i // 8, 32, (i % 8) * 32) for i in range(records)]
