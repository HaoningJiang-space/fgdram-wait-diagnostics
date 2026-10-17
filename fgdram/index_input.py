"""One real index source and a size-matched synthetic control, without weights."""
import hashlib
from pathlib import Path
import random
import urllib.request

from .frontend import Lookup

DATA_URL = "https://files.grouplens.org/datasets/movielens/ml-100k/u.data"
DATA_SHA256 = "06416e597f82b7342361e41163890c81036900f418ad91315590814211dca490"
TERMS_URL = "https://files.grouplens.org/datasets/movielens/ml-100k-README.txt"
MODEL_URL = "https://d2l.ai/chapter_recommender-systems/mf.html"
ITEMS = 1682
DIMENSION = 30
RECORD_BYTES = DIMENSION * 4
SEED = 20261017


def read_indices(path, count=1024, download=False):
    path = Path(path)
    if not path.exists() and download:
        path.parent.mkdir(parents=True, exist_ok=True)
        with urllib.request.urlopen(DATA_URL, timeout=45) as response:
            data = response.read()
        if hashlib.sha256(data).hexdigest() != DATA_SHA256:
            raise ValueError("downloaded index source hash mismatch")
        path.write_bytes(data)
    data = path.read_bytes()
    if hashlib.sha256(data).hexdigest() != DATA_SHA256:
        raise ValueError("index source hash mismatch")
    lines = data.decode('ascii').splitlines()
    if len(lines) != 100000 or not 0 < count <= len(lines):
        raise ValueError("source size or requested prefix")
    indices = [int(line.split('\t')[1])-1 for line in lines[:count]]
    if not all(0 <= x < ITEMS for x in indices):
        raise ValueError("source item domain")
    return indices


def inputs(path, count=1024, download=False):
    indices = read_indices(path, count, download)
    rng = random.Random(SEED)
    uniform = [rng.randrange(ITEMS) for _ in range(count)]
    return {name: [Lookup(i, index*RECORD_BYTES, RECORD_BYTES) for i,index in enumerate(ids)]
            for name,ids in (("movielens",indices),("uniform",uniform))}


def provenance():
    return dict(index_source=DATA_URL, source_sha256=DATA_SHA256,
                terms=TERMS_URL, model_dimension_source=MODEL_URL,
                citation="F. Maxwell Harper and Joseph A. Konstan (2015). The MovieLens Datasets: "
                         "History and Context. ACM TiiS 5(4), article 19. doi:10.1145/2827872",
                extraction="First 1024 lines in published u.data file order, item column minus one. "
                           "File order is randomized by source; timestamps are not hardware releases.",
                item_rows=ITEMS, dimension=DIMENSION, representation="FP32 assumption; complete item vector",
                record_bytes=RECORD_BYTES, table_bytes=ITEMS*RECORD_BYTES,
                layout="dense row-major, no per-record padding; page placement declared per run",
                synthetic_control_seed=SEED,
                boundary="Read-only item embedding gather generated from real indices, not a measured "
                         "DRAM trace or full MF/DLRM. User vectors, bias loads, arithmetic, training and "
                         "accuracy are outside this kernel.",
                redistribution="Source terms prohibit redistribution without separate permission. "
                               "Raw indices and reversible address traces remain local and ignored; "
                               "public artifacts contain aggregates, hashes and redacted event examples.")
