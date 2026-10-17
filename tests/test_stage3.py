from dataclasses import asdict
import importlib.util
from pathlib import Path
import tempfile
import unittest

from fgdram.index_input import read_indices, provenance

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('stage3',ROOT/'scripts/run_stage3_gather.py')
stage3=importlib.util.module_from_spec(spec);spec.loader.exec_module(stage3)


class Stage3Tests(unittest.TestCase):
    def test_design_only_changes_declared_hardware_and_producer(self):
        cases=stage3.cases()
        self.assertEqual(len(cases),38)
        self.assertEqual(sum(x['scenario']=='cache_fit' for x in cases),2)
        baseline={}
        for case in cases:
            c,p=stage3.settings(case)
            self.assertEqual(c.trtp_ns,16)
            self.assertEqual(c.trcd_ns,16)
            self.assertEqual(c.grains,128)
            self.assertEqual(c.power_scope,'die')
            self.assertEqual(c.outstanding_records,512)
            key=(case['source'],case['mapping'],case['scenario'])
            d=asdict(c);d.pop('row_lanes');d.pop('return_slots_per_grain')
            if key not in baseline:
                baseline[key]=(d,asdict(p))
            self.assertEqual(baseline[key],(d,asdict(p)))

    def test_dataset_is_hash_pinned_and_width_is_not_truncated(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'source';path.write_text('1\t2\t3\t4\n')
            with self.assertRaisesRegex(ValueError,'hash mismatch'):
                read_indices(path)
        info=provenance()
        self.assertEqual(info['record_bytes'],120)
        self.assertEqual(info['table_bytes'],201840)
        self.assertIn('not a measured',info['boundary'])


if __name__=='__main__':
    unittest.main()
