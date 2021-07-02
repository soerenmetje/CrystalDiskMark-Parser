# Tests parsing
#
# author Soeren Metje
# created on 2021-06-23
import os
import unittest

from crystaldiskmark_parser.parser import parse, parse_df

THIS_DIR = os.path.dirname(os.path.abspath(__file__))  # needed access data from other scopes


class TestCaseParser(unittest.TestCase):
    def test_parse1(self):
        benchmark_result = parse(os.path.join(THIS_DIR, "data/CrystalDiskMark_20210622162528 WD Blue 3D 1TB WDS100T2B0A.txt"))

        self.assertEqual("Windows 10  [10.0 Build 19042] (x64)", benchmark_result.os)
        self.assertEqual("2021/06/22 17:19:21", benchmark_result.date)
        self.assertEqual("Measure 5 sec / Interval 5 sec", benchmark_result.time)
        self.assertEqual("[Admin]", benchmark_result.mode)
        self.assertEqual("1 GiB (x5) [E: 96% (894/932GiB)]", benchmark_result.test)
        self.assertEqual("Default", benchmark_result.profile)
        self.assertEqual("WD Blue 3D 1TB", benchmark_result.comment)

        self.assertEqual(4, len(benchmark_result.write_results))
        self.assertEqual(4, len(benchmark_result.read_results))

        self.assertEqual("SEQ", benchmark_result.read_results[0].test_type)
        self.assertAlmostEqual(1, benchmark_result.read_results[0].block_size, places=2)
        self.assertEqual(8, benchmark_result.read_results[0].queues)
        self.assertEqual(1, benchmark_result.read_results[0].threads)
        self.assertAlmostEqual(531.458, benchmark_result.read_results[0].rate, places=2)
        self.assertAlmostEqual(506.8, benchmark_result.read_results[0].iops, places=2)
        self.assertAlmostEqual(15726.77, benchmark_result.read_results[0].latency, places=2)

        self.assertEqual("RND", benchmark_result.read_results[2].test_type)
        self.assertAlmostEqual(4, benchmark_result.read_results[2].block_size, places=2)
        self.assertEqual(32, benchmark_result.read_results[2].queues)
        self.assertEqual(1, benchmark_result.read_results[2].threads)
        self.assertAlmostEqual(269.406, benchmark_result.read_results[2].rate, places=2)
        self.assertAlmostEqual(65772.9, benchmark_result.read_results[2].iops, places=2)
        self.assertAlmostEqual(470.58, benchmark_result.read_results[2].latency, places=2)

    def test_parse2(self):
        benchmark_result = parse(os.path.join(THIS_DIR, "data/CrystalDiskMark_20210622163451 SAMSUNG 840 EVO 120GB.txt"))

        self.assertEqual("Windows 10  [10.0 Build 19042] (x64)", benchmark_result.os)
        self.assertEqual("2021/06/22 16:34:54", benchmark_result.date)
        self.assertEqual("Measure 5 sec / Interval 5 sec", benchmark_result.time)
        self.assertEqual("[Admin]", benchmark_result.mode)
        self.assertEqual("1 GiB (x5) [C: 98% (109/111GiB)]", benchmark_result.test)
        self.assertEqual("Default", benchmark_result.profile)
        self.assertEqual("Samsung 840 EVO 120GB", benchmark_result.comment)
        self.assertEqual(4, len(benchmark_result.write_results))
        self.assertEqual(4, len(benchmark_result.read_results))
        # TODO add assert TestResults

    def test_parse3(self):
        benchmark_result = parse(os.path.join(THIS_DIR, "data/CrystalDiskMark_20210623000551.txt"))

        self.assertEqual("Windows 10  [10.0 Build 19042] (x64)", benchmark_result.os)
        self.assertEqual("2021/06/23 0:05:57", benchmark_result.date)
        self.assertEqual("Measure 5 sec / Interval 5 sec", benchmark_result.time)
        self.assertEqual("[Admin]", benchmark_result.mode)
        self.assertEqual("512 MiB (x2) [F: 78% (182/233GiB)]", benchmark_result.test)
        self.assertEqual("Default", benchmark_result.profile)
        self.assertIsNone(benchmark_result.comment)
        self.assertEqual(4, len(benchmark_result.write_results))
        self.assertEqual(4, len(benchmark_result.read_results))
        # TODO add assert TestResults

    def test_parse_df1(self):
        df = parse_df(os.path.join(THIS_DIR, "data/CrystalDiskMark_20210623000551.txt"))

        for e in df['test']:
            self.assertEqual("512 MiB (x2) [F: 78% (182/233GiB)]", e)
        for e in df['type']:
            self.assertIn(e, ["SEQ", "RND"])
        for e in df['read_write']:
            self.assertIn(e, ["read", "write"])

        for e in df.loc[(df['test'] == "512 MiB (x2) [F: 78% (182/233GiB)]")  # only 1 entry
                        & (df['read_write'] == 'read')
                        & (df['type'] == "SEQ")
                        & (df['queues'] == 8)]["blocksize"]:
            self.assertAlmostEqual(e, 1.0)

        for e in df.loc[(df['test'] == "512 MiB (x2) [F: 78% (182/233GiB)]")  # only 1 entry
                        & (df['read_write'] == 'read')
                        & (df['type'] == "SEQ")
                        & (df['queues'] == 1)]["blocksize"]:
            self.assertAlmostEqual(e, 1.0)

        for e in df.loc[(df['test'] == "512 MiB (x2) [F: 78% (182/233GiB)]")  # only 1 entry
                        & (df['read_write'] == 'read')
                        & (df['type'] == "SEQ")
                        & (df['queues'] == 8)]["rate"]:
            self.assertAlmostEqual(e, 550.022)

        for e in df.loc[(df['test'] == "512 MiB (x2) [F: 78% (182/233GiB)]")  # only 1 entry
                        & (df['read_write'] == 'read')
                        & (df['type'] == "SEQ")
                        & (df['queues'] == 1)]["rate"]:
            self.assertAlmostEqual(e, 494.879)

        for e in df.loc[(df['test'] == "512 MiB (x2) [F: 78% (182/233GiB)]")  # only 1 entry
                        & (df['read_write'] == 'read')
                        & (df['type'] == "RND")
                        & (df['queues'] == 32)]["rate"]:
            self.assertAlmostEqual(e, 258.572)

        for e in df.loc[(df['test'] == "512 MiB (x2) [F: 78% (182/233GiB)]")  # only 1 entry
                        & (df['read_write'] == 'read')
                        & (df['type'] == "RND")
                        & (df['queues'] == 1)]["rate"]:
            self.assertAlmostEqual(e, 46.627)

        for e in df.loc[(df['test'] == "512 MiB (x2) [F: 78% (182/233GiB)]")  # only 1 entry
                        & (df['read_write'] == 'read')
                        & (df['type'] == "RND")
                        & (df['queues'] == 1)]["iops"]:
            self.assertAlmostEqual(e, 11383.5)

        for e in df.loc[(df['test'] == "512 MiB (x2) [F: 78% (182/233GiB)]")  # only 1 entry
                        & (df['read_write'] == 'read')
                        & (df['type'] == "RND")
                        & (df['queues'] == 1)]["latency"]:
            self.assertAlmostEqual(e, 87.50)

        for e in df.loc[(df['test'] == "512 MiB (x2) [F: 78% (182/233GiB)]")  # only 1 entry
                        & (df['read_write'] == 'write')
                        & (df['type'] == "SEQ")
                        & (df['queues'] == 8)]["rate"]:
            self.assertAlmostEqual(e, 513.437)

        for e in df.loc[(df['test'] == "512 MiB (x2) [F: 78% (182/233GiB)]")  # only 1 entry
                        & (df['read_write'] == 'write')
                        & (df['type'] == "RND")
                        & (df['queues'] == 32)]["rate"]:
            self.assertAlmostEqual(e, 237.469)

        for e in df.loc[(df['test'] == "512 MiB (x2) [F: 78% (182/233GiB)]")]["os"]:
            self.assertEqual("Windows 10  [10.0 Build 19042] (x64)", e)


if __name__ == '__main__':
    unittest.main()
