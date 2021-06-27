# Parse CrystalDiskMark text files
#
# author Soeren Metje
# created on 2021-06-23


import re
from typing import List
import pandas as pd

# regex to match lines
# use https://regex101.com to visualise regex
rx_dict = {
    'test_res': re.compile(
        r"\s*(?P<type>SEQ|RND)\s*(?P<blocksize>\d+)(?P<ublocksize>\S+)\s*\(Q=\s*(?P<queue>\d+), T=\s*(?P<threads>\d+)\):\s*(?P<rate>[\d\.\,]+) (?P<urate>\S+)\s*\[\s*(?P<iops>[\d\.\,]+) (?P<uiops>\S+)\]\s*<\s*(?P<us>[\d\.\,]+) (?P<uus>\S+)>\s*\n"),

    'read_or_write': re.compile(
        r"\s*\[(?P<read_or_write>Read|Write)\]\s*"),

    'profile': re.compile(
        r"\s*Profile: (?P<profile>.+)\s*"),

    'test': re.compile(
        r"\s*Test: (?P<test>.+)\s*"),

    'mode': re.compile(
        r"\s*Mode: (?P<mode>.+)\s*"),

    'time': re.compile(
        r"\s*Time: (?P<time>.+)\s*"),

    'date': re.compile(
        r"\s*Date: (?P<date>.+)\s*"),

    'os': re.compile(
        r"\s*OS: (?P<os>.+)\s*"),

    'comment': re.compile(
        r"\s*Comment: (?P<comment>.+)\s*"),

}


class BenchmarkResult:
    """
    Contains information about the performed benchmark.
    This includes multiple test results (TestResult) for read and write
    """

    def __init__(self) -> None:
        super().__init__()
        self.test = None
        self.date = None
        self.os = None
        self.profile = None
        self.time = None
        self.mode = None
        self.comment = None
        self.write_results: List[TestResult] = []
        self.read_results: List[TestResult] = []

    def __repr__(self):
        return "BenchmarkResult({!r})".format(self.__dict__)


class TestResult:
    """
    Contains information about the performed test.
    This includes the test type (sequential or random), block size, average read or write rate, IOPS, and latency
    """

    def __init__(self) -> None:
        super().__init__()
        self.test_type = None
        self.block_size = None
        self.unit_block_size = None
        self.queues = None
        self.threads = None
        self.rate = None
        self.unit_rate = None
        self.iops = None
        self.unit_iops = None
        self.latency = None
        self.unit_latency = None

    def __repr__(self):
        return "TestResult({!r})".format(self.__dict__)


def __parse_line(line):
    """
    Do a regex search against all defined regexes and
    return the key and match results of the first matching regex


    :param str line: line to be parsed
    :returns:
        - key (str) - key of regex that matched passed line
        - match_results (str) - matched groups
    """

    for key, rx in rx_dict.items():
        match = rx.search(line)
        if match:
            return key, match
    # if there are no matches
    return None, None


def parse_df(filepath) -> pd.DataFrame:
    """
    Parse CrystalDiskMark text file at given filepath to a `pandas.DataFrame`.
    Rates may vary slightly due to floating point arithmetic.


    Columns in Dataframe:
        - date --- (same value each row)
        - test --- (same value each row)
        - time --- (same value each row)
        - os --- (same value each row)
        - mode --- (same value each row)
        - profile --- (same value each row)
        - comment --- (same value each row)
        - read_write
        - type
        - blocksize
        - unit_blocksize
        - queues
        - threads
        - rate
        - unit_rate
        - iops
        - unit_iops
        - latency
        - unit_latency

    Parameters
    ----------
    filepath : str
        Filepath for file to be parsed

    Returns
    -------
    data : pandas.Dataframe
        Parsed data

    """

    df = pd.DataFrame(columns=["date", "test", "time", "os", "mode", "profile", "comment", "read_write", "type", "blocksize", "unit_blocksize", "queues", "threads",
                               "rate", "unit_rate", "iops", "unit_iops", "latency", "unit_latency"])

    res = parse(filepath)

    for r in res.read_results:
        df = df.append({
            "read_write": "read",
            "date": res.date,
            "test": res.test,
            "time": res.time,
            "os": res.os,
            "mode": res.mode,
            "profile": res.profile,
            "comment": res.comment,
            "type": r.test_type,
            "blocksize": r.block_size,
            "unit_blocksize": r.unit_block_size,
            "queues": r.queues,
            "threads": r.threads,
            "rate": r.rate,
            "unit_rate": r.unit_rate,
            "iops": r.iops,
            "unit_iops": r.unit_iops,
            "latency": r.latency,
            "unit_latency": r.unit_latency,
        }, ignore_index=True)

    for r in res.write_results:
        df = df.append({
            "read_write": "write",
            "date": res.date,
            "test": res.test,
            "time": res.time,
            "os": res.os,
            "mode": res.mode,
            "profile": res.profile,
            "comment": res.comment,
            "type": r.test_type,
            "blocksize": r.block_size,
            "unit_blocksize": r.unit_block_size,
            "queues": r.queues,
            "threads": r.threads,
            "rate": r.rate,
            "unit_rate": r.unit_rate,
            "iops": r.iops,
            "unit_iops": r.unit_iops,
            "latency": r.latency,
            "unit_latency": r.unit_latency,
        }, ignore_index=True)

    return df


def parse(filepath) -> BenchmarkResult:
    """
    Parse CrystalDiskMark text file at given filepath. Rates may vary slightly due to floating point arithmetic.

    Parameters
    ----------
    filepath : str
        Filepath for file to be parsed

    Returns
    -------
    data : BenchmarkResult
        Parsed data

    """
    result = BenchmarkResult()
    # open the file and read through it line by line
    with open(filepath, 'r') as file:
        line = file.readline()
        read_or_write = None

        while line:
            # at each line check for a match with a regex
            key, match = __parse_line(line)

            if key == 'read_or_write':
                read_or_write = match.group('read_or_write').lower()  # in read or write section

            elif key == 'test_res':
                test_result = TestResult()

                test_result.test_type = match.group('type').strip()
                test_result.block_size = float(match.group('blocksize'))
                test_result.unit_block_size = match.group('ublocksize').strip()
                test_result.queues = int(match.group('queue'))
                test_result.threads = int(match.group('threads'))
                test_result.rate = float(match.group('rate'))
                test_result.unit_rate = match.group('urate').strip()
                test_result.iops = float(match.group('iops'))
                test_result.unit_iops = match.group('uiops').strip()
                test_result.latency = float(match.group('us'))
                test_result.unit_latency = match.group('uus').strip()

                if read_or_write == "write":
                    result.write_results += [test_result]
                elif read_or_write == "read":
                    result.read_results += [test_result]
                else:
                    raise Exception("can not classify test result to 'read' or 'write'")

            else:
                read_or_write = None  # reset state

                if key == 'profile':
                    result.profile = match.group('profile').strip()

                elif key == 'test':
                    result.test = match.group('test').strip()

                elif key == 'os':
                    result.os = match.group('os').strip()

                elif key == 'date':
                    result.date = match.group('date').strip()

                elif key == 'time':
                    result.time = match.group('time').strip()

                elif key == 'mode':
                    result.mode = match.group('mode').strip()

                elif key == 'comment':
                    result.comment = match.group('comment').strip()

            line = file.readline()
    return result
