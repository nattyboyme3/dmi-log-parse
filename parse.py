#! /usr/bin/env python3
import datetime
import sys
from _datetime import datetime as dt
from patterns import *
from dmi_transaction import DMITransaction


class DMIParser:
    def __init__(self):
        self.current_transactions: list[DMITransaction] = []
        self.current_perf_info: dict = {}
        self.current_perf_info_list: list[dict] = []
        self.current_state: int = 0
        self.error_transactions: list[DMITransaction] = []
        self.current_time: dt = dt.now()
        self.completed_transactions: list[DMITransaction] = []

    def flush_perf_info_cache(self):
        # by this point our perf info should have, at a minimum, an extension.
        if 'extension' not in self.current_perf_info.keys() or not self.current_perf_info['extension']:
            raise Exception("Tried to flush incomplete perf info cache.", self.current_perf_info)
        for trans in self.current_transactions:
            if trans.extension == self.current_perf_info['extension']:
                trans.elapsed_time = self.try_get(self.current_perf_info, 'elapsed_time', int)
                trans.user = self.try_get(self.current_perf_info, 'user', str)
                trans.error = self.try_get(self.current_perf_info, 'error', str)
                trans.incoming_length = self.try_get(self.current_perf_info, 'incoming_length', int)
                trans.outgoing_length = self.try_get(self.current_perf_info, 'outgoing_length', int)
                time_elapsed = self.current_time - trans.timestamp
                trans.calc_elapsed = time_elapsed.total_seconds()
                trans.complete = True
                if len(self.current_perf_info_list) < 0:
                    self.current_perf_info = self.current_perf_info_list[0]
                else:
                    self.current_perf_info = {}
                break

    @staticmethod
    def try_get(d: dict, k: str, t: type):
        if k in d.keys():
            return t(d[k])
        else:
            if t is int:
                return 0
            return None

    def errors(self):
        return self.error_transactions

    def stats(self):
        return self.completed_transactions

    def cleanup(self):
        tmp_list = []
        now = dt.now()
        for trans in self.current_transactions:
            if not trans.complete:
                td = self.current_time - trans.timestamp
                # if a transaction is more than 15 min old, we don't care.
                if td.total_seconds() > 900:
                    trans.elapsed_time = -2
                    self.error_transactions.append(trans)
                else:
                    tmp_list.append(trans)
            else:
                if trans.in_error():
                    self.error_transactions.append(trans)
                else:
                    # just drop it, don't keep it, because it's completed and not in error
                    self.completed_transactions.append(trans)
        self.current_transactions = tmp_list

    def parse_dmi_line(self, line, start_time=None):
        self.cleanup()
        split_line = line.split()
        time_match = time_pattern.search(" ".join(split_line[:2]))
        if time_match:
            self.current_time = dt.strptime(" ".join(split_line[:2]), "%Y-%m-%d %H:%M:%S,%f")
            rest = " ".join(split_line[2:])
        else:
            rest = line
        if self.current_time < start_time:
            return
        exec_match = execute_pattern.search(rest)
        if exec_match and self.current_time:
            self.current_state = 1  # this is nominal. We can do anything.
            new_transaction = DMITransaction(exec_match.group(1), self.current_time)
            self.current_transactions.append(new_transaction)
        perf_match = perf_pattern.search(rest)
        if perf_match:
            # found performance info
            tmp_perf_info = {'timestamp': self.current_time}
            # only set timestamp and the values we found\
            gd = perf_match.groupdict()
            tmp_perf_info.update(gd)
            # Convert integers
            for key in ['elapsed_time', 'session', 'transaction', 'client_port']:
                if key in tmp_perf_info.keys() and tmp_perf_info[key]:
                    tmp_perf_info[key] = int(tmp_perf_info[key])
            self.current_state = 2  # This means we have perf info in the cache, and are looking for i/o matches
            # if we already have perf info in the cache, we need to store this one.
            if self.current_perf_info:
                self.current_perf_info_list.append(tmp_perf_info)
            # Otherwise, cache this info
            else:
                self.current_perf_info = tmp_perf_info
        flush = False
        incoming_match = incoming_pattern.search(rest)
        if incoming_match:
            self.current_perf_info['incoming_length'] = len(rest)
            inc_trans_match = incoming_transaction_pattern.search(rest)
            if inc_trans_match:
                self.current_perf_info['extension'] = inc_trans_match.group('extension')
        outgoing_match = outgoing_pattern.search(rest)
        if outgoing_match and 'extension' in self.current_perf_info.keys():
            self.current_perf_info['outgoing_length'] = len(rest)
            flush = True
        logon_match = logon_pattern.search(rest)
        if logon_match:
            self.current_perf_info['user'] = logon_match.group(1)
        error_match = error_pattern.search(rest)
        if error_match:
            self.current_perf_info['error'] = error_match.group(1)
            flush = True
        if flush:
            # This is the end of any given transaction, so we need to store the perf info.
            self.flush_perf_info_cache()


def main(args):
    if len(args) > 1:
        filename = args[1]
    else:
        filename = "dmi.log"
    if len(args) > 2:
        min_to_check = int(args[2])
    else:
        min_to_check = None
    if len(args) > 3:
        stats = True
    else:
        stats = False
    p = DMIParser()
    with open(filename, 'r', errors='replace') as f:
        if min_to_check:
            start_date = dt.now() - datetime.timedelta(minutes=min_to_check)
        else:
            start_date = None
        for line in f.readlines():
            p.parse_dmi_line(line, start_date)
    if stats:
        return p.stats()
    else:
        return p.errors()


if __name__ == "__main__":
    issues = main(sys.argv)
    for i in issues:
        print(i)
