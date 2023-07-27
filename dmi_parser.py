import logging

from patterns import *
from dmi_transaction import DMITransaction
from datetime import datetime as dt
import logging


class DMIParser:
    def __init__(self, debug):
        self.current_transactions: list[DMITransaction] = []
        self.current_perf_info: dict = {}
        self.current_perf_info_list: list[dict] = []
        self.current_state: int = 0
        self.error_transactions: list[DMITransaction] = []
        self.current_time: dt = dt.now()
        self.completed_transactions: list[DMITransaction] = []
        self.log = logging.getLogger()
        self.last_extension = ""
        if debug:
            self.log.setLevel(logging.DEBUG)

    def total_transactions(self):
        return len(self.completed_transactions) + len(self.error_transactions) + len(self.current_transactions)

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
                self.log.debug(f"completed transaction with extension {trans.extension} and time of {trans.elapsed_time}")
                if len(self.current_perf_info_list) > 0:
                    self.current_perf_info = self.current_perf_info_list.pop(0)
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

    def finalize(self):
        pending_transactions = []
        for trans in self.current_transactions:
            if not trans.complete:
                trans.elapsed_time = -3
                pending_transactions.append(trans)
            elif trans.in_error():
                self.error_transactions.append(trans)
            else:
                self.completed_transactions.append(trans)
        return pending_transactions

    def cleanup(self):
        tmp_list = []
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
                    self.completed_transactions.append(trans)
        self.current_transactions = tmp_list

    def parse_dmi_line(self, line, start_time=None):
        self.cleanup()
        split_line = line.strip().split()
        time_match = time_pattern.search(" ".join(split_line[:2]))
        if time_match:
            self.current_time = dt.strptime(" ".join(split_line[:2]), "%Y-%m-%d %H:%M:%S,%f")
            self.log.debug(f'matched time with {self.current_time}')
            rest = " ".join(split_line[2:])
        else:
            rest = line
        if start_time and self.current_time and self.current_time < start_time:
            self.log.debug(f'discarding old log {self.current_time} is before start time of {start_time}')
            return
        exec_match = execute_pattern.search(rest)
        if exec_match and self.current_time:
            self.current_state = 1  # this is nominal. We can do anything.
            new_transaction = DMITransaction(exec_match.group(1), self.current_time)
            self.log.debug(f'matched execution with extension {new_transaction.extension}')
            self.current_transactions.append(new_transaction)
            self.last_extension = new_transaction.extension
        perf_match = perf_pattern.search(rest)
        if perf_match:
            # found performance info
            tmp_perf_info = {'timestamp': self.current_time}
            # only set timestamp and the values we found\
            gd = perf_match.groupdict()
            tmp_perf_info.update(gd)
            self.log.debug(f'matched perf info info {tmp_perf_info}')
            # Convert integers
            for key in ['elapsed_time', 'session', 'transaction', 'client_port']:
                if key in tmp_perf_info.keys() and tmp_perf_info[key]:
                    tmp_perf_info[key] = int(tmp_perf_info[key])
            self.current_state = 2  # This means we have perf info in the cache, and are looking for i/o matches
            # if we already have perf info in the cache, we need to store this one.
            if self.current_perf_info:
                if len(self.current_perf_info.keys()) > 1:
                    # if it's more than one key, we will just chuck this one in the cache
                    self.current_perf_info_list.append(tmp_perf_info)
                else:
                    # if it's only one key, we'll merge them (logins)
                    self.current_perf_info.update(tmp_perf_info)
            # Otherwise, cache this info
            else:
                self.current_perf_info = tmp_perf_info
        flush = False
        incoming_match = incoming_pattern.search(rest)
        if incoming_match:
            self.current_perf_info['incoming_length'] = len(rest)
            self.log.debug(f'matched incoming with info {rest[:10]}')
            inc_trans_match = incoming_transaction_pattern.search(rest)
            if inc_trans_match:
                self.log.debug(f'matched incoming transaction with extension info: {inc_trans_match.group("extension")}')
                self.current_perf_info['extension'] = inc_trans_match.group('extension')
            else:
                self.current_perf_info['extension'] = self.last_extension
        outgoing_match = outgoing_pattern.search(rest)
        if outgoing_match and 'extension' in self.current_perf_info.keys():
            self.log.debug(f'matched outgoing with info {rest[:10]}')
            self.current_perf_info['outgoing_length'] = len(rest)
            flush = True
        logon_match = logon_pattern.search(rest)
        if logon_match:
            self.log.debug(f'matched login with info {logon_match.group(1)}')
            self.current_perf_info['user'] = logon_match.group(1)
        error_match = error_pattern.search(rest)
        if error_match:
            self.log.debug(f'matched error with info {error_match.group(1)}')
            self.current_perf_info['error'] = error_match.group(1)
            flush = True
        if flush:
            # This is the end of any given transaction, so we need to store the perf info.
            self.log.debug(f'flushing perf info cache')
            self.flush_perf_info_cache()

    def filter_errors(self):
        self.error_transactions = self.filtered_errors()

    def filtered_errors(self):
        new_list = []
        for error in self.error_transactions:
            if not error.contains_any(ignore_list):
                new_list.append(error)
        return new_list

