#! /usr/bin/env python3
import sys
import re
from _datetime import datetime

time_pattern = re.compile('^\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\,\d+')
execute_pattern = re.compile('^Executing extension (\w+)')
perf_pattern = re.compile('^session:\s+(?P<session>\d+)\s+transaction:(?:\s+(?P<transaction>\d+))?\s?(?:\s+clientport(?P<client_port>\d+))?\s+account:\s+(?:(?P<account>\w+)\s+)?token:(?:\s+(?P<token>\d+))?\s+elapsed time:(?:\s+(?P<elapsed_time>\d+))')
incoming_pattern = re.compile('incoming: ')
outgoing_pattern = re.compile('incoming: ')
logon_pattern = re.compile('^[*]+\[6\]:\sDMI:127\.0\.0\.1:6700,live,(\w+)')
error_pattern = re.compile('SERRS(.*)SERRS.END')
issues = []


def store_issues(current_perf_info):
    if 'elapsed_time' in current_perf_info.keys() and current_perf_info['elapsed_time'] > 10000:
        issues.append(current_perf_info)
    elif 'incoming_length' in current_perf_info.keys() and current_perf_info['incoming_length'] > 100000:
        issues.append(current_perf_info)
    elif 'outgoing_length' in current_perf_info.keys() and current_perf_info['outgoing_length'] > 100000:
        issues.append(current_perf_info)
    elif 'error' in current_perf_info.keys() and current_perf_info['error']:
        issues.append(current_perf_info)


def parse(line, current_time, state, current_extension, current_perf_info):
    split_line = line.split()
    time_match = time_pattern.search(" ".join(split_line[:2]))
    if time_match:
        current_time = datetime.strptime(" ".join(split_line[:2]), "%Y-%m-%d %H:%M:%S,%f")
        rest = " ".join(split_line[2:])
    else:
        rest = line
    exec_match = execute_pattern.search(rest)
    if exec_match:
        # found an execution. If we have perf info, we need to print it.
        if current_perf_info:
            #print(current_perf_info)
            store_issues(current_perf_info)
            current_perf_info = {}
        state = 1
        current_extension = exec_match.group(1)
    perf_match = perf_pattern.search(rest)
    if state == 1 and perf_match:
        # found performance info
        current_perf_info = {'timestamp': current_time,
                             'extension': current_extension,
                             'user': 'udproxy',
                             'error': None
                             }
        current_perf_info.update(perf_match.groupdict())
        for key in ['elapsed_time', 'session', 'transaction', 'client_port']:
            if key in current_perf_info.keys() and current_perf_info[key]:
                current_perf_info[key] = int(current_perf_info[key])
        state = 2
    incoming_match = incoming_pattern.search(rest)
    if state == 2 and current_perf_info and incoming_match:
        current_perf_info['incoming_length'] = len(rest)
    outgoing_match = outgoing_pattern.search(rest)
    if state == 2 and current_perf_info and outgoing_match:
        current_perf_info['outgoing_length'] = len(rest)
    logon_match = logon_pattern.search(rest)
    if state == 2 and current_perf_info and logon_match:
        current_perf_info['user'] = logon_match.group(1)
    error_match = error_pattern.search(rest)
    if state == 2 and current_perf_info and error_match:
        current_perf_info['error'] = error_match.group(1)
    return current_time, state, current_extension, current_perf_info


def main(args):
    if len(args) > 1:
        filename = args[1]
    else:
        filename = "dmi.log"
    current_extension = ""
    state = 0
    current_perf_info = {}
    current_time = None
    with open(filename, 'r', errors='replace') as f:
        for line in f.readlines():
            current_time, state, current_extension, current_perf_info = parse(line, current_time, state, current_extension, current_perf_info)
    store_issues(current_perf_info)
    # print(current_perf_info)


if __name__ == "__main__":
    main(sys.argv)
    for i in issues:
        print(i)
