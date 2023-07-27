#! /usr/bin/env python3
import datetime
import sys
import os
from datetime import datetime as dt
from dmi_parser import DMIParser
import logging
import argparse

NOTIFICATION_EMAILS = ['howders@cedarville.edu','nbiggs112@cedarville.edu']


def main(filename: str, min_to_check, show_stats: bool):
    p = DMIParser()
    log.debug(f"Reading file {filename}")
    with open(filename, 'r', errors='replace') as f:
        if min_to_check:
            start_date = dt.now() - datetime.timedelta(minutes=min_to_check)
        else:
            start_date = None
        for line in f.readlines():
            p.parse_dmi_line(line, start_date)
    log.debug(f"Parsed {p.total_transactions()} lines of logs")
    pending = p.finalize()
    if len(pending) >= 5:
        log.debug(f"Found {len(pending)} pending transactions. Adding them to errors.")
        p.error_transactions.extend(pending)
    else:
        log.debug(f"Found only {len(pending)} pending transactions. Ignoring for now.")
    p.filter_errors()
    log.debug(f"Still reporting {len(p.error_transactions)} after filtering")
    if show_stats:
        return p.stats()
    else:
        log.debug(f"returning {len(p.error_transactions)} errors.")
        return p.errors()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger()
    a = argparse.ArgumentParser()
    a.add_argument('-d', '--debug', action='store_true', help="turn on debugging", default=False)
    a.add_argument('-s', '--stats', action='store_true', help="turn on stats", default=False)
    a.add_argument('-m', '--minutes', type=int, help="how many minutes of log to parse", default=5)
    a.add_argument('-r', '--restart', type=int, help="how many errors will trigger a DMI restart", default=10)
    a.add_argument('-f', '--file', type=str, help="logfile to parse", default='dmi.log')
    args = a.parse_args()
    filepath = args.file
    pending_max = args.restart
    minutes = args.minutes
    stats = args.stats
    if args.debug:
        log.setLevel(logging.DEBUG)

    issues = main(filepath, minutes, stats)
    pending_issues = [x for x in issues if x.get_status_time() == "pending"]
    for i in issues:
        print(i)
    if len(pending_issues) >= pending_max:
        log.debug(f"found at least {pending_max} pending transactions. Sending emails and restarting live_ui listener")
        result = os.system("/usr/local/bin/restart_listener.sh live_ui_test")
        for email in NOTIFICATION_EMAILS:
            os.system(f"echo 'Dmi-log-parser restarted the Live_UI listener at {dt.now()} with result: {result}'"
                      f" | mail -s 'dmi-log-parser restarted live_ui listener' {email}")
