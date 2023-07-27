#! /usr/bin/env python3
import datetime
import sys
import os
from datetime import datetime as dt
from dmi_parser import DMIParser

MAX_PENDING_BEFORE_RESTART = 10
NOTIFICATION_EMAILS = ['howders@cedarville.edu','nbiggs112@cedarville.edu']


def main(filename: str, min_to_check, show_stats: bool):
    p = DMIParser()
    with open(filename, 'r', errors='replace') as f:
        if min_to_check:
            start_date = dt.now() - datetime.timedelta(minutes=min_to_check)
        else:
            start_date = None
        for line in f.readlines():
            p.parse_dmi_line(line, start_date)
    pending = p.finalize()
    if len(pending) >= 5:
        p.error_transactions.extend(pending)
    p.filter_errors()
    if show_stats:
        return p.stats()
    else:
        return p.errors()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        filepath = sys.argv[1]
    else:
        filepath = "dmi.log"
    if len(sys.argv) > 2:
        MAX_PENDING_BEFORE_RESTART = int(sys.argv[2])
    if len(sys.argv) > 3:
        minutes = int(sys.argv[3])
    else:
        minutes = None
    if len(sys.argv) > 4:
        stats = True
    else:
        stats = False
    issues = main(filepath, minutes, stats)
    pending_issues = [x for x in issues if x.get_status_time() == "pending"]
    for i in issues:
        print(i)
    if len(pending_issues) > MAX_PENDING_BEFORE_RESTART:
        result = os.system("/usr/local/bin/restart_listener.sh live_ui_test")
        for email in NOTIFICATION_EMAILS:
            os.system(f"echo 'Dmi-log-parser restarted the Live_UI listener at {dt.now()} with result: {result}'"
                      f" | mail -s 'dmi-log-parser restarted live_ui listener' {email}")
