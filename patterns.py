import re

time_pattern = re.compile('^\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}\,\d+')
execute_pattern = re.compile('^Executing extension (\w+)')
perf_pattern = re.compile('^session:\s+(?P<session>\d+)\s+transaction:(?:\s+(?P<transaction>\d+))?\s?(?:\s+clientport(?P<client_port>\d+))?\s+account:\s+(?:(?P<account>\w+)\s+)?token:(?:\s+(?P<token>\d+))?\s+elapsed time:(?:\s+(?P<elapsed_time>\d+))')
incoming_pattern = re.compile('incoming: ')
outgoing_pattern = re.compile('outgoing: ')
logon_pattern = re.compile('^[*]+\[6\]:\sDMI:127\.0\.0\.1:6700,live,(\w+)')
error_pattern = re.compile('SERRS(.*)SERRS.END')
incoming_transaction_pattern = re.compile('incoming: DMI.\d\.\d.(?P<extension>\w{2,6})')