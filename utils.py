import re
import pprint
import datetime
import os
pretty_printer = pprint.PrettyPrinter(indent=4)
pp = pretty_printer.pprint

# https://tomayko.com/blog/2004/cleanest-python-find-in-list-function


re_cpu_rparen = re.compile(r'CPU.*\)')

# https://stackoverflow.com/questions/287871/how-to-print-colored-text-to-the-terminal


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def find(f, seq):
    """Return first item in sequence where f(item) == True."""
    for item in seq:
        if f(item):
            return item


def get_when_block(when_orig_str):
    result = {}
    # https://docs.python.org/3/library/datetime.html#strftime-strptime-behavior
    # https://stackabuse.com/converting-strings-to-datetime-in-python/
    # Sat May 8 10:32:56 2021 -> %a %b %d %H:%M:%S %Y
    datetime_obj = datetime.datetime.strptime(
        when_orig_str, '%a %b %d %H:%M:%S %Y')  # match the plot log file date format
    result['when_datetime_obj'] = datetime_obj
    result['when_isoformat'] = datetime_obj.isoformat()
    result['when_unixformat'] = int(datetime_obj.timestamp())
    result['when_str'] = when_orig_str
    return result


def parse_starting_phase_line(str):
    result = {}
    parts = str.split('...')
    when_orig_str = parts[1].strip()
    result = get_when_block(when_orig_str)
    return result


def parse_time_for_phase_line(str):
    p = re.compile(r'CPU.*\)')
    parts = p.split(str)
    # pp(['parts', parts])
    when_orig_str = parts[1].strip()
    result = get_when_block(when_orig_str)
    return result


phase_template = {
    "duration_secs": "NA",
    "when_started_unixformat": "NA",
    "when_ended_unixformat": "NA",
}


def parse_plot_log(lines):
    """lines is an array of lines read in from a chia plot log file with f.readlines()"""
    # phase data
    p1 = phase_template.copy()
    p2 = phase_template.copy()
    p3 = phase_template.copy()
    p4 = phase_template.copy()

    # other ways to find lines:
    phase_hits = filter(lambda line: re.search('phase', line), lines)
    # pp(['phase_hits', phase_hits])
    # phase1_begin = filter(lambda line: re.search(
    #    'Starting phase 1/4', line), lines)
    # pp(list(phase1_begin))
    # p1_start = next((line for line in lines if re.search(
    #    'Starting phase 1/4', line)), None)
    # p1_end_str = next((line for line in lines if re.search(
    #    'Time for phase 1', line)), None)

    id_line = find(lambda line: re.search('ID: ', line), lines)

    total_time_line = find(lambda line: re.search('Total time =', line), lines)

    copy_time_line = find(lambda line: re.search('Copy time =', line), lines)

    p1_start_str = find(lambda line: re.search(
        'Starting phase 1/4', line), lines)
    p1_end_str = find(lambda line: re.search(
        'Time for phase 1', line), lines)

    p2_start_str = find(lambda line: re.search(
        'Starting phase 2/4', line), lines)
    p2_end_str = find(lambda line: re.search(
        'Time for phase 2', line), lines)

    p3_start_str = find(lambda line: re.search(
        'Starting phase 3/4', line), lines)
    p3_end_str = find(lambda line: re.search(
        'Time for phase 3', line), lines)

    p4_start_str = find(lambda line: re.search(
        'Starting phase 4/4', line), lines)
    p4_end_str = find(lambda line: re.search(
        'Time for phase 4', line), lines)

    plot_id = False
    if id_line:
        p = re.compile('ID:\s+')
        plot_id = p.split(id_line)[1].strip()

    if p1_start_str:
        details = parse_starting_phase_line(p1_start_str)
        p1['when_started'] = details['when_datetime_obj']
        p1['when_started_unixformat'] = details['when_unixformat']

    if p1_end_str:
        details = parse_time_for_phase_line(p1_end_str)
        p1['when_ended'] = details['when_datetime_obj']
        p1['when_ended_unixformat'] = details['when_unixformat']

        # NOTE: this could also be parsed out of the time for phase line
        dur = p1['when_ended'] - p1['when_started']
        # dur is a datetime.timedelta
        p1['duration_secs'] = dur.seconds

    if p2_start_str:
        details = parse_starting_phase_line(p2_start_str)
        p2['when_started'] = details['when_datetime_obj']
        p2['when_started_unixformat'] = details['when_unixformat']

    if p2_end_str:
        details = parse_time_for_phase_line(p2_end_str)
        p2['when_ended'] = details['when_datetime_obj']
        p2['when_ended_unixformat'] = details['when_unixformat']

        # NOTE: this could also be parsed out of the time for phase line
        dur = p2['when_ended'] - p2['when_started']
        # dur is a datetime.timedelta
        p2['duration_secs'] = dur.seconds

    if p3_start_str:
        details = parse_starting_phase_line(p3_start_str)
        p3['when_started'] = details['when_datetime_obj']
        p3['when_started_unixformat'] = details['when_unixformat']

    if p3_end_str:
        details = parse_time_for_phase_line(p3_end_str)

        #pp(['p3 details', details])

        p3['when_ended'] = details['when_datetime_obj']
        p3['when_ended_unixformat'] = details['when_unixformat']
        # NOTE: this could also be parsed out of the time for phase line
        dur = p3['when_ended'] - p3['when_started']
        # dur is a datetime.timedelta
        p3['duration_secs'] = dur.seconds

    if p4_start_str:
        details = parse_starting_phase_line(p4_start_str)
        p4['when_started'] = details['when_datetime_obj']
        p4['when_started_unixformat'] = details['when_unixformat']

    if p4_end_str:
        details = parse_time_for_phase_line(p4_end_str)
        p4['when_ended'] = details['when_datetime_obj']
        p4['when_ended_unixformat'] = details['when_unixformat']
        # NOTE: this could also be parsed out of the time for phase line
        dur = p4['when_ended'] - p4['when_started']
        # dur is a datetime.timedelta
        p4['duration_secs'] = dur.seconds

    total_time = "NA"
    if total_time_line:
        parts = re_cpu_rparen.split(total_time_line)
        q = re.compile(r'\d+\.+\d+')
        seconds = q.search(parts[0])
        total_time = float(seconds.group())

    copy_time = "NA"
    if copy_time_line:
        parts = re_cpu_rparen.split(copy_time_line)
        q = re.compile(r'\d+\.+\d+')
        seconds = q.search(parts[0])
        copy_time = float(seconds.group())

    # pp(['** >> p1', p1])
    # pp(['** >> p2', p2])
    # pp(['** >> p3', p3])
    # pp(['** >> p4', p4])

    # unsure whether to keep this nested or flat for redis.
    # as-is, phase1 (p1) through phase4 (p4) would make it structured

    result = {
        "plot_id": plot_id,
        "phase1_started_unixformat": p1['when_started_unixformat'],
        "phase1_duration_secs": p1['duration_secs'],
        "phase2_duration_secs": p2['duration_secs'],
        "phase3_duration_secs": p3['duration_secs'],
        "phase4_duration_secs": p4['duration_secs'],
        "phase4_ended_unixformat": p4['when_ended_unixformat'],
        # keep it flat for now...
        # "phase2": p2,
        # "phase3": p3,
        # "phase4": p4,
        "total_time": total_time,
        "copy_time": copy_time
    }
    return result


def parse_plot_log_file(filename):
    # pp(['parsing file', filename])
    f = open(filename, 'r')
    lines = f.readlines()
    result = parse_plot_log(lines)
    return result


def cbtest():
    filename = '/logs/tractor23/lotus/logs/27941.log'
    result = parse_plot_log_file(filename)
    return result


def walker(basepath, callback):
    """recursively walk basepath and trigger callback on all files found"""
    for entry in os.listdir(basepath):
        joined = os.path.join(basepath, entry)
        if os.path.isfile(joined):
            callback(
                {
                    "entry": entry,
                    "joined": joined
                }

            )  # (pp(['file entry', entry, 'joined', joined])
        if os.path.isdir(joined):
            # print('RECURSE')
            walker(joined, callback)
