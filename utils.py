import re
import pprint
import datetime
import os

from redis import Redis

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


class AppContext:
    # NOTE: charset="utf-8", decode_responses=True is used to
    # auto convert responses from byte strings to utf-8 strings
    # b'ffa122b1c454225fc33ae72bdf8a5e29e6a0cdee94b25752e33bef04a12aff6a' =>
    # 'ffa122b1c454225fc33ae72bdf8a5e29e6a0cdee94b25752e33bef04a12aff6a'

    # https://stackoverflow.com/questions/25745053/about-char-b-prefix-in-python3-4-1-client-connect-to-redis
    # redis = Redis(host='redis', port=6379, charset="utf-8", decode_responses=True)

    redis = Redis(host='redis', port=6379,
                  charset="utf-8", decode_responses=True)
    known_plot_ids = redis.smembers('set::plot_ids')
    verbose = False


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
    # phase_hits = filter(lambda line: re.search('phase', line), lines)
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

        # pp(['p3 details', details])

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

    # we're going with flat, alex

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
        # pp(['file entry', entry, 'joined', joined])
        if os.path.isfile(joined):
            callback(joined)
        if os.path.isdir(joined):
            # print('RECURSE')
            walker(joined, callback)


def ingest_one_file(full_filename, ctx):
    known_plot_ids = ctx.known_plot_ids
    redis = ctx.redis

    if not re.search(r'\d+.log', full_filename):
        print(bcolors.RED + 'skipping unrecognized file: ' +
              full_filename + bcolors.ENDC)
        return False

    parts = re.findall(r'\d+', full_filename)
    tractor = int(parts[0])
    PID = int(parts[1])
    meta = parse_plot_log_file(full_filename)

    # add some extra key -> values not found within the log file content
    meta.update({
        'tractor_id': tractor,
        'PID': PID
    })

    # pp(['meta', meta])

    long_plot_id_hash = meta['plot_id']

    if long_plot_id_hash in known_plot_ids:
        print( 'already have it. skipping plot: ' + bcolors.WARNING +
              long_plot_id_hash + bcolors.ENDC)
        return False

    print('ingesting plot '
          + bcolors.OKGREEN
          + long_plot_id_hash
          + bcolors.ENDC
          + ' (' + full_filename + ')'
          )

    # NOW FOR THE REDIS

    # NOTE: I am trying a bunch of different redis schema things
    # here. I am unsure what I want until I gain some more experience.
    # You might need a change of clothes and a brisk walk after reading this code.

    # I am prefixing things, for now, with set:: list:: and sorted_set:: just to
    # keep things straight as I learn. I probably don't want to continue doing this
    # for too much longer, but it's a helpful aide for me as I learn.

    # I think I would rather have a lookup list for the plot hash IDs
    # rather than having to carry the long plot hash everywhere in the schema

    # in contrast to sets (sadd & smembers), lists have the advantage that I can
    # refer to long plot ID hashes by the index by which they were added to the list, instead
    # of having to carry the plot hash everywhere. This requires being
    # organized, disciplined about how they're referred to in other lists, sets
    # because it's another level of indirection

    # these are easy numbers/strings. fine being a set, I think
    # I could have also just made them an rpush list, but we don't always use every number,
    # so I'll just keep the numbers in this set 1:1 with their tractor ID
    redis.sadd('set::tractor_ids', tractor)

    # IMPORTANT. this list::plot_ids will be a lookup table for the real plot hashes
    length = redis.rpush('list::plot_ids', long_plot_id_hash)
    plot_ingest_index = length - 1  # make zero-based so that LINDEX and LRANGE work

    meta['plot_ingest_index'] = plot_ingest_index

    # so plot_id hash => plot_ingest_index lookups are possible
    redis.hset('hash::plot_ingest_index_by_plot_id',
               long_plot_id_hash, plot_ingest_index)

    # I'm making a distinction between plot index and plot ID. By ID, I always mean
    # the longer hash like 547bef5aa3ce9f5806bfe67849efc7a625949ea860261dc50c8bda43a018fdd8
    # plot index is its 0-based index in the list::plot_ids list

    # the set of long "hash" plot ids for this tractor
    redis.sadd('set::tractor:' + str(tractor) +
               ':plot_ids', long_plot_id_hash)

    # the global set of long hash plot ids
    redis.sadd('set::plot_ids', long_plot_id_hash)

    # the global count of plots, duh
    redis.incr('plot_count')

    if ctx.verbose:
        pp(meta)

    # the plot metadata, keyed by its *list index*, returned from an earlier *rpush*
    #  onto the 'list::plot_ids' list
    redis.hmset('hash::plot:' + str(plot_ingest_index), meta)

    # COPY TIME
    tmp_dict = {}
    # tmp_dict's key is the plot index, value is the score per redis-py zadd rules
    #
    # default to infinite copy_time to sort longer than any other copy time
    tmp_dict[plot_ingest_index] = '+inf'
    if meta['copy_time'] != 'NA':
        tmp_dict[plot_ingest_index] = round(meta['copy_time'])

    redis.zadd('sorted_set::copy_time:', tmp_dict)

    # TOTAL TIME
    tmp_dict = {}
    tmp_dict[plot_ingest_index] = '+inf'
    if meta['total_time'] != 'NA':
        tmp_dict[plot_ingest_index] = round(meta['total_time'])

    # not enough, need to distinguish plots from tractors
    redis.zadd('sorted_set::total_time:', tmp_dict)
