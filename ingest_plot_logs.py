import argparse
import utils
from redis import Redis

import pprint

from flask import Flask
import re

from inspect import currentframe, getframeinfo

from utils import bcolors
import os

frameinfo = getframeinfo(currentframe())

#app = Flask(__name__)


pretty_printer = pprint.PrettyPrinter(indent=4)
pp = pretty_printer.pprint


parser = argparse.ArgumentParser()
parser.add_argument("--flushall-before-ingest",
                    help="flush Redis DB", action="store_true")

parser.add_argument("--small-sample",
                    help="only ingest 100 plots (debugging)", action="store_true")


args = parser.parse_args()
# pp(args)
# quit()

# NOTE: charset="utf-8", decode_responses=True is used to
# auto convert responses from byte strings to utf-8 strings
# b'ffa122b1c454225fc33ae72bdf8a5e29e6a0cdee94b25752e33bef04a12aff6a' =>
# 'ffa122b1c454225fc33ae72bdf8a5e29e6a0cdee94b25752e33bef04a12aff6a'

# https://stackoverflow.com/questions/25745053/about-char-b-prefix-in-python3-4-1-client-connect-to-redis
redis = Redis(host='redis', port=6379, charset="utf-8", decode_responses=True)

if args.flushall_before_ingest:

    print(bcolors.HEADER + bcolors.FAIL +
          'flushing all Redis entries...' + bcolors.ENDC)
    redis.flushall()


# aka, this is the closest to "ingest" for now

# This ingests and adds to redis.


known_plot_ids = redis.smembers('plot_ids')
# known_plot_ids = list(map(str, known_plot_ids))
# pp(known_plot_ids)
# print('wee')
# quit()


# can't decide which handler I like more.

# the first handler immediately begins processing everything.

# the alternate handler at least puts things into an array/list first so that I can troubleshoot
# by temporarily shrinking the size of the array
#

def ingest_one_file(payload):
    # print(payload)
    if re.search(r'\d+.log', payload['entry']):
        full_filename = payload['joined']
        parts = re.findall(r'\d+', full_filename)
        tractor = int(parts[0])
        seq = int(parts[1])
        meta = utils.parse_plot_log_file(full_filename)

        # add some extra key -> values not found within the log file content
        meta.update({
            'tractor_id': tractor,
            'seq': seq
        })

        # pp(['meta', meta])
        # pp(['tractor', tractor, 'seq', seq, 'plot_id', meta['plot_id']])

        if meta['plot_id'] in known_plot_ids:
            print('already have it. skipping plot: ' + meta['plot_id'])
            return False

        print('ingesting plot '
              + bcolors.OKGREEN
              + meta['plot_id']
              + bcolors.ENDC
              + ' (' + full_filename + ')'
              )
        # pp(['adding...'])
        pp(meta)
        redis.sadd('tractor_ids', tractor)
        redis.sadd('plot_ids', meta['plot_id'])
        redis.incr('plot_count')
        redis.hmset('plot:' + meta['plot_id'], meta)


def ingest_plot_logs():
    # example of a good payload:
    # {'entry': '9922.log', 'joined': '/logs/tractor23/lotus/logs/9922.log'}
    basepath = '/logs'

    files = []

    def only_plot_log_handler(payload):
        if not re.search(r'\d+.log', payload['entry']):
            # not a plot log, skip
            return False
        files.append(payload)

    # recurse and call the handler on each file found
    utils.walker(basepath, only_plot_log_handler)

    if args.small_sample:
        print(bcolors.WARNING + 'NOTE: using a small sample..(',
              frameinfo.filename, ' at line', frameinfo.lineno, ')'
              + bcolors.ENDC)

        files = files[0:100]
    #pp(['files', files])

    for file in files:
        #pp(['file loop:', file])
        ingest_one_file(file)


#
#  FLUSH!!

ingest_plot_logs()
