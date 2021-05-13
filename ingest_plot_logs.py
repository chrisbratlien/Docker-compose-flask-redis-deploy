import argparse
import utils

import random
import pprint

import re

from inspect import currentframe, getframeinfo

from utils import bcolors

import os

frameinfo = getframeinfo(currentframe())

pretty_printer = pprint.PrettyPrinter(indent=4)
pp = pretty_printer.pprint


parser = argparse.ArgumentParser()
parser.add_argument("--flushall-before-ingest",
                    help="flush Redis DB", action="store_true")

parser.add_argument("--small-sample",
                    help="only ingest a few random plot logs (for debugging)", action="store_true")

parser.add_argument("--verbose",
                    help="show more feedback", action="store_true")


args = parser.parse_args()
# pp(args)
# quit()


ctx = utils.AppContext()
redis = ctx.redis

if args.verbose:
    ctx.verbose = True



if args.flushall_before_ingest:
    print(bcolors.HEADER + bcolors.FAIL +
          'flushing all Redis entries...' + bcolors.ENDC)
    redis.flushall()


# Ingesting vs Parsing: ingesting goes a step beyond parsing by
# also putting the parsed metadata (file => python dictionary) into redis for later query / retrieval


def ingest_plot_logs(opts):

    # example of a good payload:
    # {'entry': '9922.log', 'joined': '/logs/tractor23/lotus/logs/9922.log'}
    basepath = '/logs'

    files = []

    def only_plot_log_handler(full_filename):
        if not re.search(r'\d+.log$', full_filename):
            # not a plot log, skip
            return False
        files.append(full_filename)

    # recurse and call the handler on each file found
    utils.walker(basepath, only_plot_log_handler)

    if args.small_sample:
        print(bcolors.WARNING + 'NOTE: using a small sample..(',
              frameinfo.filename, ' at line', frameinfo.lineno, ')'
              + bcolors.ENDC)
        # grab a few files at random
        random.shuffle(files)
        files = files[0:5]

    for file in files:
        #pp(['file loop:', file])
        utils.ingest_one_file(file, ctx)


ingest_plot_logs(ctx)
