import argparse
import utils
from redis import Redis
import random
import pprint

from flask import Flask
import re

from inspect import currentframe, getframeinfo

from utils import bcolors
import os

import time

frameinfo = getframeinfo(currentframe())


pretty_printer = pprint.PrettyPrinter(indent=4)
pp = pretty_printer.pprint


parser = argparse.ArgumentParser()
parser.add_argument("--flushall-before-ingest",
                    help="flush Redis DB", action="store_true")

parser.add_argument("--small-sample",
                    help="only ingest a few random plot logs (for debugging)", action="store_true")


args = parser.parse_args()
# pp(args)
# quit()

# NOTE: charset="utf-8", decode_responses=True is used to
# auto convert responses from byte strings to utf-8 strings
# b'ffa122b1c454225fc33ae72bdf8a5e29e6a0cdee94b25752e33bef04a12aff6a' =>
# 'ffa122b1c454225fc33ae72bdf8a5e29e6a0cdee94b25752e33bef04a12aff6a'

# https://stackoverflow.com/questions/25745053/about-char-b-prefix-in-python3-4-1-client-connect-to-redis
redis = Redis(host='redis', port=6379, charset="utf-8", decode_responses=True)


# aka, this is the closest to "ingest" for now

# This ingests and adds to redis.


if args.flushall_before_ingest:
    print(bcolors.HEADER + bcolors.FAIL +
          'flushing all Redis entries...' + bcolors.ENDC)
    redis.flushall()

known_plot_ids = redis.smembers('set::plot_ids')

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
        PID = int(parts[1])
        meta = utils.parse_plot_log_file(full_filename)

        # add some extra key -> values not found within the log file content
        meta.update({
            'tractor_id': tractor,
            'PID': PID
        })

        # pp(['meta', meta])

        long_plot_id_hash = meta['plot_id']

        if long_plot_id_hash in known_plot_ids:
            print('already have it. skipping plot: ' + long_plot_id_hash)
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
        plot_ingest_index = length - 1  # make zero-based so that LINDEX works

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

        #files = files[0:100]
        random.shuffle(files)
        files = files[0:5]
        #pp(['files', files])

    for file in files:
        #pp(['file loop:', file])
        ingest_one_file(file)


ingest_plot_logs()
