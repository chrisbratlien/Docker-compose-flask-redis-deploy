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
parser.add_argument('filename')
args = parser.parse_args()

ctx = utils.AppContext()
redis = ctx.redis

if not os.path.exists(args.filename):
    print(bcolors.FAIL + 'not found: ' + args.filename + bcolors.ENDC)
    quit()
utils.ingest_one_file(args.filename, ctx)
