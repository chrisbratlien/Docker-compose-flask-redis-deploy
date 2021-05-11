import utils
from redis import Redis

import pprint

from flask import Flask

app = Flask(__name__)


pretty_printer = pprint.PrettyPrinter(indent=4)
pp = pretty_printer.pprint

#pp(['app', app])

redis = Redis(host='redis', port=6379)

# print("Hello")
#result = utils.cbtest()
#result = utils.look_for_plots()
pp(['???WHWHWHL', result])
