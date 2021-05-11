import datetime
import pprint
from flask import Flask
from flask import jsonify

from redis import Redis
import re

import functools
import utils

reduce = functools.reduce

pretty_printer = pprint.PrettyPrinter(indent=4)
pp = pretty_printer.pprint


app = Flask(__name__)
# https://stackoverflow.com/questions/25745053/about-char-b-prefix-in-python3-4-1-client-connect-to-redis
redis = Redis(host='redis', port=6379, charset="utf-8", decode_responses=True)


@app.route('/')
def hello():
    count = redis.incr('hits')
    return 'Hello from Docker! I have been seen {} times.\n'.format(count)


@app.route('/api/tractor_ids')
def show_tractors():
    ids = redis.smembers('tractor_ids')
    ids = list(map(int, ids))
    return jsonify(ids)


@app.route('/api/plot_ids')
def show_plot_ids():
    ids = list(redis.smembers('plot_ids'))
    return jsonify(list(ids))


@app.route('/api/plot/<id>')
def show_plot(id):
    result = redis.hgetall('plot:' + id)
    return jsonify(result)


@app.route('/api/plot_count')
def show_plot_count():
    result = redis.get('plot_count')
    if result == None:
        return 0
    return jsonify(int(result))


@app.route('/cbtest')
def cbtest():
    result = utils.cbtest()
    return jsonify(result)


if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)
