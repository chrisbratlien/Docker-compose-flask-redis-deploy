import datetime
import pprint
from flask import Flask, jsonify, render_template

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
    # count = redis.incr('hits')
    # return 'Hello from Docker! I have been seen {} times.\n'.format(count)
    return render_template("index.html")


# NOTE: lots of schema changes happening in ingest_plot_logs.py that may break these endpoints


@app.route('/api/tractor_ids')
def show_tractors():
    ids = redis.smembers('set::tractor_ids')
    ids = list(map(int, ids))
    return jsonify(ids)


@app.route('/api/plot_ids')
def show_plot_ids():
    ids = list(redis.smembers('set::plot_ids'))
    return jsonify(list(ids))


@app.route('/api/plot/<plot_id>')
def show_plot(plot_id):

    # first convert the plot_id to a plot_ingest_index
    plot_ingest_index = redis.hget(
        'hash::plot_ingest_index_by_plot_id', plot_id)

    # return jsonify(idx)

    result = redis.hgetall('hash::plot:' + plot_ingest_index)
    # did I gain anything by doing this indirection?
    # Hopefully yes. later set intersections, scorings, sortings, etc will use the smaller plot_ingest_index
    # membership

    # but the downside is that this lookup is now a 2-step lookup
    # 1. plot_id -> plot_ingest_index
    # 2. plot_ingest_index => the plot metadata

    return jsonify(result)


@app.route('/api/plots_by_total_time')
def plots_by_total_time():
    plot_ingest_indexes_by_total_time = redis.zrange(
        'sorted_set::total_time:', 0, -1)
    # map a function that turns the plot_ingest_index into the plot dictionary
    # onto the sorted-by-total-time indexes pulled above
    sorted_plot_objects = list(map(
        lambda plot_ingest_index: redis.hgetall(
            'hash::plot:' + plot_ingest_index),
        plot_ingest_indexes_by_total_time))
    result = jsonify(sorted_plot_objects)
    return result


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
