import datetime
import json

from flask import Flask, jsonify
from math import sqrt
from webob import request
from flask import render_template

from monitor import RPCStateMonitor

app = Flask(__name__, static_folder="./static",
            template_folder="./static")


@app.route('/api/servers/ping', methods=['POST'])
def ping_host():
    data = request.json


@app.route("/")
def index():
    return render_template('index.html')


@app.route("/api/method/stat")
def get_method_stat():
    data = request.json
    topic = data['topic']
    host = data['host']
    wid = data['wid']
    endpoint = data['endpoint']
    method = data['method']
    stat = monitor.rpc_method_state(topic, host, wid, endpoint, method)
    return stat


@app.route('/api/topics')
def topic_list():
    return jsonify({'topics': monitor.topic_list()})


@app.route('/api/workers/stat')
def workers_list():
    return jsonify(monitor.workers_list())


def format_timestamp(timestamp, fmt='%m-%d %H:%M:%S', absolute=False):
    if not timestamp:
        return '-'
    if absolute:
        timestamp -= 3600
    d = datetime.datetime.fromtimestamp(timestamp)
    return d.strftime(fmt)


def calculate_metrics(statistics):
    ts_dist = statistics['ts']['distribution']
    cs_dist = statistics['cs']['distribution']
    time_total = sum(ts_dist)
    call_total = sum(cs_dist)
    sample_avg = time_total / (call_total or 1)
    averaged_ts = []

    quadratic_subs = 0
    non_empty_buckets = 0
    for i in range(len(cs_dist)):
        averaged = ts_dist[i] / (cs_dist[i] or 1)
        if averaged > 0:
            quadratic_subs += (averaged - sample_avg) ** 2
            non_empty_buckets += 1
        averaged_ts.append(averaged)

    deviation = sqrt(sample_avg / (non_empty_buckets - 1))

    statistics['average_ts'] = averaged_ts
    metrics = statistics['metrics'] = {
        'avg': sample_avg,
        'min': ts_dist['min'],
        'max': ts_dist['max'],
        'dev': deviation,
        'time': time_total,
        'calls': call_total,
        'last_call': format_timestamp(statistics['ts']['last_action'])
    }
    return metrics


@app.route('/api/global/state')
def global_state():
    return jsonify(monitor.actual_state)


@app.route('/api/topics/state')
def group_by_topic():
    response = {}
    for topic, hosts in monitor.actual_state.iteritems():
        topic_state = response[topic] = {}
        workers_state = topic_state['workers'] = []
        methods_state = topic_state['methods'] = []
        for host, workers in monitor.actual_state.iteritems():
            for worker, state in workers.iteritems():
                latency = round(state['resp_time'] - state['req_time'], 3)
                w_state = {'time': 0, 'calls': 0, 'min': 0, 'max': 0}
                for endpoint, methods in state['endpoints'].iteritems():
                    for method, statistics in methods:
                        method_state = {'endpoint': endpoint,
                                        'method': method,
                                        'host': host,
                                        'wid': worker}
                        # metrics are reset on each incoming msg
                        # with different state of the method
                        if 'metrics' not in statistics:
                            metrics = calculate_metrics(statistics)
                        else:
                            metrics = statistics['metrics']

                        method_state.update(metrics)
                        methods_state.append(methods_state)

                        w_state['time'] += metrics['time']
                        w_state['calls'] += metrics['calls']
                        w_state['min'] = min(metrics['min'], w_state['min']) if w_state['min'] else metrics['min']
                        w_state['max'] = max(metrics['max'], w_state['max'])

                resp_time = format_timestamp(worker['resp_time'])
                w_state.update({
                    'latency': latency,
                    'resp_time': resp_time,
                    'runtime': format_timestamp(state['runtime'], absolute=True)
                })
                workers_state.append(w_state)


# return jsonify(monitor.actual_state)
# response = {}
# for topic in monitor.actual_state:
#     topic_response = response[topic] = []
#     for host in monitor.actual_state[topic]:
#         for worker in monitor.actual_state[topic][host]:
#             worker_stat = monitor.actual_state[topic][host][worker]
#             executor_stat = worker_stat['executor']
#             for endpoint in worker_stat['endpoints']:
#                 for method in worker_stat['endpoints'][endpoint]:
#                     stat = worker_stat['endpoints'][endpoint][method]
#                     calls_series = stat['cs']
#                     time_series = stat['ts']
#                     avg_time = time_series['total'] / (
#                         calls_series['total'] or 1)
#                     topic_response.append({
#                         'id': topic + host + str(worker) + endpoint + method,
#                         'loop_time': worker_stat['loop_time'],
#                         'host': host,
#                         'worker': worker,
#                         'endpoint': endpoint,
#                         'method': method + "()",
#                         'last_update_time': datetime.datetime.fromtimestamp(
#                             worker_stat['last_update_time']
#                         ).strftime('%m-%d %H:%M:%S'),
#                         'executor': executor_stat,
#                         'total_calls': calls_series['total'],
#                         'total_time': round(time_series['total'], 3),
#                         'calls_distribution': calls_series['distribution'],
#                         'time_distribution': time_series['distribution'],
#                         'time_labels': calc_labels(time_series),
#                         'avg_time': round(avg_time, 3),
#                         'max_time': round(time_series['max'], 3),
#                         'min_time': round(time_series['min'], 3),
#                         'last_action': datetime.datetime.fromtimestamp(
#                             time_series['last_action']
#                         ).strftime('%m-%d %H:%M:%S') if time_series['last_action'] else '-'
#                     })


@app.route("/api/servers/<string:server_topic>/ping", methods=['POST'])
def ping(server_topic):
    pass


if __name__ == "__main__":
    monitor = RPCStateMonitor('localhost', 5673,
                              'test',
                              'test',
                              update_time=10)

app.run()
