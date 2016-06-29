import datetime
import json
from math import sqrt

from flask import Flask, jsonify
from flask import request, render_template

from monitor import RPCStateMonitor


def calculate_labels(granularity, loop_time, last_action):
    if not last_action:
        last_action = datetime.time()

    upper_bound = last_action + (granularity - last_action % granularity)
    lower_bound = upper_bound - loop_time
    labels = []

    while lower_bound < upper_bound:
        labels.append(format_timestamp(lower_bound))
        lower_bound += granularity

    return labels


app = Flask(__name__, static_folder="./static",
            template_folder="./static")


@app.route('/api/servers/ping', methods=['POST'])
def ping_host():
    data = request.json


@app.route("/")
def index():
    return render_template('index.html')


@app.route("/api/method/state", methods=['POST'])
def get_method_state():
    data = request.json
    topic, host, wid = data['topic'], data['host'], data['wid']
    endpoint, method = data['endpoint'], data['method']

    w_state = monitor.actual_state[topic][host][wid]
    m_state = w_state['endpoints'][endpoint][method]

    g, l, a = w_state['granularity'], w_state['loop_time'], m_state['cs']['last_action']
    labels = calculate_labels(g, l, a)
    if 'average_ts' not in m_state:
        calculate_metrics(m_state)
    m_state['labels'] = labels
    return jsonify(m_state)


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

    deviation = 0
    if non_empty_buckets > 1:
        deviation = sqrt(sample_avg / (non_empty_buckets - 1))

    statistics['average_ts'] = averaged_ts
    metrics = statistics['metrics'] = {
        'avg': round(sample_avg, 3),
        'min': round(statistics['ts']['min'], 3),
        'max': round(statistics['ts']['max'], 3),
        'dev': round(deviation, 3),
        'time': round(time_total, 3),
        'calls': call_total,
        'last_call': format_timestamp(statistics['ts']['last_action'])
    }
    return metrics


@app.route('/api/global/state')
def global_state():
    return jsonify(monitor.actual_state)


@app.route('/api/methods/state', methods=['GET'])
def group_by_topic():
    response = {}
    for topic, hosts in monitor.actual_state.iteritems():
        topic_state = response[topic] = {}
        workers_state = topic_state['workers'] = []
        methods_state = topic_state['methods'] = []
        for host, workers in hosts.iteritems():
            for worker, state in workers.iteritems():
                latency = round(state['resp_time'] - state['req_time'], 3)
                w_state = {'time': 0, 'calls': 0, 'min': 0, 'max': 0}
                for endpoint, methods in state['endpoints'].iteritems():
                    for method, statistics in methods.iteritems():
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
                        methods_state.append(method_state)

                        w_state['time'] += metrics['time']
                        w_state['calls'] += metrics['calls']
                        w_state['min'] = min(metrics['min'], w_state['min']) if w_state['min'] else metrics['min']
                        w_state['max'] = max(metrics['max'], w_state['max'])

                resp_time = format_timestamp(state['resp_time'])
                w_state.update({
                    'latency': latency,
                    'resp_time': resp_time,
                    'runtime': format_timestamp(state['runtime'], absolute=True)
                })
                workers_state.append(w_state)
    return jsonify(response)


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
