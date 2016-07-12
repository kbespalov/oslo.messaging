import datetime
from math import sqrt

from flask import Flask, jsonify
from flask import request, render_template
from monitor import RPCStateMonitor
from oslo_messaging.rpc.server import loop_bucket


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

    g, l, a = w_state['granularity'], w_state['loop_time'], m_state['latest_call']
    labels = calculate_labels(g, l, a)
    m_state.setdefault('ats', calculate_metrics(m_state))
    m_state.setdefault('cs', [loop_bucket.get_cnt(bucket) for bucket in m_state['distribution']])
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
    buckets = statistics['distribution']

    time_total = 0
    call_total = 0

    for b in buckets:
        call_total += loop_bucket.get_cnt(b)
        time_total += loop_bucket.get_sum(b)

    sample_avg = time_total / (call_total or 1)
    averaged_ts = []

    quadratic_subs = 0
    non_empty_buckets = 0

    for b in buckets:
        averaged = loop_bucket.get_sum(b) / (loop_bucket.get_cnt(b) or 1)
        if averaged > 0:
            quadratic_subs += (averaged - sample_avg) ** 2
            non_empty_buckets += 1
        averaged_ts.append(averaged)

    deviation = 0
    if non_empty_buckets > 1:
        deviation = sqrt(quadratic_subs / non_empty_buckets)

    statistics['ats'] = averaged_ts
    metrics = statistics['metrics'] = {
        'avg': round(sample_avg, 3),
        'min': round(min(loop_bucket.get_min(b) for b in buckets), 3),
        'max': round(max(loop_bucket.get_max(b) for b in buckets), 3),
        'dev': round(deviation, 3),
        'time': round(time_total, 3),
        'calls': call_total,
        'last_call': format_timestamp(statistics['latest_call'])
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

                w_state = {'time': 0, 'calls': 0, 'min': 0, 'max': 0}
                latency = round(state['resp_time'] - state['req_time'], 3)

                for endpoint, methods in state['endpoints'].iteritems():
                    for method, stats in methods.iteritems():
                        method_id = '-'.join([endpoint, str(worker), method])
                        method_state = {'id': method_id,
                                        'endpoint': endpoint,
                                        'method': method,
                                        'host': host,
                                        'wid': worker}

                        if 'metrics' not in stats:
                            metrics = calculate_metrics(stats)
                        else:
                            metrics = stats['metrics']

                        method_state.update(metrics)
                        methods_state.append(method_state)

                        w_state['time'] += metrics['time']
                        w_state['calls'] += metrics['calls']
                        w_state['min'] = min(metrics['min'], w_state['min']) if w_state['min'] else metrics['min']
                        w_state['max'] = max(metrics['max'], w_state['max'])

                resp_time = format_timestamp(state['resp_time'])
                w_state.update({
                    'wid': state['wid'],
                    'host': state['hostname'],
                    'proc_name': state['proc_name'],
                    'server': state['server'],
                    'avg': w_state['time'] / (w_state['calls'] or 1),
                    'latency': latency,
                    'resp_time': resp_time,
                    'runtime': str(datetime.timedelta(seconds=state['runtime']))
                })
                workers_state.append(w_state)
    return jsonify(response)


if __name__ == "__main__":
    monitor = RPCStateMonitor('172.18.78.110', 5672,
                              'guest',
                              'guest',
                              update_time=10)

app.run()
