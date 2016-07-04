import json
import logging

import time
from threading import Thread, RLock

from oslo_config import cfg
from client.rabbit_client import RPCStateClient
from state import InfluxDBStateRepository

monitor_opt_group = cfg.OptGroup(name='rpc_state_monitor')
rabbit_opt_group = cfg.OptGroup(name='oslo_messaging_rabbit')

opts = [cfg.StrOpt('url', default='http://localhost:15672'),
        cfg.StrOpt('user_id', default='guest'),
        cfg.StrOpt('password', default='guest')]

CONF = cfg.CONF
CONF.register_group(monitor_opt_group)
CONF.register_group(rabbit_opt_group)
CONF.register_opts(opts=opts, group=monitor_opt_group)

LOG = logging.Logger(__name__)


class nested_dict(dict):
    def path(self, *path):
        level = self
        for key in path:
            if key not in level:
                level[key] = nested_dict()
            level = level[key]
        return level


class PingPongMixin(object):
    def on_pong(self, pong):
        topic = pong['topic']
        ping = self.running_pings.path(topic)
        ping['pongs'].append((time.time(), pong))

    def ping_thread(self, topic, consumers_count, is_run):
        while is_run:
            for i in range(0, consumers_count):
                self.client.ping(time.time(), '', routing_key=topic)

    def start_ping(self, topic):
        topic_ping = self.running_pings.path(topic)
        topic_ping['is_run'] = is_run = True
        topic_ping['pongs'] = []
        queue_info = self.client.rabbit_client.queue_info(topic)
        consumers = queue_info['consumers']
        thread = Thread(target=self.ping_thread, args=[topic, consumers, is_run])
        thread.start()

    def stop_ping(self, topic, server):
        topic_ping = self.running_pings.path(topic)
        topic_ping['is_run'] = False


class RPCStateRepositoryBase(object):
    def append(self, incoming):
        pass

    def topics(self):
        pass

    def workers(self):
        pass

    def host(self):
        pass

    def state_of_method(self, endpoint, method_name):
        pass

    def state_of_worker(self, host, worker):
        pass

    def state_of_host(self, topic):
        pass


class RPCStateMonitor(PingPongMixin):
    def __init__(self, host, port, user, passw, update_time=60):

        self.actual_state = nested_dict()
        self.history = nested_dict()
        self.running_pings = nested_dict()
        self.repository = InfluxDBStateRepository()
        self.callbacks_routes = {'sample': self.repository.append, 'pong': self.on_pong}
        self.client = RPCStateClient(host, port, user, passw, self.on_incoming)
        self.update_interval = update_time
        self.periodic_updates = Thread(target=self.update_rpc_state)
        self.periodic_updates.start()
        self.ping_lock = RLock()

    def topic_list(self):
        return self.actual_state.keys()

    def workers_list(self):
        result = []
        for topic, hosts in self.actual_state.iteritems():
            for host, workers in hosts.iteritems():
                for worker in workers:
                    result.append((host, worker))
        return result

    def rpc_method_state(self, topic, host, wid, endpoint, name):
        return self.actual_state[topic][host][wid]['endpoints'][endpoint][name]

    def get_history_timeline(self, timestamp):
        for topic, hosts in self.history.iteritems():
            for server, workers in hosts.iteritems():
                for wid, state in workers.iteritems():
                    pass

    def merge_to_history(self, sample):
        """
        Merge to the history a incoming statistics sample from RPCServer
        """
        topic, server, wid = sample['topic'], sample['server'], sample['wid']
        worker_history = self.history.path(topic, server, wid)

        duration = sample['loop_time']
        start_time = sample['started']
        granularity = sample['granularity']

        for endpoint, methods in sample['endpoints'].iteritems():
            for method, state in methods.iteritems():

                latest_call = state['latest_call']
                distribution = state['distribution']

                metric_history = worker_history.path(endpoint, method)
                timeline = metric_history.path('timeline')
                metric_history['latest_call'] = latest_call

                upper_time_bucket = int((latest_call - start_time) / granularity)
                lower_time_bucket = upper_time_bucket - duration / granularity

                for value in distribution:
                    timeline[lower_time_bucket] = value
                    lower_time_bucket += 1

    def on_sample(self, sample):
        print json.dumps(sample)
        topic, server, wid = sample['topic'], sample['server'], sample['wid']
        worker_state = self.actual_state.path(topic, server, wid)
        worker_state.update(sample)
        worker_state['resp_time'] = time.time()
        self.merge_to_history(sample)

    def on_incoming(self, msg):
        response = msg['result']
        if response:
            msg_type = response['msg_type']
            self.callbacks_routes[msg_type](response)
        else:
            print 'Failed'

    def update_exchanges_list(self):
        exchanges = self.client.rabbit_client.exchanges_list()
        self.client._setup_exchange_bindings(exchanges)

    def update_rpc_state(self):
        while True:
            request_time = time.time()
            self.client.get_rpc_stats(request_time)
            time.sleep(self.update_interval)
            self.update_exchanges_list()
