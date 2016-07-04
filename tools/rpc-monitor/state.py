from Queue import Queue, Empty
from threading import Thread

import pymongo
import time
from influxdb import InfluxDBClient
from oslo_config import cfg

from oslo_messaging.rpc.server import loop_bucket


class RPCStateRepositoryBase(object):
    def append(self, incoming):
        raise NotImplemented

    def topics(self):
        raise NotImplemented

    def workers(self):
        raise NotImplemented

    def host(self):
        raise NotImplemented

    def state_of_method(self, endpoint, method_name):
        raise NotImplemented

    def state_of_worker(self, host, worker):
        raise NotImplemented

    def state_of_host(self, topic):
        raise NotImplemented


class InfluxDBStateRepository(RPCStateRepositoryBase):
    opts = [cfg.StrOpt('influx_host', default='localhost'),
            cfg.IntOpt('influx_port', default=8086),
            cfg.StrOpt('influx_user', default='root'),
            cfg.StrOpt('influx_pass', default='root'),
            cfg.StrOpt('influx_db', default='rpc_monitor')]

    def __init__(self):
        self.conf = self.setup_conf(cfg.CONF)
        self.samples_queue = Queue(maxsize=1000)

        self.influx_client = InfluxDBClient(self.conf.influx_host,
                                            self.conf.influx_port,
                                            self.conf.influx_user,
                                            self.conf.influx_pass,
                                            self.conf.influx_db)

        self.influx_client.create_database(self.conf.influx_db)
        self.sender = Thread(target=self.sample_sender)
        self.sender.start()

    def setup_conf(self, conf):
        opt_group = cfg.OptGroup(name='influx_repository')
        conf.register_group(opt_group)
        conf.register_opts(InfluxDBStateRepository.opts, group=opt_group)
        return conf.influx_repository

    def _over_methods(self, sample):
        for endpoint, methods in sample['endpoints'].iteritems():
            for method, state in methods.iteritems():
                yield endpoint, method, state

    def populate_batch(self, batch, max_size=1000):
        try:
            timestamp = time.time()
            while 1:
                batch.append(self.samples_queue.get(timeout=5))
                max_size -= 1
                if max_size == 0 or time.time() - timestamp > 5:
                    break
        except Empty:
            pass

    def sample_sender(self):
        batch = []
        while 1:
            batch.append(self.samples_queue.get())
            self.populate_batch(batch)
            self.influx_client.write_points(batch)
            batch = []

    def append(self, sample):
        for endpoint, method, state in self._over_methods(sample):
            print state['latest_call']
            aligned_time = int(state['latest_call'] - state['latest_call'] % sample['granularity'])
            print sample['granularity']
            for bucket in reversed(state['distribution']):
                s = {
                    'measurement': 'time_consumption',
                    'time': aligned_time * 10 ** 9,
                    'tags': {
                        'topic': sample['topic'],
                        'server': sample['server'],
                        'host': sample['hostname'],
                        'wid': sample['wid'],
                        'process_name': sample['proc_name'],
                        'endpoint': endpoint,
                        'method': method
                    },
                    'fields': {
                        'avg': float(loop_bucket.get_avg(bucket)),
                        'max': float(loop_bucket.get_max(bucket)),
                        'min': float(loop_bucket.get_min(bucket)),
                        'cnt': float(loop_bucket.get_cnt(bucket))
                    }
                }
                self.samples_queue.put(s)
                aligned_time -= sample['granularity']


class MongoStateRepository(RPCStateRepositoryBase):
    def __init__(self):
        self.client = pymongo.MongoClient()
        self.db = self.client.oslo_messaging


class DictStateRepository(RPCStateRepositoryBase):
    def __init__(self):
        self.storage = dict()

    def _over_methods(self, sample):
        for endpoint, methods in sample['endpoints'].iterintems():
            for method, state in methods.iteritems():
                yield endpoint, method, state

    def append(self, sample):
        tp, srv, wid = sample['topic'], sample['server'], sample['wid']
        for endpoint, method, state in self._over_methods(sample):
            pass
