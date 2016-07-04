import pymongo


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
