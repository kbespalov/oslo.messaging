# Copyright 2013 Red Hat, Inc.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""
An RPC server exposes a number of endpoints, each of which contain a set of
methods which may be invoked remotely by clients over a given transport.

To create an RPC server, you supply a transport, target and a list of
endpoints.

A transport can be obtained simply by calling the get_transport() method::

    transport = messaging.get_transport(conf)

which will load the appropriate transport driver according to the user's
messaging configuration. See get_transport() for more details.

The target supplied when creating an RPC server expresses the topic, server
name and - optionally - the exchange to listen on. See Target for more details
on these attributes.

Each endpoint object may have a target attribute which may have namespace and
version fields set. By default, we use the 'null namespace' and version 1.0.
Incoming method calls will be dispatched to the first endpoint with the
requested method, a matching namespace and a compatible version number.

RPC servers have start(), stop() and wait() messages to begin handling
requests, stop handling requests and wait for all in-process requests to
complete.

A simple example of an RPC server with multiple endpoints might be::

    from oslo_config import cfg
    import oslo_messaging
    import time

    class ServerControlEndpoint(object):

        target = oslo_messaging.Target(namespace='control',
                                       version='2.0')

        def __init__(self, server):
            self.server = server

        def stop(self, ctx):
            if self.server:
                self.server.stop()

    class TestEndpoint(object):

        def test(self, ctx, arg):
            return arg

    transport = oslo_messaging.get_transport(cfg.CONF)
    target = oslo_messaging.Target(topic='test', server='server1')
    endpoints = [
        ServerControlEndpoint(None),
        TestEndpoint(),
    ]
    server = oslo_messaging.get_rpc_server(transport, target, endpoints,
                                           executor='blocking')
    try:
        server.start()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping server")

    server.stop()
    server.wait()

Clients can invoke methods on the server by sending the request to a topic and
it gets sent to one of the servers listening on the topic, or by sending the
request to a specific server listening on the topic, or by sending the request
to all servers listening on the topic (known as fanout). These modes are chosen
via the server and fanout attributes on Target but the mode used is transparent
to the server.

The first parameter to method invocations is always the request context
supplied by the client.

Parameters to the method invocation are primitive types and so must be the
return values from the methods. By supplying a serializer object, a server can
deserialize a request context and arguments from - and serialize return values
to - primitive types.
"""
import functools
import inspect
import json
import os
import time
import uuid

__all__ = [
    'get_rpc_server',
    'expected_exceptions',
]

import logging
import sys

from oslo_messaging._i18n import _LE
from oslo_messaging.rpc import dispatcher as rpc_dispatcher
from oslo_messaging import server as msg_server
from oslo_messaging.target import Target

LOG = logging.getLogger(__name__)


class TimeLoop(object):
    """
     The collection to persisting a time distribution values with
     specified time interval (time loop) and granularity.

     For example: if duration 60 min, granularity 5 min.
     then self.distribution is list of 12 buckets:
     [0-5 min] [5-10 min] [10-15 min] ... [55-60 min]
     to each of them we are accumulate values by adding.
    """

    def __init__(self, loop_time, loop_granularity):
        self.loop_time = loop_time
        self.granularity = loop_granularity
        self.start_time = time.time()
        self.latest_action_time = 0
        self.latest_index = 0
        self.total = 0
        self.min, self.max = 0, 0
        self.prev_loop_time = 0
        self.distribution = [0] * (loop_time / loop_granularity)

    def get_index(self, time_value):
        return int((time_value % self.loop_time) / self.granularity)

    def add(self, value):
        if self.min > value or not self.min:
            self.min = value
        if self.max < value:
            self.max = value
        cur_time = time.time()
        time_index = self.get_index(cur_time)
        if time_index < self.latest_index or self.is_loop_expired(cur_time):
            self.flush(cur_time)
        if time_index > self.latest_index:
            self.distribution[time_index] = value
        else:
            self.distribution[time_index] += value
        if time_index - self.latest_index > 1:
            for i in range(self.latest_index + 1, time_index):
                self.distribution[i] = 0
        self.latest_index = time_index
        self.latest_action_time = cur_time

    def is_loop_expired(self, current_time):
        return current_time - self.latest_action_time > self.loop_time

    def dump(self):
        current_total = self.total + sum(self.distribution[:self.latest_index + 1])
        return {'start_time': self.start_time,
                'last_action': self.latest_action_time,
                'min': self.min,
                'max': self.max,
                'total': current_total,
                'distribution': self.distribution[self.latest_index + 1:] + self.distribution[:self.latest_index + 1]}

    def flush(self, ctime):
        # todo: fix overlooping   loop_3 | loop_0 swap() loop_0 | loop_3 - wrong
        self.min = self.max = 0
        self.total += sum(self.distribution[:self.latest_index])
        self.prev_loop_time = self.latest_action_time
        for i in xrange(0, self.get_index(ctime)):
            self.distribution[i] = 0


class RPCStateEndpoint(object):
    # namespace is used in order to avoid a methods name conflicts
    # target = Target(namespace="oslo.messaging.rpc_state")

    def __init__(self, server, target, loop_time=120, granularity=5):
        self.rpc_server = server
        self.target = target
        self.endpoints_stats = {}
        self.start_time = time.time()
        self.loop_time = loop_time
        self.granularity = granularity
        self.register_endpoints()
        self.worker_pid = os.getpid()
        self.info = {'wid': self.worker_pid,
                     'topic': self.target.topic,
                     'server': self.target.server,
                     'loop_time': self.loop_time,
                     'granularity': self.granularity}

    def register_endpoints(self):

        def rpc_stats_aware(stats, method):
            method_name = method.__name__
            time_series = TimeLoop(self.loop_time, self.granularity)
            calls_series = TimeLoop(self.loop_time, self.granularity)
            stats[method_name] = {'ts': time_series,
                                  'cs': calls_series}

            @functools.wraps(method)
            def wrap(*args, **kwargs):
                start = time.time()
                res = method(*args, **kwargs)
                end = time.time()
                duration = end - start
                time_series.add(duration)
                calls_series.add(1)
                return res

            return wrap

        endpoints = self.rpc_server.dispatcher.endpoints
        for endpoint in endpoints:
            e_stat = dict()
            public_methods = [attr for attr in dir(endpoint) if
                              inspect.ismethod(getattr(endpoint, attr)) and
                              not attr.startswith("_")]
            for name in public_methods:
                aware = rpc_stats_aware(e_stat, getattr(endpoint, name))
                setattr(endpoint, name, aware)
            self.endpoints_stats[type(endpoint).__name__] = e_stat

    def rpc_echo_reply(self, ctx, request_time):
        response = {'req_time': request_time}
        response.update(self.info)
        return response

    def dump_endpoints_stats(self, sample):
        endpoints_sample = sample['endpoints'] = {}
        for endpoint in self.endpoints_stats:
            endpoints_sample[endpoint] = {}
            for method in self.endpoints_stats[endpoint]:
                endpoints_sample[endpoint][method] = {}
                for k, m in self.endpoints_stats[endpoint][method].items():
                    endpoints_sample[endpoint][method][k] = m.dump()

    def runtime(self):
        return time.time() - self.start_time

    def rpc_stats(self, ctx, request_time):
        sample = dict(msg_type='sample',
                      req_time=request_time,
                      runtime=self.runtime())
        sample.update(self.info)
        self.dump_endpoints_stats(sample)
        return sample


class RPCServer(msg_server.MessageHandlingServer):
    def __init__(self, transport, target, dispatcher, executor='blocking'):
        super(RPCServer, self).__init__(transport, dispatcher, executor)
        self._target = target
        state_endpoint = RPCStateEndpoint(self, target)
        self.dispatcher.endpoints.append(state_endpoint)

    def _create_listener(self):
        return self.transport._listen(self._target, 1, None)

    def _process_incoming(self, incoming):
        message = incoming[0]
        try:
            message.acknowledge()
        except Exception:
            LOG.exception(_LE("Can not acknowledge message. Skip processing"))
            return

        failure = None
        try:
            res = self.dispatcher.dispatch(message)
        except rpc_dispatcher.ExpectedException as e:
            failure = e.exc_info
            LOG.debug(u'Expected exception during message handling (%s)', e)
        except Exception:
            # current sys.exc_info() content can be overriden
            # by another exception raised by a log handler during
            # LOG.exception(). So keep a copy and delete it later.
            failure = sys.exc_info()
            LOG.exception(_LE('Exception during message handling'))

        try:
            if failure is None:
                message.reply(res)
            else:
                message.reply(failure=failure)
        except Exception:
            LOG.exception(_LE("Can not send reply for message"))
        finally:
            # NOTE(dhellmann): Remove circular object reference
            # between the current stack frame and the traceback in
            # exc_info.
            del failure


def get_rpc_server(transport, target, endpoints,
                   executor='blocking', serializer=None):
    """Construct an RPC server.

    The executor parameter controls how incoming messages will be received and
    dispatched. By default, the most simple executor is used - the blocking
    executor.

    If the eventlet executor is used, the threading and time library need to be
    monkeypatched.

    :param transport: the messaging transport
    :type transport: Transport
    :param target: the exchange, topic and server to listen on
    :type target: Target
    :param endpoints: a list of endpoint objects
    :type endpoints: list
    :param executor: name of a message executor - for example
                     'eventlet', 'blocking'
    :type executor: str
    :param serializer: an optional entity serializer
    :type serializer: Serializer
    """
    dispatcher = rpc_dispatcher.RPCDispatcher(endpoints, serializer)
    return RPCServer(transport, target, dispatcher, executor)


def expected_exceptions(*exceptions):
    """Decorator for RPC endpoint methods that raise expected exceptions.

    Marking an endpoint method with this decorator allows the declaration
    of expected exceptions that the RPC server should not consider fatal,
    and not log as if they were generated in a real error scenario.

    Note that this will cause listed exceptions to be wrapped in an
    ExpectedException, which is used internally by the RPC sever. The RPC
    client will see the original exception type.
    """

    def outer(func):
        def inner(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            # Take advantage of the fact that we can catch
            # multiple exception types using a tuple of
            # exception classes, with subclass detection
            # for free. Any exception that is not in or
            # derived from the args passed to us will be
            # ignored and thrown as normal.
            except exceptions:
                raise rpc_dispatcher.ExpectedException()

        return inner

    return outer
