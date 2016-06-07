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

LOG = logging.getLogger(__name__)


class TimeCyclicList(object):
    """
     The collection to persisting a time distribution values with
     specified time interval (time loop) and granularity.

     For example: if duration 60 min, granularity 5 min.
     then self.distribution is list of 12 buckets:
     [0-5 min] [5-10 min] [10-15 min] ... [55-60 min]
     to each of them we are accumulate values by adding.

    """

    def __init__(self, time_loop, granularity):
        self.time_loop = time_loop
        self.granularity = granularity
        self.start_time = time.time()
        self.latest_action_time = 0
        self.latest_index = 0
        self.total = 0
        self.min, self.max = 0, 0
        self.distribution = [0] * (time_loop / granularity)

    def get_index(self, time_value):
        return int((time_value % self.time_loop) / self.granularity)

    def add(self, value):
        if self.min > value or not self.min:
            self.min = value
        if self.max < value:
            self.max = value
        self.latest_action_time = time.time()
        time_index = self.get_index(self.latest_action_time)
        if time_index < self.latest_index:
            self.flush()
        self.distribution[time_index] += value
        self.latest_index = time_index

    def __repr__(self):
        current_total = self.total + sum(self.distribution)
        return {'start_time': self.start_time,
                'last_action_time': self.latest_action_time,
                'min': self.min,
                'max': self.max,
                'duration': self.time_loop,
                'granularity': self.granularity,
                'total': current_total,
                'current_time': time.time(),
                'distribution': self.distribution}

    def flush(self):
        self.total += sum(self.distribution)
        for i in range(len(self.distribution)):
            self.distribution[i] = 0


class RPCStateEndpoint(object):
    # namespace is used in order to avoid a methods name conflicts
    # target = Target(namespace="oslo.messaging.rpc_state")

    def __init__(self, server, target, time_loop=3600, granularity=60):
        self.rpc_server = server
        self.target = target
        self.endpoints_stats = {}
        self.register_endpoints()
        self.id = uuid.uuid4()
        # properties for TimeSeriesCollector
        self.time_loop = time_loop
        self.granularity = granularity

        self.info = {'worker_id': self.id,
                     'topic': self.target.topic,
                     'exchange': self.target.exchange,
                     'server': self.target.server}

    def register_endpoints(self):

        def rpc_stats_aware(stats, method):
            method_name = method.__name__
            time_series = TimeCyclicList(self.time_loop, self.granularity)
            calls_series = TimeCyclicList(self.time_loop, self.granularity)
            stats[method_name] = {'time_series': time_series,
                                  'calls_series': calls_series}

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
                w = rpc_stats_aware(e_stat, getattr(endpoint, name))
                setattr(endpoint, name, w)

            self.endpoints_stats[type(endpoint).__name__] = e_stat

    def rpc_echo_reply(self, ctx):
        return self.info

    def _make_sample(self):
        msg = dict()

        executor_stat = dict()
        stats = self.rpc_server._work_executor.statistics
        executor_stat['failures'] = stats.failures
        executor_stat['executed'] = stats.executed
        executor_stat['runtime'] = stats.runtime
        avg = stats.average_runtime if stats.executed else 0
        executor_stat['average_runtime'] = avg

        msg['executor_stats'] = executor_stat
        msg['endpoint_stats'] = self.endpoints_stats

        return msg

    def rpc_stats(self, ctx):
        msg = self._make_sample()
        msg.update(self.info)
        return msg


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
