import json
import logging
import uuid
from threading import Thread

import pika
import requests

from oslo_messaging._drivers.common import deserialize_msg

url = 'amqp://guest:guest@localhost:5672/%2F'

LOG = logging.getLogger('RPC State Controller')
LOG.setLevel(logging.INFO)
LOG.addHandler(logging.StreamHandler())


class FetchingException(Exception):
    pass


class RabbitAPIClient(object):
    # RabbitMQ API resources
    QUEUES = "queues"
    EXCHANGES = "exchanges"
    QUEUE_INFO = "queues/%s/%s"
    BINDINGS = "queues/%s/%s/bindings"

    def __init__(self, management_url, login, password):
        self.auth = (login, password)
        self.api = management_url + "api/%s/"
        print self.api

    def _get(self, resource, data=None):
        print self.api % resource
        r = requests.get(self.api % resource, data, auth=self.auth)
        if r.status_code == 200:
            return r.json()
        else:
            raise FetchingException("Failed to get %s list: response"
                                    " code %s" % (resource, r.status_code))

    def bindings_list(self, queue_name, vhost='%2F'):
        return self._get(self.BINDINGS % (vhost, queue_name))

    def queue_info(self, queue_name, vhost='%2F'):
        return self._get(self.QUEUE_INFO % (vhost, queue_name))

    def queues_list(self, columns='name,consumers'):
        return self._get(self.QUEUES, data=dict(columns=columns))

    def exchanges_list(self, columns='name,type'):
        return self._get(self.EXCHANGES, data=dict(columns=columns))


class RPCStateClient(object):
    BROADCAST_EXCHANGE = "rpc_state_broadcast"

    def __init__(self, host, port, login, passwd, on_incoming):

        management_url = "http://%s:1%d/" % (host, port)
        amqp_url = "amqp://%s:%s@%s:%d" % (login, passwd, host, port)

        self.rabbit_client = RabbitAPIClient(management_url, login, passwd)
        self.params = pika.URLParameters(amqp_url)

        self.connection = pika.BlockingConnection(parameters=self.params)
        self.channel = self.connection.channel()

        self.reply_listener = RPCStateConsumer(amqp_url, on_incoming)
        self.reply_listener.start()
        self.reply_to = self.reply_listener.reply_to
        self.bindings = {}
        self._setup_publish_exchange()

    def _setup_publish_exchange(self):
        LOG.info("Setup broadcast exchange ...")
        self.channel.exchange_declare(exchange=self.BROADCAST_EXCHANGE,
                                      type='fanout',
                                      durable=False)

        exchanges = self.rabbit_client.exchanges_list()
        self._setup_exchange_bindings(exchanges)

    def _setup_exchange_bindings(self, exchanges):
        LOG.info("Setup bindings to broadcast exchange ...")
        for exchange in exchanges:
            if exchange['type'] == 'fanout' and '_fanout' in exchange['name']:
                if exchange['name'] != self.BROADCAST_EXCHANGE \
                        and exchange['name'] not in self.bindings:
                    LOG.info("Successful binding: %s -> %s " % (
                        self.BROADCAST_EXCHANGE, exchange['name']
                    ))
                    self.channel.exchange_bind(source=self.BROADCAST_EXCHANGE,
                                               destination=exchange['name'],
                                               routing_key='rpc_state')

    def _create_call_msg(self, method, args=None):
        msg_id = uuid.uuid4().hex
        msg = dict()
        msg['method'] = method
        msg['_unique_id'] = msg_id
        msg['_msg_id'] = msg_id
        msg['_reply_q'] = self.reply_to
        msg['version'] = '1.0'
        msg['args'] = args or {}
        return msg

    def _publish(self, msg, exchange=None, routing_key=None):
        if not (exchange or routing_key):
            exchange = self.BROADCAST_EXCHANGE
            routing_key = '*'
        properties = pika.BasicProperties(content_type='application/json')
        self.channel.basic_publish(exchange=exchange,
                                   routing_key=routing_key,
                                   body=json.dumps(msg),
                                   properties=properties)

    def ping(self, request_time, exchange=None, routing_key=None):
        args = {'request_time': request_time}
        msg = self._create_call_msg('echo_reply', args)
        self._publish(msg, exchange, routing_key)

    def get_rpc_stats(self, request_time, exchange=None, routing_key=None):
        args = {'request_time': request_time}
        msg = self._create_call_msg('rpc_stats', args)
        self._publish(msg, exchange, routing_key)

    def call_private_method(self, request_time, exchange=None, routing_key=None):
        args = {'request_time': request_time}
        msg = self._create_call_msg('__hash__', args)
        self._publish(msg, exchange, routing_key)


class RPCStateConsumer(object):
    def __init__(self, amqp_url, on_incoming):
        self.params = pika.URLParameters(amqp_url)
        self.connection = pika.BlockingConnection(parameters=self.params)
        self.channel = self.connection.channel()
        self.reply_to = "rpc_state.reply_%s" % str(uuid.uuid4())
        self.exchange_bindings = []
        self.on_incoming = on_incoming
        self._setup_reply_queue()
        self.consumer_thread = Thread(target=self._consume)

    def _setup_reply_queue(self):
        LOG.info("Setup reply queue ...")
        self.channel.queue_declare(self.reply_to)
        self.channel.exchange_declare(self.reply_to, 'direct', durable=False,
                                      auto_delete=True)
        self.channel.queue_bind(self.reply_to, self.reply_to, self.reply_to)

    def _consume(self):
        LOG.info("Starting consuming rpc states ...")
        self.channel.basic_consume(consumer_callback=self._on_message,
                                   no_ack=True,
                                   queue=self.reply_to)
        self.channel.start_consuming()

    def _on_message(self, ch, method_frame, header_frame, body):
        self.on_incoming(deserialize_msg(json.loads(body)))

    def start(self):
        self.consumer_thread.start()

    def stop(self):
        self.channel.close()
        self.connection.close()
