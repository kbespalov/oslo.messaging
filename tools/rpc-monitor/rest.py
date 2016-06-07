from flask import Flask
from oslo_config import cfg
import oslo_messaging as messaging

app = Flask(__name__)


class RPCStateClient(object):
    def __init__(self, transport, target):

        client = messaging.rpc.RPCClient(transport, target).prepare(timeout=timeout)
        method = _rpc_cast if is_cast else _rpc_call

        super(RPCClient, self).__init__(client_id, client, method,
                                        not is_cast, wait_after_msg)


@app.route('/servers/list', methods=['GET'])
def get_rpc_servers_list():
    pass


@app.route("/servers/<string:server_topic>/ping", methods=['POST'])
def ping(server_topic):
    pass


if __name__ == "__main__":
    cfg.CONF(["--config-file", "./conf.ini"])
    transport = messaging.get_transport(cfg.CONF)
    messaging.RPCClient()
    app.run()
