from oslo_config import cfg

opt_group = cfg.OptGroup(name='rpc-monitor',
                         title='RabbitMQ driver options')

opts = [cfg.StrOpt('url', default='rabbitmq://localhost:5672'),
        cfg.StrOpt('user_id', default='guest'),
        cfg.StrOpt('password', default='guest')]
