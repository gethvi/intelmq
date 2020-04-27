from gunicorn.six import iteritems
from gunicorn.app.base import BaseApplication

from intelmq.lib.views import intelmq_api


class IntelMQApplication(BaseApplication):

    def __init__(self, application, options=None):
        self.options = options or {}
        self.application = intelmq_api
        super(IntelMQApplication, self).__init__()

        from gunicorn.glogging import Logger

    def load_config(self):
        config = dict([(key, value) for key, value in iteritems(self.options)
                       if key in self.cfg.settings and value is not None])
        for key, value in iteritems(config):
            self.cfg.set(key.lower(), value)

    def load(self):
        return self.application


import os


def main():
    options = {
        'bind': '%s:%s' % ('0.0.0.0', '8080'),
        'workers': 10,
        'accesslog': os.path.join(intelmq_api.controller.defaults_configuration['logging_path'], 'access.log'),
        'on_exit': intelmq_api.on_exit,
        'user': "legion",
        "worker_class": "sync"
    }

    IntelMQApplication(intelmq_api, options).run()

    # For development and debugging
    # intelmq_api.run(port=8080)

if __name__ == '__main__':
    main()
