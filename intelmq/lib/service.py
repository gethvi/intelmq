import os
from typing import Union
from intelmq.lib.controller import IntelMQControllerNG, LOG_LEVEL
from flask import Flask, request, jsonify
from gunicorn.six import iteritems
from gunicorn.app.base import BaseApplication
from intelmq.lib import exceptions


class IntelMQControllerHTTP(Flask):

    def __init__(self, *args, **kwargs):
        self.controller = IntelMQControllerNG()
        super().__init__(*args, **kwargs)

    def on_exit(self, server):
        self.controller.bots_stop()


intelmq_restapi = IntelMQControllerHTTP(__name__)


def error(msg):
    return jsonify({"status": "ERROR", "message": msg})


@intelmq_restapi.errorhandler(exceptions.BotNotExists)
def error_bot_not_exists(e):
    return error("Bot does not exist."), 422


@intelmq_restapi.errorhandler(500)
def error_500(e):
    return jsonify({"status": "ERROR", "message": e.description}), 500


@intelmq_restapi.errorhandler(400)
def error_400(e):
    return jsonify({"status": "ERROR", "message": e.description}), 400


@intelmq_restapi.errorhandler(404)
def error_404(e):
    return jsonify({"status": "ERROR", "message": e.description}), 404


@intelmq_restapi.errorhandler(405)
def error_405(e):
    return jsonify({"status": "ERROR", "message": e.description}), 405


@intelmq_restapi.route("/")
def hello():
    return jsonify({"intelmq": {"version": intelmq_restapi.controller.version}})


@intelmq_restapi.route("/logs")
def logs():
    # TODO intelmqctl logs
    return jsonify({})


@intelmq_restapi.route("/bots", methods=["GET"])
@intelmq_restapi.route("/bots/<string:group_or_bot_id>", methods=["GET"])
@intelmq_restapi.route("/bots/<string:bot_id>", methods=["PUT", "DELETE"])
def bots(group_or_bot_id=None, bot_id=None):
    if request.method == "GET":
        verbose = (not request.args.get('v', True, bool) or not request.args.get('verbose', True, bool))

        if verbose:
            bots_config = {bot_id: intelmq_restapi.controller.runtime_configuration[bot_id]
                           for bot_id in intelmq_restapi.controller.get_bots(group_or_bot_id)}
            return jsonify(bots_config)

        else:
            return jsonify(intelmq_restapi.controller.get_bots(group_or_bot_id))

    elif request.method == "PUT":
        bot_configuration = request.get_json(force=True)
        intelmq_restapi.controller.bot_add(bot_id, bot_configuration)
        return jsonify({"status": "OK"})

    elif request.method == "DELETE":
        intelmq_restapi.controller.bot_delete(bot_id)
        return jsonify({"status": "OK"})


@intelmq_restapi.route("/bots/status", methods=["GET"])
@intelmq_restapi.route("/bots/status/<string:group_or_bot_id>", methods=["GET"])
def bots_status(group_or_bot_id=None):
    return jsonify(intelmq_restapi.controller.bots_status(group_or_bot_id))


@intelmq_restapi.route("/bots/logs", methods=["GET"])
@intelmq_restapi.route("/bots/logs/<string:group_or_bot_id>", methods=["GET"])
def bots_logs(group_or_bot_id=None):
    lines = request.args.get('lines', -1, int)
    level = request.args.get('level', intelmq_restapi.controller.log_level).upper()
    if level not in LOG_LEVEL:
        level = intelmq_restapi.controller.log_level
    return jsonify(intelmq_restapi.controller.bots_logs(group_or_bot_id, lines=lines, level=level))


@intelmq_restapi.route("/bots/start", methods=["POST"])
@intelmq_restapi.route("/bots/start/<string:group_or_bot_id>", methods=["POST"])
def bots_start(group_or_bot_id=None):
    return jsonify(intelmq_restapi.controller.bots_start(group_or_bot_id))


@intelmq_restapi.route("/bots/stop", methods=["POST"])
@intelmq_restapi.route("/bots/stop/<string:group_or_bot_id>", methods=["POST"])
def bots_stop(group_or_bot_id=None):
    return jsonify(intelmq_restapi.controller.bots_stop(group_or_bot_id))


@intelmq_restapi.route("/bots/reload", methods=["POST"])
@intelmq_restapi.route("/bots/reload/<string:group_or_bot_id>", methods=["POST"])
def bots_reload(group_or_bot_id=None):
    return jsonify(intelmq_restapi.controller.bots_reload(group_or_bot_id))


@intelmq_restapi.route("/bots/restart", methods=["POST"])
@intelmq_restapi.route("/bots/restart/<string:group_or_bot_id>", methods=["POST"])
def bots_restart(group_or_bot_id=None):
    return jsonify(intelmq_restapi.controller.bots_restart(group_or_bot_id))


@intelmq_restapi.route("/bots/enable", methods=["POST"])
@intelmq_restapi.route("/bots/enable/<string:group_or_bot_id>", methods=["POST"])
def bots_enable(group_or_bot_id=None):
    start = (not request.args.get('s', True, bool) or not request.args.get('start', True, bool))
    return jsonify(intelmq_restapi.controller.bots_enable(group_or_bot_id, start))


@intelmq_restapi.route("/bots/disable", methods=["POST"])
@intelmq_restapi.route("/bots/disable/<string:group_or_bot_id>", methods=["POST"])
def bots_disable(group_or_bot_id=None):
    stop = (not request.args.get('s', True, bool) or not request.args.get('stop', True, bool))
    return jsonify(intelmq_restapi.controller.bots_disable(group_or_bot_id, stop))


@intelmq_restapi.route("/queues", methods=["GET"])
@intelmq_restapi.route("/queues/<string:group_or_bot_id>", methods=["GET"])
def bots_queues(group_or_bot_id=None):
    all = (not request.args.get('a', True, bool) or not request.args.get('all', True, bool))
    internal = (not request.args.get('i', True, bool) or not request.args.get('internal', True, bool))
    return jsonify(intelmq_restapi.controller.get_queues(group_or_bot_id, internal, all))


@intelmq_restapi.route("/queues/status", methods=["GET"])
@intelmq_restapi.route("/queues/status/<string:group_or_bot_id>", methods=["GET"])
def bots_queues_status(group_or_bot_id=None):
    all = (not request.args.get('a', True, bool) or not request.args.get('all', True, bool))
    internal = (not request.args.get('i', True, bool) or not request.args.get('internal', True, bool))
    return jsonify(intelmq_restapi.controller.queue_status(group_or_bot_id, internal, all))


@intelmq_restapi.route("/queues/clear", methods=["POST"])
@intelmq_restapi.route("/queues/clear/<string:group_or_bot_id>", methods=["POST"])
def queues_clear(group_or_bot_id=None):
    all = (not request.args.get('a', True, bool) or not request.args.get('all', True, bool))
    internal = (not request.args.get('i', True, bool) or not request.args.get('internal', True, bool))
    return jsonify(intelmq_restapi.controller.queue_clear(group_or_bot_id, internal, all))


@intelmq_restapi.route("/pipeline", methods=["GET"])
def pipeline():
    return jsonify(intelmq_restapi.controller.get_pipeline())


class IntelMQService(BaseApplication):

    def __init__(self, options=None):
        self.options = options or {}
        self.application = intelmq_restapi
        super(IntelMQService, self).__init__()
        self.cfg.set('worker_class', 'sync')
        self.cfg.set('on_exit', self.application.on_exit)
        self.cfg.set('accesslog',
                     os.path.join(self.application.controller.defaults_configuration['logging_path'], 'access.log'))

    def load_config(self):
        config = dict([(key, value) for key, value in iteritems(self.options)
                       if key in self.cfg.settings and value is not None])
        for key, value in iteritems(config):
            self.cfg.set(key.lower(), value)

    def load(self):
        return self.application


def run(daemon: bool = False,
        host: str = "0.0.0.0",
        port: Union[str, int] = 8080,
        workers: int = 3,
        debug: bool = False):

    options = {
        'bind': '%s:%s' % (host, str(port)),
        'workers': workers,
        # 'user': "intelmq",
        "daemon": daemon
    }

    if debug:
        intelmq_restapi.run(host=host, port=int(8080))
    else:
        IntelMQService(options).run()
