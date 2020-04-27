import intelmq.lib.controller as controller
from flask import Flask, request, jsonify


class IntelMQControllerHTTP(Flask):

    def __init__(self, *args, **kwargs):
        self.controller = controller.IntelMQProcessManager()
        super().__init__(*args, **kwargs)

    def on_exit(self, server):
        self.controller.bots_stop()


intelmq_api = IntelMQControllerHTTP(__name__)


# on exit stop all bots
# weakref.finalize(intelmq_api, intelmq_api.controller.bots_stop)


def error(msg):
    return jsonify({"status": "ERROR", "message": msg})


@intelmq_api.errorhandler(500)
def error_500(e):
    return jsonify({"status": "ERROR", "message": e.description}), 500


@intelmq_api.errorhandler(400)
def error_400(e):
    return jsonify({"status": "ERROR", "message": e.description}), 400


@intelmq_api.errorhandler(404)
def error_404(e):
    return jsonify({"status": "ERROR", "message": e.description}), 404


@intelmq_api.errorhandler(405)
def error_405(e):
    return jsonify({"status": "ERROR", "message": e.description}), 405


@intelmq_api.route("/")
def hello():
    return jsonify({"intelmq": {"version": intelmq_api.controller.version}})


@intelmq_api.route("/debug")
def debug():
    return jsonify({"object": str(intelmq_api.controller)})


@intelmq_api.route("/logs")
def logs():
    #TODO
    return jsonify({})


@intelmq_api.route("/bots", methods=["GET"])
@intelmq_api.route("/bots/<string:group_or_bot_id>", methods=["GET"])
@intelmq_api.route("/bots/<string:bot_id>", methods=["PUT", "DELETE"])
def bots(group_or_bot_id=None, bot_id=None):
    if request.method == "GET":
        short = (not request.args.get('s', True, bool) or not request.args.get('short', True, bool))

        bots = intelmq_api.controller.get_bots(group_or_bot_id)

        if short:
            return jsonify(bots)
        else:
            bots_config = {bot_id: intelmq_api.controller.runtime_configuration[bot_id] for bot_id in bots if
                           bot_id in intelmq_api.controller.runtime_configuration.keys()}
            return jsonify(bots_config)

    elif request.method == "PUT":
        data = request.get_json(force=True)
        if data is not None:
            with intelmq_api.controller.edit_runtime_configuration() as conf:
                conf[bot_id] = data
            return jsonify({"status": "OK"})

    elif request.method == "DELETE":
        bots_deleted = {}
        bots = intelmq_api.controller.get_bots(bot_id)
        intelmq_api.controller.bots_stop(bots)

        with intelmq_api.controller.edit_runtime_configuration() as conf:
            for bot_id in bots:
                conf.pop(bot_id, None)
                bots_deleted[bot_id] = "deleted"

        with intelmq_api.controller.edit_pipeline_configuration() as conf:
            for bot_id in bots:
                conf.pop(bot_id, None)

        return jsonify(bots_deleted)


@intelmq_api.route("/bots/status", methods=["GET"])
@intelmq_api.route("/bots/status/<string:group_or_bot_id>", methods=["GET"])
def bots_status(group_or_bot_id=None):
    return jsonify(intelmq_api.controller.bots_status(bots=intelmq_api.controller.get_bots(group_or_bot_id)))


@intelmq_api.route("/bots/logs", methods=["GET"])
@intelmq_api.route("/bots/logs/<string:group_or_bot_id>", methods=["GET"])
def bots_logs(group_or_bot_id=None):
    lines = request.args.get('lines', -1, int)
    level = request.args.get('level', intelmq_api.controller.log_level).upper()
    if level not in controller.LOG_LEVEL:
        level = intelmq_api.controller.log_level
    return jsonify(intelmq_api.controller.bots_logs(bots=intelmq_api.controller.get_bots(group_or_bot_id), lines=lines,
                                                    level=level))


@intelmq_api.route("/bots/start", methods=["POST"])
@intelmq_api.route("/bots/start/<string:group_or_bot_id>", methods=["POST"])
def bots_start(group_or_bot_id=None):
    return jsonify(intelmq_api.controller.bots_start(bots=intelmq_api.controller.get_bots(group_or_bot_id)))


@intelmq_api.route("/bots/stop", methods=["POST"])
@intelmq_api.route("/bots/stop/<string:group_or_bot_id>", methods=["POST"])
def bots_stop(group_or_bot_id=None):
    return jsonify(intelmq_api.controller.bots_stop(bots=intelmq_api.controller.get_bots(group_or_bot_id)))


@intelmq_api.route("/bots/reload", methods=["POST"])
@intelmq_api.route("/bots/reload/<string:group_or_bot_id>", methods=["POST"])
def bots_reload(group_or_bot_id=None):
    return jsonify(intelmq_api.controller.bots_reload(bots=intelmq_api.controller.get_bots(group_or_bot_id)))


@intelmq_api.route("/bots/restart", methods=["POST"])
@intelmq_api.route("/bots/restart/<string:group_or_bot_id>", methods=["POST"])
def bots_restart(group_or_bot_id=None):
    return jsonify(intelmq_api.controller.bots_restart(bots=intelmq_api.controller.get_bots(group_or_bot_id)))


@intelmq_api.route("/bots/enable", methods=["POST"])
@intelmq_api.route("/bots/enable/<string:group_or_bot_id>", methods=["POST"])
def bots_enable(group_or_bot_id=None):
    enabled_bots = dict()
    with intelmq_api.controller.edit_runtime_configuration() as conf:
        for bot_id in intelmq_api.controller.get_bots(group_or_bot_id):
            conf[bot_id]["enabled"] = True
            enabled_bots[bot_id] = "enabled"
    return jsonify(enabled_bots)


@intelmq_api.route("/bots/disable", methods=["POST"])
@intelmq_api.route("/bots/disable/<string:group_or_bot_id>", methods=["POST"])
def bots_disable(group_or_bot_id=None):
    disabled_bots = dict()
    with intelmq_api.controller.edit_runtime_configuration() as conf:
        for bot_id in intelmq_api.controller.get_bots(group_or_bot_id):
            conf[bot_id]["enabled"] = False
            disabled_bots[bot_id] = "disabled"
    return jsonify(disabled_bots)


@intelmq_api.route("/bots/queues", methods=["GET"])
@intelmq_api.route("/bots/queues/<string:group_or_bot_id>", methods=["GET"])
@intelmq_api.route("/bots/queues/<string:bot_id>", methods=["PUT", "DELETE"])
def bots_queues(group_or_bot_id=None, bot_id=None):
    if request.method == "GET":
        return jsonify(intelmq_api.controller.bots_queues(intelmq_api.controller.get_bots(group_or_bot_id)))

    elif request.method == "PUT":
        data = request.get_json(force=True)
        if data is not None:
            with intelmq_api.controller.edit_pipeline_configuration() as conf:
                conf[bot_id] = data
            return jsonify({"status": "OK"})

    elif request.method == "DELETE":
        with intelmq_api.controller.edit_pipeline_configuration() as conf:
            conf.pop(bot_id, None)
        return jsonify({"status": "OK"})
        # TODO delete queue from message broker?


@intelmq_api.route("/bots/queues/status", methods=["GET"])
@intelmq_api.route("/bots/queues/status/<string:group_or_bot_id>", methods=["GET"])
def bots_queues_status(group_or_bot_id=None):
    return jsonify(
        intelmq_api.controller.bots_queues(intelmq_api.controller.get_bots(group_or_bot_id), include_status=True))


@intelmq_api.route("/queues", methods=["GET"])
@intelmq_api.route("/queues/<string:type_or_queue_id>", methods=["GET"])
def queues_list(type_or_queue_id=None):
    return jsonify(intelmq_api.controller.get_queues(type_or_queue_id))


@intelmq_api.route("/queues/status", methods=["GET"])
@intelmq_api.route("/queues/status/<string:type_or_queue_id>", methods=["GET"])
def queues_status(type_or_queue_id=None):
    return jsonify(intelmq_api.controller.queues_status(intelmq_api.controller.get_queues(type_or_queue_id)))


@intelmq_api.route("/queues/clear", methods=["POST"])
@intelmq_api.route("/queues/clear/<string:type_or_queue_id>", methods=["POST"])
def queues_clear(type_or_queue_id=None):
    return jsonify(intelmq_api.controller.queues_clear(intelmq_api.controller.get_queues(type_or_queue_id)))
