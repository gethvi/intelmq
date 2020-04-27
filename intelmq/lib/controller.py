# -*- coding: utf-8 -*-
import json
import os
import shutil
import signal
import subprocess
import time

from contextlib import contextmanager
from collections import OrderedDict

import psutil

from intelmq import (BOTS_FILE, DEFAULT_LOGGING_LEVEL, DEFAULTS_CONF_FILE,
                     HARMONIZATION_CONF_FILE, PIPELINE_CONF_FILE,
                     RUNTIME_CONF_FILE, VAR_RUN_PATH, VAR_STATE_PATH, DEFAULT_LOGGING_PATH, __version__)

from intelmq.lib import utils
from intelmq.lib.bot_debugger import BotDebugger
from intelmq.lib.pipeline import PipelineFactory


# import intelmq.lib.upgrades as upgrades


class Parameters(object):
    pass


STATUSES = {
    'starting': 0,
    'running': 1,
    'stopping': 2,
    'stopped': 3,
}

MESSAGES = {
    'disabled': 'Bot %s is disabled.',
    'starting': 'Starting %s...',
    'running': 'Bot %s is running.',
    'stopped': 'Bot %s is stopped.',
    'stopping': 'Stopping bot %s...',
    'reloading': 'Reloading bot %s ...',
    'reloaded': 'Bot %s is reloaded.',
}

ERROR_MESSAGES = {
    'starting': 'Bot %s failed to START.',
    'running': 'Bot %s is still running.',
    'stopped': 'Bot %s was NOT RUNNING.',
    'stopping': 'Bot %s failed to STOP.',
    'not found': 'Bot %s failed to START because the file cannot be found.',
    'access denied': 'Bot %s failed to %s because of missing permissions.',
    'unknown': 'Status of Bot %s is unknown: %r.',
}

LOG_LEVEL = OrderedDict([
    ('DEBUG', 0),
    ('INFO', 1),
    ('WARNING', 2),
    ('ERROR', 3),
    ('CRITICAL', 4),
])

RETURN_TYPES = ['text', 'json']
RETURN_TYPE = None
QUIET = False

BOT_GROUP = {"collectors": "Collector", "parsers": "Parser", "experts": "Expert", "outputs": "Output"}

QUEUE_TYPE = ["source", "destination", "internal", "all"]


class IntelMQProcessManager:
    PIDDIR = VAR_RUN_PATH
    PIDFILE = os.path.join(PIDDIR, "{}.pid")

    def __init__(self, logger=None):
        if not self.check():
            exit(1)
        self.version = __version__
        self._runtime_configuration = utils.load_configuration(RUNTIME_CONF_FILE)
        self._defaults_configuration = utils.load_configuration(DEFAULTS_CONF_FILE)
        self._pipeline_configuration = utils.load_configuration(PIPELINE_CONF_FILE)
        self.log_level = self.defaults_configuration["logging_level"].upper()

        if logger is None:

            try:
                self.logger = utils.log('intelmqctl',
                                        log_level=self.log_level,
                                        log_path="/home/legion/projects/intelmq-api",
                                        log_format_stream=utils.LOG_FORMAT)

            except (FileNotFoundError, PermissionError) as e:
                self.logger = utils.log('intelmqctl',
                                        log_level=self.log_level,
                                        log_path=False,
                                        log_format_stream=utils.LOG_FORMAT)

                self.logger.error('Not logging to file: %s', e)

        else:
            self.logger = logger

    @property
    def runtime_configuration(self) -> dict:
        return self._runtime_configuration

    @contextmanager
    def edit_runtime_configuration(self):
        original_hash = hash(str(self._runtime_configuration))
        yield self._runtime_configuration
        modified_hash = hash(str(self._runtime_configuration))
        if original_hash != modified_hash:
            try:
                with open(RUNTIME_CONF_FILE, 'w') as f:
                    json.dump(self._runtime_configuration, fp=f, indent=4, sort_keys=True, separators=(',', ': '))

            except PermissionError:
                # TODO
                self.abort("Can't update runtime configuration: Permission denied.")

    @property
    def pipeline_configuration(self) -> dict:
        return self._pipeline_configuration

    @contextmanager
    def edit_pipeline_configuration(self):
        original_hash = hash(str(self._pipeline_configuration))
        yield self._pipeline_configuration
        modified_hash = hash(str(self._pipeline_configuration))
        if original_hash != modified_hash:
            # try:
            with open(PIPELINE_CONF_FILE, 'w') as f:
                json.dump(self._pipeline_configuration, fp=f, indent=4, sort_keys=True, separators=(',', ': '))

            # except PermissionError:
            #     # TODO
            #     self.abort("Can't update pipeline configuration: Permission denied.")

    @property
    def defaults_configuration(self) -> dict:
        return self._defaults_configuration

    def check(self, logger=None):

        system_ready = True
        external_logger = False

        if os.access(DEFAULT_LOGGING_PATH, os.R_OK) and os.access(DEFAULT_LOGGING_PATH, os.W_OK):
            print(f"Logging path {DEFAULT_LOGGING_PATH} permissions are cool.")
        else:
            print(f"Logging path {DEFAULT_LOGGING_PATH} permissions are not cool.")
            exit(1)

        if logger is None:
            logger = utils.log('intelmqctl', log_format_stream='%(name)s: %(levelname)s %(message)s')
        else:
            external_logger = True

        logger.info("Running startup check")

        # logger = utils.log("intelmqctl")
        # TODO
        # VAR_RUN_PATH writeable?
        # RUNTIME_CONF_FILE writeable?
        # DEFAULTS_CONF_FILE readable?
        # PIPELINE_CONF_FILE writeable?
        # log_path writeable

        # logging_handler = file/syslog

        logger.info(f"PID: {os.getpid()}")

        #TODO mory DRY approach

        if os.access(VAR_RUN_PATH, os.R_OK) and os.access(VAR_RUN_PATH, os.W_OK):
            logger.info(f"Permission check for {VAR_RUN_PATH} returned OK.")
        else:
            logger.error(f"Permission check for {VAR_RUN_PATH} returned FAIL.")
            system_ready = False

        if os.access(RUNTIME_CONF_FILE, os.R_OK) and os.access(RUNTIME_CONF_FILE, os.W_OK):
            logger.info(f"Permission check for {RUNTIME_CONF_FILE} returned OK.")
        else:
            logger.error(f"Permission check for {RUNTIME_CONF_FILE} returned FAIL.")
            system_ready = False

        if os.access(PIPELINE_CONF_FILE, os.R_OK) and os.access(PIPELINE_CONF_FILE, os.W_OK):
            logger.info(f"Permission check for {PIPELINE_CONF_FILE} returned OK.")
        else:
            logger.error(f"Permission check for {PIPELINE_CONF_FILE} returned FAIL.")
            system_ready = False

        if os.access(DEFAULTS_CONF_FILE, os.R_OK):
            logger.info(f"Permission check for {DEFAULTS_CONF_FILE} returned OK.")
        else:
            logger.error(f"Permission check for {DEFAULTS_CONF_FILE} returned FAIL.")
            system_ready = False

        if not os.path.exists(self.PIDDIR):
            try:
                os.makedirs(self.PIDDIR)

            except PermissionError as e:  # pragma: no cover
                logger.error(f"Directory {self.PIDDIR} does not exist and cannot be created: {e}.")
                system_ready = False

        # del(logger)

        if not external_logger:
            logger.handlers = []

        return system_ready

    def bots_status(self, bots: list = None) -> dict:

        if bots is None:
            bots = self.runtime_configuration.keys()

        status = dict()
        intelmqctl_pid = os.getpid()

        for bot_id in bots:

            if not self._bot_enabled(bot_id):
                status[bot_id] = "disabled"

            else:
                bot_process = self._get_bot_process(bot_id)
                if bot_process.pid == intelmqctl_pid:
                    status[bot_id] = "stopped"

                elif bot_process.status() not in [psutil.STATUS_STOPPED, psutil.STATUS_DEAD, psutil.STATUS_ZOMBIE]:
                    status[bot_id] = "running"

                else:
                    status[bot_id] = "unknown"

        return status

    def bots_start(self, bots: list = None) -> dict:

        if bots is None:
            bots = self.runtime_configuration.keys()

        bots_status = self.bots_status(bots)

        for bot_id in bots:

            bot_status = bots_status[bot_id]

            if bot_status == "disabled" or bot_status == "running":
                self.logger.info(MESSAGES['running'], bot_id)
                continue

            # we try to terminate in non blocking way, otherwise "failed"
            elif bot_status == "unknown":
                if not self._process_finished(self._get_bot_process(bot_id)):
                    self.logger.error(ERROR_MESSAGES['unknown'], bot_id, 'shieeet')
                    bots_status[bot_id] = "failed"
                    continue

            module = self.runtime_configuration[bot_id]['module']
            cmdargs = [module, bot_id]

            try:
                bot_process = psutil.Popen(cmdargs, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                self._create_pidfile(bot_id, bot_process.pid)  # this can raise FileNotFound!
                bots_status[bot_id] = "starting"
                self.logger.info(MESSAGES["starting"], bot_id)

            except FileNotFoundError:
                bots_status[bot_id] = "failed"
                self.logger.error(ERROR_MESSAGES["not found"], bot_id)

        return bots_status

    def bots_stop(self, bots: list = None) -> dict:

        if bots is None:
            bots = self.runtime_configuration.keys()

        bots_status = self.bots_status(bots)

        for bot_id in bots:
            bot_status = bots_status[bot_id]

            if bot_status == "disabled":
                continue

            if bot_status == "stopped":
                self.logger.info(MESSAGES['stopped'], bot_id)
                continue

            elif bot_status == "unknown":
                if self._process_finished(self._get_bot_process(bot_id)):
                    bots_status[bot_id] = "stopped"
                    self.logger.info(MESSAGES['stopped'], bot_id)
                else:
                    bots_status[bot_id] = "failed"
                    self.logger.error(ERROR_MESSAGES['unknown'], bot_id, 'shieeet')
                continue

            try:
                bot_process = self._get_bot_process(bot_id)
                bot_process.terminate()
                bots_status[bot_id] = "stopping"
                self.logger.info(MESSAGES['stopping'], bot_id)

            except psutil.AccessDenied:
                bots_status[bot_id] = "failed"
                self.logger.error(ERROR_MESSAGES['access denied'], bot_id, 'STOP')

        return bots_status

    def bots_restart(self, bots: list = None) -> dict:

        if bots is None:
            bots = self.runtime_configuration.keys()

        stopping_bots = self.bots_stop(bots)

        # filter a list of bots that are successfully stopping
        stopping_bots = [bot for bot in stopping_bots.keys() if stopping_bots[bot] == "stopping"]

        # check if all stopping bots are no longer running
        for i in range(10):
            stopping_bots_status = self.bots_status(stopping_bots)
            if 'running' not in stopping_bots_status.values():
                break
            time.sleep(0.5)

        return self.bots_start(stopping_bots)

    def bots_logs(self, bots: list = None, lines: int = -1, level: str = DEFAULT_LOGGING_LEVEL) -> dict:

        if bots is None:
            bots = self.runtime_configuration.keys()

        bots_logs = dict()
        logging_level = self.defaults_configuration['logging_level']
        logging_handler = self.defaults_configuration['logging_handler']

        for bot_id in bots:

            if logging_handler == 'file':
                bots_logs[bot_id] = self._get_bot_logs_from_file(bot_id, level, lines)

            elif logging_handler == 'syslog':
                bots_logs[bot_id] = self._get_bot_logs_from_syslog(bot_id, level, lines)

            else:
                self.logger.error(f"Unknow logging handler '{logging_handler}'.")

        return bots_logs

    def bots_reload(self, bots: list = None) -> dict:

        bots_status = self.bots_status(bots)

        for bot_id in bots:

            if bots_status[bot_id] == "running":

                try:
                    bot_process = self._get_bot_process(bot_id)
                    bot_process.send_signal(signal.SIGHUP)
                    bots_status[bot_id] = "reloading"

                except psutil.AccessDenied:
                    bots_status[bot_id] = "failed"
                    self.logger(ERROR_MESSAGES["access denied"], bot_id, 'RELOAD')

        return bots_status

    def bots_queues(self, bots: list = None, include_status: bool = False) -> dict:

        if bots is None:
            bots = self.get_bots()

        bots_queues = {}

        if not include_status:
            bots_queues = {bot_id: self.pipeline_configuration[bot_id] for bot_id in bots if
                           bot_id in self.pipeline_configuration.keys()}

            return bots_queues

        counters = self.queues_status()

        for bot_id, info in self.pipeline_configuration.items():

            if bot_id not in bots:
                continue

            bots_queues[bot_id] = {}

            # source_queue = self.pipeline_configuration.get(bot_id).get()

            if 'source-queue' in info:
                bots_queues[bot_id]['source_queue'] = {info['source-queue']: counters[info['source-queue']]}
                internal_queue_value = counters.get(info['source-queue'] + '-internal', None)
                if internal_queue_value is not None:
                    bots_queues[bot_id]['internal_queue'] = internal_queue_value

            if 'destination-queues' in info:
                bots_queues[bot_id]['destination_queues'] = []
                for dest_queue in utils.flatten_queues(info['destination-queues']):
                    bots_queues[bot_id]['destination_queues'].append({dest_queue: counters[dest_queue]})

        return bots_queues

    def bot_run(self, bot_id, run_subcommand=None, console_type=None, message_action_kind=None, dryrun=None, msg=None,
                show_sent=None, loglevel=None) -> int:

        bot_status = not self.bots_status([bot_id])[bot_id]
        bot_paused = False

        if bot_status == "disabled":
            with self.edit_runtime_configuration() as conf:
                conf[bot_id]["enabled"] = True

        if bot_status == "running":
            self.logger.info("Main instance of the bot is running in the background and will be stopped; "
                             "when finished, we try to relaunch it again. "
                             "You may want to launch: 'intelmqctl stop {}' to prevent this message."
                             .format(bot_id))
            self.bots_stop([bot_id])
            bot_paused = True

        try:
            self._create_pidfile(bot_id, os.getpid())
            BotDebugger(self.runtime_configuration[bot_id], bot_id, run_subcommand,
                        console_type, message_action_kind, dryrun, msg, show_sent,
                        loglevel=loglevel)
            ret = 0

        except KeyboardInterrupt:
            print('Keyboard interrupt.')
            ret = 0

        except SystemExit as e:
            print('Bot exited with code %s.' % e.code)
            ret = e.code

        else:
            self._remove_pidfile(bot_id)

        if bot_paused:
            self.bots_start([bot_id])

        if bot_status == "disabled":
            with self.edit_runtime_configuration() as conf:
                conf[bot_id]["enabled"] = False

        return ret

    def queues_status(self, queues: list = None) -> dict:

        parameters = Parameters()
        for option, value in self.defaults_configuration.items():
            setattr(parameters, option, value)

        pipeline = PipelineFactory.create(parameters, logger=self.logger)
        pipeline.set_queues(None, "source")

        if queues is None:
            queues = self.get_queues(include_internal=pipeline.has_internal_queues)

        pipeline.connect()
        counters = pipeline.count_queued_messages(*queues)
        pipeline.disconnect()

        return counters

    def queues_clear(self, queues: list = None) -> list:

        all_queues = self.get_queues(include_internal=True)

        if queues is None:
            queues = all_queues

        else:
            for queue in queues:
                if queue not in all_queues:
                    # TODO
                    raise ValueError(f"Queue {queue} does not exist.")

        parameters = Parameters()
        for option, value in self.defaults_configuration.items():
            setattr(parameters, option, value)

        pipeline = PipelineFactory.create(parameters, logger=self.logger)
        pipeline.set_queues(None, "source")
        pipeline.connect()

        cleared_queues = list()

        for queue in queues:

            try:
                pipeline.clear_queue(queue)
                cleared_queues.append(queue)

            except Exception:  # pragma: no cover
                # logger.exception("Error while clearing queue %s.", queue)
                # return 1, 'error'
                pass

        pipeline.disconnect()

        return cleared_queues

    def get_bots(self, group_or_bot_id: str = None) -> list:

        if group_or_bot_id is None:
            return sorted(self.runtime_configuration.keys())

        elif group_or_bot_id in BOT_GROUP:
            return sorted([bot_id for bot_id in self.runtime_configuration.keys() if
                           self.runtime_configuration[bot_id].get("group") == BOT_GROUP[group_or_bot_id]])

        elif group_or_bot_id in self.runtime_configuration.keys():
            return [group_or_bot_id]

        else:
            return list()

    def get_queues(self, type_or_queue_id: str = None, include_internal: bool = False) -> list:

        if type_or_queue_id is None:
            type = "all"

        elif type_or_queue_id in QUEUE_TYPE:
            type = type_or_queue_id
            if type == "internal":
                include_internal = True

        elif type_or_queue_id in self.get_queues(include_internal=True):
            return [type_or_queue_id]

        else:
            return list()

        queues = set()

        for bot_id, value in self.pipeline_configuration.items():

            if 'source-queue' in value:
                if type in ["source", "all"]:
                    queues.add(value['source-queue'])

                if include_internal:
                    queues.add(value['source-queue'] + '-internal')

            if 'destination-queues' in value:
                if type in ["destination", "all"]:
                    # flattens ["one", "two"] → {"one", "two"}, {"_default": "one", "other": ["two", "three"]} → {"one", "two", "three"}
                    queues.update(utils.flatten_queues(value['destination-queues']))

        return sorted(queues)

    def _get_bot_pid(self, bot_id) -> int:
        filename = self.PIDFILE.format(bot_id)
        if os.path.isfile(filename):

            with open(filename, 'r') as fp:
                pid = fp.read()

            try:
                return int(pid.strip())

            except ValueError:
                return -1

        return -1

    def _get_bot_process(self, bot_id: str, pid: int = None) -> psutil.Process:
        """
        :type bot_id: str
        :param bot_id:

        :type pid: int
        :param pid: Process ID of the Bot.

        :rtype: psutil.Process
        :return: Returns Bot Process on success or current process on failure.
        """

        if pid is None:
            pid = self._get_bot_pid(bot_id)

        elif not psutil.pid_exists(pid):
            return psutil.Process()

        if pid == -1:
            return psutil.Process()

        module = self.runtime_configuration[bot_id]['module']
        module_path = shutil.which(module)
        intelmqctl_path = shutil.which("intelmqctl")

        if module_path is None:
            self.logger.error(f"Module {module} not in PATH env.")
            return psutil.Process()

        try:
            process = psutil.Process(int(pid))
            argv = process.cmdline()
            argc = len(argv)

            if process.status() in [psutil.STATUS_STOPPED, psutil.STATUS_DEAD, psutil.STATUS_ZOMBIE]:
                if self._process_finished(process):
                    self._remove_pidfile(bot_id)
                    return psutil.Process()
                else:
                    self.logger.error(f"Bot {bot_id} is in {process.status()} state and can not be finished.")

            # bot is running
            if argc > 1 and argv[1] == module_path:
                return process

            # bot is running in interactive mode
            elif argc > 3 and argv[1] == intelmqctl_path and argv[2] == "run" and argv[3] == bot_id:
                return process

            elif argc > 1:
                self.logger.error(f"Commandline of the program {argv[1]} does not match expected value {module_path}.")
                return psutil.Process()

            return process

        except psutil.NoSuchProcess:
            self._remove_pidfile(pid)
            return psutil.Process()

        except psutil.AccessDenied:
            self.logger.error(f"Could not get status of process {pid}: Access denied.")
            return psutil.Process()

    def _create_pidfile(self, bot_id, pid) -> None:
        filename = self.PIDFILE.format(bot_id)
        with open(filename, 'w') as fp:
            fp.write(str(pid))

    def _remove_pidfile(self, bot_id) -> None:
        filename = self.PIDFILE.format(bot_id)
        if os.path.isfile(filename):
            os.remove(filename)

    def _bot_enabled(self, bot_id: str) -> bool:
        return self._runtime_configuration[bot_id].get('enabled', True)

    def _process_finished(self, process: psutil.Process) -> bool:
        try:
            process.wait(timeout=0)
            return True
        except psutil.TimeoutExpired as e:
            self.logger.error(f"Can not finish process {process.pid}: {e}")
            return False

    def _get_bot_logs_from_file(self, bot_id: str, log_level: str, lines: int = -1) -> list:

        logs = list()
        logline_overflow = str()
        bot_logfile = os.path.join(self.defaults_configuration['logging_path'], bot_id + '.log')

        if not os.path.isfile(bot_logfile):
            self.logger.error(f"Log path not found: {bot_logfile}")
            return list()

        if not os.access(bot_logfile, os.R_OK):
            self.logger.error(f"File {bot_logfile} is not readable.")
            return list()

        for line in utils.reverse_readline(bot_logfile):

            logline = utils.parse_logline(line)

            if type(logline) is not dict:
                logline_overflow = '\n'.join([line, logline_overflow])
                continue

            if logline['bot_id'] != bot_id:
                continue

            if LOG_LEVEL[logline['log_level']] < LOG_LEVEL[log_level]:
                continue

            if logline_overflow:
                logline['extended_message'] = logline_overflow
                logline_overflow = str()

            logs.append(logline)

            if lines != -1 and len(logs) >= lines:
                break

        return logs[::-1]

    def _get_bot_logs_from_syslog(self, bot_id: str, log_level: str, lines: int = -1) -> list:

        logs = list()
        bot_logfile = '/var/log/syslog'

        if not os.access(bot_logfile, os.R_OK):
            self.logger.error(f"File {bot_logfile} is not readable.")
            return list()

        for line in utils.reverse_readline(bot_logfile):

            log_message = utils.parse_logline(line, regex=utils.SYSLOG_REGEX)

            if type(log_message) is not dict:
                continue

            if log_message['bot_id'] != bot_id:
                continue

            if LOG_LEVEL[log_message['log_level']] < LOG_LEVEL[log_level]:
                continue

            log_message['message'] = log_message['message'].replace('#012', '\n')

            logs.append(log_message)

            if lines != -1 and len(logs) >= lines:
                break

        return logs
