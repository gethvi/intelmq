import argparse
import datetime
import distutils.version
import getpass
import http.client
import importlib
import json
import logging
import os
import re
import signal
import socket
import subprocess
import sys
import textwrap
import traceback
import time
import xmlrpc.client
from collections import OrderedDict
import shutil

import pkg_resources
from termstyle import green

from intelmq import (BOTS_FILE, DEFAULT_LOGGING_LEVEL, DEFAULTS_CONF_FILE,
                     HARMONIZATION_CONF_FILE, PIPELINE_CONF_FILE,
                     RUNTIME_CONF_FILE, VAR_RUN_PATH, STATE_FILE_PATH,
                     DEFAULT_LOGGING_PATH, __version_info__,
                     CONFIG_DIR, ROOT_DIR)
from intelmq.lib import utils
from intelmq.lib.bot_debugger import BotDebugger
from intelmq.lib.exceptions import MissingDependencyError
from intelmq.lib.pipeline import PipelineFactory
import intelmq.lib.upgrades as upgrades
from typing import Union, Iterable

try:
    import psutil
except ImportError:
    psutil = None


class Parameters(object):
    pass


STATUSES = {
    'starting': 0,
    'running': 1,
    'stopping': 2,
    'stopped': 3,
}

MESSAGES = {
    'enabled': 'Bot %s is enabled.',
    'disabled': 'Bot %s is disabled.',
    'starting': 'Starting %s...',
    'running': green('Bot %s is running.'),
    'stopped': 'Bot %s is stopped.',
    'stopping': 'Stopping bot %s...',
    'reloading': 'Reloading bot %s ...',
    'enabling': 'Enabling %s.',
    'disabling': 'Disabling %s.',
    'reloaded': 'Bot %s is reloaded.',
    'restarting': 'Restarting %s...',
}

ERROR_MESSAGES = {
    'starting': 'Bot %s failed to START.',
    'running': 'Bot %s is still running.',
    'stopped': 'Bot %s was NOT RUNNING.',
    'stopping': 'Bot %s failed to STOP.',
    'not found': ('Bot %s FAILED to start because the executable cannot be found. '
                  'Check your PATH variable and your the installation.'),
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


def log_bot_error(status, *args):
    if RETURN_TYPE == 'text':
        logger.error(ERROR_MESSAGES[status], *args)


def log_bot_message(status, *args):
    if QUIET:
        return
    if RETURN_TYPE == 'text':
        logger.info(MESSAGES[status], *args)


def log_botnet_error(status, group=None):
    if RETURN_TYPE == 'text':
        logger.error(ERROR_MESSAGES[status], BOT_GROUP[group] + (" group" if group else ""))


def log_botnet_message(status, group=None):
    if QUIET:
        return
    if RETURN_TYPE == 'text':
        if group:
            logger.info(MESSAGES[status], BOT_GROUP[group] + " group")
        else:
            logger.info(MESSAGES[status], 'Botnet')


def log_log_messages(messages):
    if RETURN_TYPE == 'text':
        for message in messages:
            print(' - '.join([message['date'], message['bot_id'],
                              message['log_level'], message['message']]))
            try:
                print(message['extended_message'])
            except KeyError:
                pass


class IntelMQProcessManager:
    PIDDIR = VAR_RUN_PATH
    PIDFILE = os.path.join(PIDDIR, "{}.pid")

    def __init__(self, runtime_configuration, logger, controller):
        self.__runtime_configuration = runtime_configuration
        self.logger = logger
        self.controller = controller

        if psutil is None:
            raise MissingDependencyError('psutil')

        if not os.path.exists(self.PIDDIR):
            try:
                os.makedirs(self.PIDDIR)
            except PermissionError as exc:  # pragma: no cover
                self.logger.error('Directory %s does not exist and cannot be '
                                  'created: %s.', self.PIDDIR, exc)

    def bot_run(self, bot_id, run_subcommand=None, console_type=None, message_action_kind=None, dryrun=None, msg=None,
                show_sent=None, loglevel=None):
        pid = self.__check_pid(bot_id)
        module = self.__runtime_configuration[bot_id]['module']
        status = self.__status_process(pid, module, bot_id) if pid else False
        if pid and status is True:
            self.logger.info("Main instance of the bot is running in the background and will be stopped; "
                             "when finished, we try to relaunch it again. "
                             "You may want to launch: 'intelmqctl stop {}' to prevent this message."
                             .format(bot_id))
            paused = True
            self.bot_stop(bot_id)
        elif status is False:
            paused = False
        else:
            self.logger.error(status)
            return 1

        log_bot_message('starting', bot_id)
        filename = self.PIDFILE.format(bot_id)
        with open(filename, 'w') as fp:
            fp.write(str(os.getpid()))

        try:
            BotDebugger(self.__runtime_configuration[bot_id], bot_id, run_subcommand,
                        console_type, message_action_kind, dryrun, msg, show_sent,
                        loglevel=loglevel)
            retval = 0
        except KeyboardInterrupt:
            print('Keyboard interrupt.')
            retval = 0
        except SystemExit as exc:
            print('Bot exited with code %s.' % exc.code)
            retval = exc.code

        self.__remove_pidfile(bot_id)
        if paused:
            self.bot_start(bot_id)
        return retval

    def bot_start(self, bot_id, getstatus=True):
        pid = self.__check_pid(bot_id)
        module = self.__runtime_configuration[bot_id]['module']
        if pid:
            status = self.__status_process(pid, module, bot_id)
            if status is True:
                log_bot_message('running', bot_id)
                return 'running'
            elif status is False:
                self.__remove_pidfile(bot_id)
            else:
                self.logger.error(status)
                return 1

        log_bot_message('starting', bot_id)
        module = self.__runtime_configuration[bot_id]['module']
        cmdargs = [module, bot_id]
        try:
            proc = psutil.Popen(cmdargs, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except FileNotFoundError:
            log_bot_error("not found", bot_id)
            return 'stopped'
        else:
            filename = self.PIDFILE.format(bot_id)
            with open(filename, 'w') as fp:
                fp.write(str(proc.pid))

        if getstatus:
            time.sleep(0.5)
            return self.bot_status(bot_id, proc=proc)

    def bot_stop(self, bot_id, getstatus=True):
        pid = self.__check_pid(bot_id)
        module = self.__runtime_configuration[bot_id]['module']
        if not pid:
            if self.controller._is_enabled(bot_id):
                log_bot_error('stopped', bot_id)
                return 'stopped'
            else:
                log_bot_message('disabled', bot_id)
                return 'disabled'
        status = self.__status_process(pid, module, bot_id)
        if status is False:
            self.__remove_pidfile(bot_id)
            log_bot_error('stopped', bot_id)
            return 'stopped'
        elif status is not True:
            log_bot_error('unknown', bot_id, status)
            return 'unknown'
        log_bot_message('stopping', bot_id)
        proc = psutil.Process(int(pid))
        try:
            proc.send_signal(signal.SIGTERM)
        except psutil.AccessDenied:
            log_bot_error('access denied', bot_id, 'STOP')
            return 'running'
        else:
            if getstatus:
                # Wait for up to 2 seconds until the bot stops, #1434
                starttime = time.time()
                remaining = 2
                status = self.__status_process(pid, module, bot_id)
                while status is True and remaining > 0:
                    status = self.__status_process(pid, module, bot_id)
                    time.sleep(0.1)
                    remaining = 2 - (time.time() - starttime)

                if status is True:
                    log_bot_error('running', bot_id)
                    return 'running'
                elif status is not False:
                    log_bot_error('unknown', bot_id, status)
                    return 'unknown'
                try:
                    self.__remove_pidfile(bot_id)
                except FileNotFoundError:  # Bot was running interactively and file has been removed already
                    pass
                log_bot_message('stopped', bot_id)
                return 'stopped'

    def bot_reload(self, bot_id, getstatus=True):
        pid = self.__check_pid(bot_id)
        module = self.__runtime_configuration[bot_id]['module']
        if not pid:
            if self.controller._is_enabled(bot_id):
                log_bot_error('stopped', bot_id)
                return 'stopped'
            else:
                log_bot_message('disabled', bot_id)
                return 'disabled'
        status = self.__status_process(pid, module, bot_id)
        if status is False:
            self.__remove_pidfile(bot_id)
            log_bot_error('stopped', bot_id)
            return 'stopped'
        elif status is not True:
            log_bot_error('unknown', bot_id, status)
            return 'unknown'
        log_bot_message('reloading', bot_id)
        proc = psutil.Process(int(pid))
        try:
            proc.send_signal(signal.SIGHUP)
        except psutil.AccessDenied:
            log_bot_error('access denied', bot_id, 'RELOAD')
            return 'running'
        else:
            if getstatus:
                time.sleep(0.5)
                status = self.__status_process(pid, module, bot_id)
                if status is True:
                    log_bot_message('running', bot_id)
                    return 'running'
                elif status is False:
                    log_bot_error('stopped', bot_id)
                    return 'stopped'
                else:
                    log_bot_error('unknown', bot_id, status)
                    return 'unknown'

    def bot_status(self, bot_id, *, proc=None):
        if proc:
            if proc.status() not in [psutil.STATUS_STOPPED, psutil.STATUS_DEAD, psutil.STATUS_ZOMBIE]:
                log_bot_message('running', bot_id)
                return 'running'
        else:
            pid = self.__check_pid(bot_id)
            module = self.__runtime_configuration[bot_id]['module']
            status = self.__status_process(pid, module, bot_id) if pid else False
            if pid and status is True:
                log_bot_message('running', bot_id)
                return 'running'
            elif status is not False:
                log_bot_error('unknown', bot_id, status)
                return 'unknown'

        if self.controller._is_enabled(bot_id):
            if not proc and pid:
                self.__remove_pidfile(bot_id)
            log_bot_message('stopped', bot_id)
            if proc and RETURN_TYPE == 'text':
                log = proc.stderr.read().decode()
                if not log:  # if nothing in stderr, print stdout
                    log = proc.stdout.read().decode()
                print(log.strip(), file=sys.stderr)
            return 'stopped'
        else:
            log_bot_message('disabled', bot_id)
            return 'disabled'

    def __check_pid(self, bot_id):
        filename = self.PIDFILE.format(bot_id)
        if os.path.isfile(filename):
            with open(filename, 'r') as fp:
                pid = fp.read()
            try:
                return int(pid.strip())
            except ValueError:
                return None
        return None

    def __remove_pidfile(self, bot_id):
        filename = self.PIDFILE.format(bot_id)
        os.remove(filename)

    @staticmethod
    def _interpret_commandline(pid: int, cmdline: Iterable[str],
                               module: str, bot_id: str) -> Union[bool, str]:
        """
        Separate function to allow easy testing

        Parameters
        ----------
        pid : int
            Process ID, used for return values (error messages) only.
        cmdline : Iterable[str]
            The command line of the process.
        module : str
            The module of the bot.
        bot_id : str
            The ID of the bot.

        Returns
        -------
        Union[bool, str]
            DESCRIPTION.
        """
        if len(cmdline) > 2 and cmdline[1].endswith('/%s' % module):
            if cmdline[2] == bot_id:
                return True
            else:
                return False
        elif (len(cmdline) > 3 and cmdline[1].endswith('/intelmqctl') and
              cmdline[2] == 'run'):
            if cmdline[3] == bot_id:
                return True
            else:
                return False
        elif len(cmdline) > 1:
            return 'Commandline of the process %d with commandline %r could not be interpreted.' % (pid, cmdline)
        else:
            return 'Unhandled error checking the process %d with commandline %r.' % (pid, cmdline)

    def __status_process(self, pid, module, bot_id):
        try:
            proc = psutil.Process(int(pid))
            cmdline = proc.cmdline()
            return IntelMQProcessManager._interpret_commandline(pid, cmdline, module, bot_id)
        except psutil.NoSuchProcess:
            return False
        except psutil.AccessDenied:
            return 'Could not get status of process: Access denied.'
        except:
            raise


class SupervisorProcessManager:
    class RpcFaults:
        UNKNOWN_METHOD = 1
        INCORRECT_PARAMETERS = 2
        BAD_ARGUMENTS = 3
        SIGNATURE_UNSUPPORTED = 4
        SHUTDOWN_STATE = 6
        BAD_NAME = 10
        BAD_SIGNAL = 11
        NO_FILE = 20
        NOT_EXECUTABLE = 21
        FAILED = 30
        ABNORMAL_TERMINATION = 40
        SPAWN_ERROR = 50
        ALREADY_STARTED = 60
        NOT_RUNNING = 70
        SUCCESS = 80
        ALREADY_ADDED = 90
        STILL_RUNNING = 91
        CANT_REREAD = 92

    class ProcessState:
        STOPPED = 0
        STARTING = 10
        RUNNING = 20
        BACKOFF = 30
        STOPPING = 40
        EXITED = 100
        FATAL = 200
        UNKNOWN = 1000

        @staticmethod
        def is_running(state: int) -> bool:
            return state in (
                SupervisorProcessManager.ProcessState.STARTING,
                SupervisorProcessManager.ProcessState.RUNNING,
                SupervisorProcessManager.ProcessState.BACKOFF)

    DEFAULT_SOCKET_PATH = "/var/run/supervisor.sock"
    SUPERVISOR_GROUP = "intelmq"
    __supervisor_xmlrpc = None

    def __init__(self, runtime_configuration: dict, logger: logging.Logger, controller) -> None:
        self.__runtime_configuration = runtime_configuration
        self.__logger = logger
        self.__controller = controller

    def bot_run(self, bot_id, run_subcommand=None, console_type=None, message_action_kind=None, dryrun=None, msg=None,
                show_sent=None, loglevel=None):
        paused = False
        state = self._get_process_state(bot_id)
        if state in (self.ProcessState.STARTING, self.ProcessState.RUNNING, self.ProcessState.BACKOFF):
            self.__logger.warning("Main instance of the bot is running in the background and will be stopped; "
                                  "when finished, we try to relaunch it again. "
                                  "You may want to launch: 'intelmqctl stop {}' to prevent this message."
                                  .format(bot_id))
            paused = True
            self.bot_stop(bot_id)

        log_bot_message("starting", bot_id)

        try:
            BotDebugger(self.__runtime_configuration[bot_id], bot_id, run_subcommand,
                        console_type, message_action_kind, dryrun, msg, show_sent,
                        loglevel=loglevel)
            retval = 0
        except KeyboardInterrupt:
            print("Keyboard interrupt.")
            retval = 0
        except SystemExit as exc:
            print("Bot exited with code %s." % exc.code)
            retval = exc.code

        if paused:
            self.bot_start(bot_id)

        return retval

    def bot_start(self, bot_id: str, getstatus: bool = True):
        state = self._get_process_state(bot_id)
        if state is not None:
            if state == self.ProcessState.RUNNING:
                log_bot_message("running", bot_id)
                return "running"

            elif not self.ProcessState.is_running(state):
                self._remove_bot(bot_id)

        log_bot_message("starting", bot_id)
        self._create_and_start_bot(bot_id)

        if getstatus:
            return self.bot_status(bot_id)

    def bot_stop(self, bot_id: str, getstatus: bool = True):
        state = self._get_process_state(bot_id)
        if state is None:
            if not self.__controller._is_enabled(bot_id):
                log_bot_message("disabled", bot_id)
                return "disabled"
            else:
                log_bot_error("stopped", bot_id)
                return "stopped"

        if not self.ProcessState.is_running(state):
            self._remove_bot(bot_id)
            log_bot_error("stopped", bot_id)
            return "stopped"

        log_bot_message("stopping", bot_id)

        self._get_supervisor().supervisor.stopProcess(self._process_name(bot_id))
        self._remove_bot(bot_id)

        if getstatus:
            return self.bot_status(bot_id)

    def bot_reload(self, bot_id: str, getstatus: bool = True):
        state = self._get_process_state(bot_id)
        if state is None:
            if not self.__controller._is_enabled(bot_id):
                log_bot_message("disabled", bot_id)
                return "disabled"
            else:
                log_bot_error("stopped", bot_id)
                return "stopped"

        if not self.ProcessState.is_running(state):
            self._remove_bot(bot_id)
            log_bot_error("stopped", bot_id)
            return "stopped"

        log_bot_message("reloading", bot_id)

        try:
            self._get_supervisor().supervisor.signalProcess(self._process_name(bot_id), "HUP")
        except xmlrpc.client.Fault as e:
            if e.faultCode == self.RpcFaults.UNKNOWN_METHOD:
                self._abort("Supervisor does not support signalProcess method, that was added in supervisor 3.2.0. "
                            "Reloading bots will not work.")
            else:
                raise e

        if getstatus:
            return self.bot_status(bot_id)

    def bot_status(self, bot_id: str) -> str:
        state = self._get_process_state(bot_id)
        if state is None:
            if not self.__controller._is_enabled(bot_id):
                log_bot_message("disabled", bot_id)
                return "disabled"
            else:
                log_bot_message("stopped", bot_id)
                return "stopped"

        if state == self.ProcessState.STARTING:
            # If process is still starting, try check it later
            time.sleep(0.1)
            return self.bot_status(bot_id)

        elif state == self.ProcessState.RUNNING:
            log_bot_message("running", bot_id)
            return "running"

        elif state == self.ProcessState.STOPPING:
            log_bot_error("stopping", bot_id)
            return "stopping"

        else:
            log_bot_message("stopped", bot_id)
            return "stopped"

    def _create_and_start_bot(self, bot_id: str) -> None:
        module = self.__runtime_configuration[bot_id]["module"]
        cmdargs = (module, bot_id)

        self._get_supervisor().twiddler.addProgramToGroup(self.SUPERVISOR_GROUP, bot_id, {
            "command": " ".join(cmdargs),
            "stopsignal": "INT",
        })

    def _remove_bot(self, bot_id: str) -> None:
        self._get_supervisor().twiddler.removeProcessFromGroup(self.SUPERVISOR_GROUP, bot_id)

    def _get_process_state(self, bot_id: str):
        try:
            return self._get_supervisor().supervisor.getProcessInfo(self._process_name(bot_id))["state"]
        except xmlrpc.client.Fault as e:
            if e.faultCode == self.RpcFaults.BAD_NAME:  # Process does not exists
                return None
            raise

    def _get_supervisor(self) -> xmlrpc.client.ServerProxy:
        class UnixStreamHTTPConnection(http.client.HTTPConnection):
            def connect(self):
                self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                self.sock.connect(self.host)

        class UnixStreamTransport(xmlrpc.client.Transport, object):
            def __init__(self, socket_path):
                self.socket_path = socket_path
                super(UnixStreamTransport, self).__init__()

            def make_connection(self, host):
                return UnixStreamHTTPConnection(self.socket_path)

        if not self.__supervisor_xmlrpc:
            socket_path = os.environ.get("SUPERVISOR_SOCKET", self.DEFAULT_SOCKET_PATH)

            if not os.path.exists(socket_path):
                self._abort("Socket '{}' does not exists. Is supervisor running?".format(socket_path))

            if not os.access(socket_path, os.W_OK):
                current_user = getpass.getuser()
                self._abort("Socket '{}' is not writable. "
                            "Has user '{}' write permission?".format(socket_path, current_user))

            self.__supervisor_xmlrpc = xmlrpc.client.ServerProxy(
                "http://none",
                transport=UnixStreamTransport(socket_path)
            )

            supervisor_version = self.__supervisor_xmlrpc.supervisor.getSupervisorVersion()
            self.__logger.debug("Connected to supervisor {} named '{}' (API version {})".format(
                supervisor_version,
                self.__supervisor_xmlrpc.supervisor.getIdentification(),
                self.__supervisor_xmlrpc.supervisor.getAPIVersion()
            ))

            if distutils.version.StrictVersion(supervisor_version) < distutils.version.StrictVersion("3.2.0"):
                self.__logger.warning("Current supervisor version is supported, but reloading bots will not work. "
                                      "Please upgrade supervisor to version 3.2.0 or higher.")

            supervisor_state = self.__supervisor_xmlrpc.supervisor.getState()["statename"]
            if supervisor_state != "RUNNING":
                raise Exception("Unexpected supervisor state {}".format(supervisor_state))

            try:
                self.__supervisor_xmlrpc.twiddler.getAPIVersion()
            except xmlrpc.client.Fault as e:
                if e.faultCode == self.RpcFaults.UNKNOWN_METHOD:
                    self._abort("Twiddler is not supported. Is Twiddler for supervisor installed and enabled?")
                else:
                    raise e

            if self.SUPERVISOR_GROUP not in self.__supervisor_xmlrpc.twiddler.getGroupNames():
                self._abort("Supervisor`s process group '{}' is not defined. "
                            "It must be created manually in supervisor config.".format(self.SUPERVISOR_GROUP))

        return self.__supervisor_xmlrpc

    def _process_name(self, bot_id: str) -> str:
        return "{}:{}".format(self.SUPERVISOR_GROUP, bot_id)

    def _abort(self, message: str):
        self.__controller.abort(message)


class IntelMQProcessManagerNG:
    PID_DIR = VAR_RUN_PATH
    PID_FILE = os.path.join(PID_DIR, "{}.pid")

    def __init__(self, controller):

        self.controller = controller

        if psutil is None:
            raise MissingDependencyError('psutil')

        if not os.path.exists(self.PID_DIR):
            try:
                os.makedirs(self.PID_DIR)
            except PermissionError as exc:  # pragma: no cover
                self.controller.logger.error('Directory %s does not exist and cannot be '
                                             'created: %s.', self.PID_DIR, exc)

    def bot_run(self, bot_id, run_subcommand=None, console_type=None, message_action_kind=None, dryrun=None, msg=None,
                show_sent=None, loglevel=None):
        pid = self.__check_pid(bot_id)
        module = self.controller.runtime_configuration[bot_id]['module']
        status = self.__status_process(pid, module, bot_id) if pid else False
        if pid and status is True:
            self.logger.info("Main instance of the bot is running in the background and will be stopped; "
                             "when finished, we try to relaunch it again. "
                             "You may want to launch: 'intelmqctl stop {}' to prevent this message."
                             .format(bot_id))
            paused = True
            self.bot_stop(bot_id)
        elif status is False:
            paused = False
        else:
            self.logger.error(status)
            return 1

        log_bot_message('starting', bot_id)
        filename = self.PID_FILE.format(bot_id)
        with open(filename, 'w') as fp:
            fp.write(str(os.getpid()))

        try:
            BotDebugger(self.__runtime_configuration[bot_id], bot_id, run_subcommand,
                        console_type, message_action_kind, dryrun, msg, show_sent,
                        loglevel=loglevel)
            retval = 0
        except KeyboardInterrupt:
            print('Keyboard interrupt.')
            retval = 0
        except SystemExit as exc:
            print('Bot exited with code %s.' % exc.code)
            retval = exc.code

        self.__remove_pidfile(bot_id)
        if paused:
            self.bot_start(bot_id)
        return retval

    def bot_start(self, bot_id: str, getstatus: bool = True) -> str:

        bot_status = self.bot_status(bot_id)

        if bot_status == "disabled" or bot_status == "running":
            return bot_status

        # we try to terminate in non blocking way, otherwise "failed"
        elif bot_status == "unknown":
            if not self._process_finished(self._get_bot_process(bot_id)):
                self.controller.logger.error(ERROR_MESSAGES['unknown'], bot_id, 'shieeet')
                return "failed"

        module = self.controller.runtime_configuration[bot_id]['module']
        cmdargs = [module, bot_id]

        try:
            bot_process = psutil.Popen(cmdargs, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            self._create_pidfile(bot_id, bot_process.pid)  # this can raise FileNotFound!
            self.controller.logger.info(MESSAGES["starting"], bot_id)
            return "starting"

        except FileNotFoundError:
            self.controller.logger.error(ERROR_MESSAGES["not found"], bot_id)
            return "failed"

    def bot_stop(self, bot_id, getstatus: bool = True) -> str:

        bot_status = self.bot_status(bot_id)

        if bot_status == "disabled" or bot_status == "stopped":
            return bot_status

        if bot_status == "unknown":
            if self._process_finished(self._get_bot_process(bot_id)):
                return "stopped"
            else:
                self.controller.logger.error(ERROR_MESSAGES['unknown'], bot_id, 'shieeet')
                return "failed"

        try:
            bot_process = self._get_bot_process(bot_id)
            bot_process.terminate()
            self.controller.logger.info(MESSAGES['stopping'], bot_id)
            return "stopping"

        except psutil.AccessDenied:
            self.controller.logger.error(ERROR_MESSAGES['access denied'], bot_id, 'STOP')
            return "failed"

    def bot_reload(self, bot_id, getstatus: bool = True) -> str:

        bot_status = self.bot_status(bot_id)

        if bot_status == "running":

            try:
                bot_process = self._get_bot_process(bot_id)
                bot_process.send_signal(signal.SIGHUP)
                return "reloading"

            except psutil.AccessDenied:
                self.controller.logger(ERROR_MESSAGES["access denied"], bot_id, 'RELOAD')
                return "failed"

        return bot_status

    def bot_status(self, bot_id) -> str:

        bot_process = self._get_bot_process(bot_id)

        if bot_process:
            if bot_process.status() not in [psutil.STATUS_STOPPED, psutil.STATUS_DEAD, psutil.STATUS_ZOMBIE]:
                return "running"
            else:
                return "unknown"
        else:
            if self._bot_enabled(bot_id):
                return "stopped"
            else:
                return "disabled"

    def _get_bot_pid(self, bot_id: str) -> int:
        """
        :type bot_id: str
        :param bot_id: Bot ID
        :rtype: int
        :return: Returns Process ID of bot or -1 on failure.
        """

        filename = self.PID_FILE.format(bot_id)
        if os.path.isfile(filename):

            with open(filename, 'r') as fp:
                pid = fp.read()

            try:
                return int(pid.strip())

            except ValueError:
                return -1

        return -1

    def _get_bot_process(self, bot_id: str, pid: int = None) -> Union[psutil.Process, None]:
        """
        :type bot_id: str
        :param bot_id: Bot ID
        :type pid: int
        :param pid: Process ID of the Bot.
        :rtype: Union[psutil.Process, None]
        :return: Returns bot Process on success or None on failure.
        """

        if pid is None:
            pid = self._get_bot_pid(bot_id)

        # PID file doesn't exist or is corrupt
        if pid == -1:
            return None

        # no process with this PID is running
        elif not psutil.pid_exists(pid):
            self._remove_pidfile(bot_id)
            return None

        module = self.controller.runtime_configuration[bot_id]['module']
        module_path = shutil.which(module)
        intelmqctl_path = shutil.which("intelmqctl")

        if module_path is None:
            self.controller.logger.error(f"Module {module} not in PATH env.")
            return None

        try:
            process = psutil.Process(pid)
            argv = process.cmdline()
            argc = len(argv)

            # bot is running normally or in interactive mode
            if argc > 2 and argv[1] == module_path and argv[2] == bot_id or \
                    argc > 3 and argv[1] == intelmqctl_path and argv[2] == "run" and argv[3] == bot_id:

                if process.status() in [psutil.STATUS_STOPPED, psutil.STATUS_DEAD, psutil.STATUS_ZOMBIE]:
                    if self._process_finished(process):
                        self._remove_pidfile(bot_id)
                        return None
                    else:
                        # TODO kill it with fire?
                        self.controller.logger.error(
                            f"Bot {bot_id} is in {process.status()} state and can not be finished.")

                return process

            else:
                self._remove_pidfile(bot_id)
                return None

            # # bot is running
            # if argc > 1 and argv[1] == module_path:
            #     return process
            #
            # # bot is running in interactive mode
            # elif argc > 3 and argv[1] == intelmqctl_path and argv[2] == "run" and argv[3] == bot_id:
            #     return process
            #
            # elif argc > 1:
            #     self.ctl.logger.error(
            #         "Commandline of the program {0} does not match expected value {1}.".format(argv[1], module_path))
            #     return None
            #
            # return process

        except psutil.NoSuchProcess:
            self._remove_pidfile(bot_id)
            return None

        except psutil.AccessDenied:
            self.controller.logger.error(f"Could not get status of process {pid}: Access denied.")
            return None

    def _create_pidfile(self, bot_id: str, pid: Union[int, str]) -> None:
        filename = self.PID_FILE.format(bot_id)
        with open(filename, 'w') as fp:
            fp.write(str(pid))

    def _remove_pidfile(self, bot_id: str) -> None:
        filename = self.PID_FILE.format(bot_id)
        if os.path.isfile(filename):
            os.remove(filename)

    def _process_finished(self, process: psutil.Process) -> bool:
        try:
            process.wait(timeout=0)
            return True
        except psutil.TimeoutExpired as e:
            self.controller.logger.error(f"Can not finish process {process.pid}: {e}")
            return False

    def _bot_enabled(self, bot_id: str) -> bool:
        return self.controller.runtime_configuration[bot_id].get('enabled', True)
