#!/usr/bin/env python3
# PYTHON_ARGCOMPLETE_OK
# -*- coding: utf-8 -*-
import json
import argparse
import argcomplete

from intelmq.lib.utils import log
from intelmq.lib.controller import IntelMQControllerNG, LOG_LEVEL


def start_service(*args, **kwargs):
    from intelmq.lib.service import run
    run(*args, **kwargs)


class IntelMQArgumentParser(argparse.ArgumentParser):

    def print_help_and_exit(self):
        self.print_help()
        self.exit()


class IntelMQCommandLineInterface:

    def __init__(self):

        self.quiet = False
        self.print_json = False
        self.controller = IntelMQControllerNG(logger=log('intelmqctl', stream=False))

        APPNAME = "intelmqctl"

        DESCRIPTION = """
                    description: intelmqctl is the tool to control intelmq system.

                    Outputs are logged to %s/intelmqctl.log
                    """ % self.controller.defaults_configuration['logging_path']

        EPILOG = """
                    intelmqctl [start|stop|restart|status|reload] --group [collectors|parsers|experts|outputs]
                    intelmqctl [start|stop|restart|status|reload] bot-id
                    intelmqctl [start|stop|restart|status|reload]
                    intelmqctl list [bots|queues|queues-and-status]
                    intelmqctl log bot-id [number-of-lines [log-level]]
                    intelmqctl run bot-id message [get|pop|send]
                    intelmqctl run bot-id process [--msg|--dryrun]
                    intelmqctl run bot-id console
                    intelmqctl clear queue-id
                    intelmqctl check
                    intelmqctl upgrade-config
                    intelmqctl debug

            Starting a bot:
                intelmqctl start bot-id
            Stopping a bot:
                intelmqctl stop bot-id
            Reloading a bot:
                intelmqctl reload bot-id
            Restarting a bot:
                intelmqctl restart bot-id
            Get status of a bot:
                intelmqctl status bot-id

            Run a bot directly for debugging purpose and temporarily leverage the logging level to DEBUG:
                intelmqctl run bot-id
            Get a pdb (or ipdb if installed) live console.
                intelmqctl run bot-id console
            See the message that waits in the input queue.
                intelmqctl run bot-id message get
            See additional help for further explanation.
                intelmqctl run bot-id --help

            Starting the botnet (all bots):
                intelmqctl start
                etc.

            Starting a group of bots:
                intelmqctl start --group experts
                etc.

            Get a list of all configured bots:
                intelmqctl list bots
            If -q is given, only the IDs of enabled bots are listed line by line.

            Get a list of all queues:
                intelmqctl list queues
            If -q is given, only queues with more than one item are listed.

            Get a list of all queues and status of the bots:
                intelmqctl list queues-and-status

            Clear a queue:
                intelmqctl clear queue-id

            Get logs of a bot:
                intelmqctl log bot-id number-of-lines log-level
            Reads the last lines from bot log.
            Log level should be one of DEBUG, INFO, ERROR or CRITICAL.
            Default is INFO. Number of lines defaults to 10, -1 gives all. Result
            can be longer due to our logging format!

            Upgrade from a previous version:
                intelmqctl upgrade-config
            Make a backup of your configuration first, also including bot's configuration files.

            Get some debugging output on the settings and the enviroment (to be extended):
                intelmqctl debug --get-paths
                intelmqctl debug --get-environment-variables
            """

        RETURN_TYPES = ['text', 'json']
        RETURN_TYPE = None
        QUIET = False

        bots_argument_kwargs = {
            "metavar": "BOTS",
            "nargs": argparse.REMAINDER,
            "help": "bots to select",
            "choices": self.controller.get_bots() + self.controller.get_groups()
        }

        self.parser = IntelMQArgumentParser(
            prog=APPNAME,
            description=DESCRIPTION,
            # epilog=EPILOG,
            formatter_class=argparse.RawDescriptionHelpFormatter
        )

        self.parser.set_defaults(func=self.parser.print_help_and_exit)

        self.parser.add_argument('-v', '--version',
                                 action='version',
                                 version=self.controller.version)

        output_formats = self.parser.add_mutually_exclusive_group()
        output_formats.add_argument('-j', '--json', action="store_true", help="print json output")

        self.parser.add_argument('-q', '--quiet',
                                 action='store_true',
                                 help='Quiet mode, useful for reloads initiated '
                                      'scripts like logrotate')

        subcommands = self.parser.add_subparsers(metavar="COMMAND", parser_class=IntelMQArgumentParser)

        # intelmqctl start
        start = subcommands.add_parser('start', help="Start a bot or a group of bots")
        start.set_defaults(func=self.controller.bots_start)
        start.add_argument('bots', **bots_argument_kwargs)

        # intelmqctl status
        status = subcommands.add_parser('status', help="Displays status of bots")
        status.set_defaults(func=self.controller.bots_status)
        status.add_argument('bots', **bots_argument_kwargs)

        # intelmqctl logs
        logs = subcommands.add_parser('logs', help="Displays logs of bots")
        logs.set_defaults(func=self._print_bots_logs)
        logs.add_argument("-n", "--lines", action="store", help="number of lines", type=int, default=-1)
        logs.add_argument("-l", "--level", action="store", help="log level", default="INFO", choices=LOG_LEVEL.keys())
        logs.add_argument('bots', **bots_argument_kwargs)

        # intelmqctl stop
        stop = subcommands.add_parser('stop', help="Stop a bot or a group of bots")
        stop.set_defaults(func=self.controller.bots_stop)
        stop.add_argument('bots', **bots_argument_kwargs)

        # intelmqctl reload
        reload = subcommands.add_parser('reload', help="Reload a bot or a group of bots")
        reload.set_defaults(func=self.controller.bots_reload)
        reload.add_argument('bots', **bots_argument_kwargs)

        # intelmqctl restart
        restart = subcommands.add_parser('restart', help="Restart a bot or a group of bots")
        restart.set_defaults(func=self.controller.bots_restart)
        restart.add_argument('bots', **bots_argument_kwargs)

        # intelmqctl stats
        # stats = subparsers.add_parser('stats', help="View stats")
        # stats.set_defaults(func=self.controller.bots_stats)
        # stats.add_argument('bots', **bots_argument_kwargs)

        # intelmqctl disable
        disable = subcommands.add_parser('disable', help="Disable a bot or a group of bots")
        disable.set_defaults(func=self.controller.bots_disable)
        disable.add_argument("-s", "--stop", action="store_true", help="also stops the bots")
        disable.add_argument('bots', **bots_argument_kwargs)

        # intelmqctl enable
        enable = subcommands.add_parser('enable', help="Enable a bot or a group of bots")
        enable.set_defaults(func=self.controller.bots_enable)
        enable.add_argument("-s", "--start", action="store_true", help="also starts the bots")
        enable.add_argument('bots', **bots_argument_kwargs)

        # intelmqctl bots
        bots = subcommands.add_parser('bots', help='Manage bots')
        bots.set_defaults(func=bots.print_help_and_exit)
        bots_commands = bots.add_subparsers(metavar="COMMAND")

        # intelmqctl cache
        cache = subcommands.add_parser('cache', help="Manage caches")
        cache.set_defaults(func=cache.print_help_and_exit)
        cache_commands = cache.add_subparsers(metavar="COMMAND")

        # intelmqctl config
        config = subcommands.add_parser('config', help="Manage config")
        config.set_defaults(func=config.print_help_and_exit)
        config_commands = config.add_subparsers(metavar="COMMAND")

        # intelmqctl queue
        queue = subcommands.add_parser('queue', help="Manage queues")
        queue.set_defaults(func=queue.print_help_and_exit)
        queue_commands = queue.add_subparsers(metavar="COMMAND")

        # intelmqctl system
        system = subcommands.add_parser('system', help="Manage IntelMQ")
        system.set_defaults(func=system.print_help_and_exit)
        system_commands = system.add_subparsers(metavar="COMMAND")

        # intelmqctl service
        service = subcommands.add_parser('service', help="Manage service")
        service.set_defaults(func=service.print_help_and_exit)
        service_commands = service.add_subparsers(metavar="COMMAND")

        # intelmqctl debug
        debug = subcommands.add_parser('debug', help="Debugging features")
        debug.set_defaults(func=service.print_help_and_exit)
        debug_commands = debug.add_subparsers(metavar="COMMAND")

        # intelmqctl service start
        service_start = service_commands.add_parser('start', help="Starts the service")
        service_start.add_argument("-l", "--host", action="store", help="ip address to listen on", default="0.0.0.0")
        service_start.add_argument("-p", "--port", action="store", help="port to listen on", default=8080, type=int)
        service_start.add_argument("-w", "--workers", action="store", help="number of workers", default=3)
        service_start.add_argument("-d", "--daemon", action="store_true", help="daemonize the service")
        service_start.set_defaults(func=start_service)

        # intelmqctl queue ls
        queue_ls = queue_commands.add_parser('ls', help="list queues")
        queue_ls.set_defaults(func=self.controller.get_queues)
        queue_ls.add_argument('bots', **bots_argument_kwargs)
        queue_ls_arguments = queue_ls.add_mutually_exclusive_group()
        queue_ls_arguments.add_argument('-i', '--internal', action="store_true")
        queue_ls_arguments.add_argument('-a', '--all', action="store_true")

        # intelmqctl queue status
        queue_status = queue_commands.add_parser('status', help="Show status of queues")
        queue_status.set_defaults(func=self.controller.queue_status)
        queue_status.add_argument('bots', **bots_argument_kwargs)
        queue_status_arguments = queue_status.add_mutually_exclusive_group()
        queue_status_arguments.add_argument('-i', '--internal', action="store_true")
        queue_status_arguments.add_argument('-a', '--all', action="store_true")

        # intelmqctl queue clear
        queue_clear = queue_commands.add_parser('clear', help="Clear queues")
        queue_clear.set_defaults(func=self.controller.queue_clear)
        queue_clear.add_argument('bots', **bots_argument_kwargs)
        queue_clear_arguments = queue_clear.add_mutually_exclusive_group()
        queue_clear_arguments.add_argument('-i', '--internal', action="store_true")
        queue_clear_arguments.add_argument('-a', '--all', action="store_true")

        # intelmqctl queue prune
        queue_prune = queue_commands.add_parser('prune', help="remove all orphaned queues")
        queue_prune.set_defaults(func=self.controller.queue_prune())

        argcomplete.autocomplete(self.parser)

    def run(self):

        args = self.parser.parse_args()
        args_dict = vars(args).copy()

        self.quiet = args.quiet
        self.print_json = args.json
        self.controller.log_level = "WARNING" if args.quiet else "INFO"

        del args_dict['json'], args_dict['quiet'], args_dict['func']
        self.controller.logger.setLevel("WARNING" if args.quiet else "INFO")

        result = args.func(**args_dict)

        if type(result) == dict:
            self._print_dict(result)

        elif type(result) == list:
            self._print_list(result)

    def _print_list(self, data: list):
        if self.print_json:
            print(json.dumps(data))
        else:
            for record in data:
                print(record)

    def _print_dict(self, data: dict):
        if self.print_json:
            print(json.dumps(data))
        else:
            for record, val in data.items():
                print('{:<65} {:<15}'.format(record, val))

    def _print_bots_logs(self, *args, **kwargs):
        logs = self.controller.bots_logs(*args, **kwargs)
        if self.print_json:
            print(json.dumps(logs))
        else:
            for record in logs:
                print('{} - {} - {} - {}'.format(record['date'], record['log_level'], record['bot_id'],
                                                 record['message']))


def main():
    IntelMQCommandLineInterface().run()


if __name__ == "__main__":  # pragma: no cover
    main()
