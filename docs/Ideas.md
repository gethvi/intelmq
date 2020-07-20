# Ideas for future IntelMQ development
1. [Removal of pipeline.conf](#removal-of-pipelineconf)
2. [Changing configuration format to YAML](#changing-configuration-format-to-yaml)
3. [Adding defaults for groups](#adding-defaults-for-groups)
4. [Adding status of bots + force run](#adding-status-of-bots--force-run)
5. [Adding more robust IntelMQProcessManager to lib](#adding-more-robust-intelmqprocessmanager-to-lib)
6. [Adding REST API management interface](#adding-rest-api-management-interface)

## Removal of `pipeline.conf`
### Issue
I believe that names of the queues (such as `danger-rulez-bruteforce-blocker-parser-queue`) do not need to be explicitly defined in a configuration file and could be completely hidden from the user. They should be generated at runtime by the IntelMQ without any user activity.

### Suggested solution
Currently a simple bot `runtime.conf` and `pipeline.conf` looks like this:

**runtime.conf**
```json
{
  "danger-rulez-bruteforce-blocker-parser": {
      "description": "N/A",
      "group": "Parser",
      "module": "intelmq.bots.parsers.danger_rulez.parser",
      "parameters": {}
    },
  "sieve-expert": {
    "description": "This bot filters and modifies events based on a sieve-based language.",
    "group": "Expert",
    "module": "intelmq.bots.experts.sieve.expert",
    "name": "Sieve",
    "parameters": {
      "file": "/opt/intelmq/var/lib/bots/sieve/filter.sieve"
    }
  }
}
```

**pipeline.conf**
```json
{
  "danger-rulez-bruteforce-blocker-parser": {
    "source-queue": "danger-rulez-bruteforce-blocker-parser-queue",
    "destination-queues": [
      "deduplicator-expert-queue"
    ]
  },
  "sieve-expert": {
    "source-queue": "sieve-expert-queue",
    "destination-queues": {
      "cz": "abusix-expert-cz-queue",
      "foreign": "logstash-output-queue"
    }
  }
}
```

My suggested look of `runtime.conf` after the removal of the `pipeline.conf` and adding `destination` (specifying only bot id):

**runtime.conf**
```json
{
  "danger-rulez-bruteforce-blocker-parser": {
    "description": "N/A",
    "group": "Parser",
    "module": "intelmq.bots.parsers.danger_rulez.parser",
    "parameters": {},
    "destination": {
      "_default": "deduplicator-expert"
    }
  },
  "sieve-expert": {
    "description": "This bot filters and modifies events based on a sieve-based language.",
    "group": "Expert",
    "module": "intelmq.bots.experts.sieve.expert",
    "name": "Sieve",
    "parameters": {
      "file": "/opt/intelmq/var/lib/bots/sieve/filter.sieve"
    },
    "destination": {
      "cz": "abusix-expert-cz",
      "foreign": "logstash-output"
    }
  }
}
```

Everything else that is omitted from the original `pipeline.conf` doesn't need to be accessible to the user and can be automatically generated at runtime. For example the source queues are commonly named with the bot-id and appended with `-queue` string. In reality this is what `intelmq-manager` already does when a new bot is added - new queues are only defined by drawing an arrow from one bot to another and no naming is required from the user.

The removal of `pipeline.conf` would greatly reduce the overall configuration complexity for users and could prevent possible misconfigurations.

## Changing configuration format to YAML
### Issue
The `runtime.conf` is hard to read, does not allow easy manual changes (which often result in cryptic syntax error messages) and does not allow comments.

### Suggested solution
Changing format to YAML. This would greatly improve the readability of the configuration, reduce the headaches with having a silly syntax errors in current json format and would allow for comments. Simple example (including previous idea for removal of `pipeline.conf`):
```yaml
danger-rulez-bruteforce-blocker-collector: 
  description: N/A #TODO come up with a meaningful description
  group: Collector
  module: intelmq.bots.collectors.http.collector_http
  parameters: 
    http_url: http://danger.rulez.sk/projects/bruteforceblocker/blist.php
    name: Bruteforce Blocker
    provider: Danger Rulez
    rate_limit: 80220 # the feed is generated every 1337 minutes
  destination: 
    _default: danger-rulez-bruteforce-blocker-parser
``` 
## Adding defaults for groups
### Issue
Currently it is not possible to set default configuration for only a subset of bots. Example: It is not possible to have all collectors run by default with `rate_limit` set to one day, while having every other (non-collector) bot run with `rate_limit` set to `0`.

### Suggested solution
Allow optional group parameters in `defaults.conf`. This could go beyond the basic groups of `Collector, Parser, Expert and Output` and allow for named groups created by user. (This could possibly repurpose the current bot parameter `Group` - the basic bot group can be determined from `module` parameter).
## Adding status of bots + force run
### Issue
When IntelMQ is restarted, every bot drops it's previous `rate_limit` counter and starts again (all bots at once). This is especially inconvenient for containerized environments where this can happen more often. It brings a big resource usage spike at every restart. Also some feeds get collected a not deduplicated with every restart.

### Suggested solution
Every bot should have it's status file where it keeps the time of it's last message processing so that in case of a restart it's `rate_limit` counter can be restored. This would introduce a necessity of having a way to force the bot to run (= reset it's `rate_limit` counter). Currently the simplest approach would be to delete the status file and restart the bot.

## Adding more robust `IntelMQProcessManager` to `lib`

### Issue
Currently the script `intelmqctl` has two classes:
* `IntelMQProcessManager` - simple interface for managing individual bots
* `IntelMQController` - argparser and a lot of additional logic (partially using `IntelMQProcessManager`)

This is a working solution for the current state of having only one management interface (CLI). Adding a different interface is needlessly complex, because these two classes can not be simply used and they overlap with their purpose.

### Suggested solution
All of the management logic implemented in `IntelMQController` should be moved to `IntelMQProcessManager` resulting in a robust (and hopefully documented) and purely python management API. On the other hand the `IntelMQController` should be reduced to merely a CLI wrapper around `IntelMQProcessManager` with no additional logic (or as little as possible). This would allow for easier development of other management interfaces (such as REST API) and reduce the number of necessary lines of code.

## Adding REST API management interface
### Issue
Currently it is not possible to have `intelmq-manager` separated from the intelmq installation because it relies on calling `intelmqctl` CLI and passes arguments in possibly unsafe way.

### Suggested solution
Based on the previous idea for having a separate robust `IntelMQProcessManager` library, this could for a start be as simple as having a HTTP wrapper around `IntelMQProcessManager` calls. Possible REST API endpoints could for example look like this (https://github.com/gethvi/intelmq/blob/rest-api-docs/docs/REST-API.md).

### Notes
This could possibly be implemented and released in intelmq 2.x as it wouldn't break anything and could play a role of experimental feature before the release of 3.0.