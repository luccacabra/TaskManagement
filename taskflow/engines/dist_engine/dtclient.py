# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2013 Rackspace Hosting All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""Client for Distributed System"""

import collections
import kombu
from kombu import mixins
import logging
import threading
import time

from taskflow import states


LOG = logging.getLogger(__name__)
TASK_EXCHANGE = kombu.Exchange('tasks', type='direct')


class DTClient(object):

    def __init__(self, broker_uri=None):
        self._connection = kombu.Connection(broker_uri)
        self._listeners = collections.defaultdict(list)
        self.requires = collections.defaultdict(set)
        self.provides = collections.defaultdict(set)
        self._is_provided = {}
        self.mapper = {}

    def _check_requires(self, results, callback):
        """Make sure all requires for a task are satisfied before
           kicking off callback, and return accumulated results
        """
        requires = callback.requires
        if requires is None:
            return results
        waiting = []
        accum_results = {}
        for requirement in requires:
            if not (requirement in self._is_provided.keys()):
                waiting.append(requirement)
            else:
                accum_results[requirement] = self._is_provided[requirement]
        if len(waiting) == 0:
            res = callback.celery_task.delay(provides=list(callback.provides),
                                             **accum_results)
            self.mapper[res.id] = callback.task_id
            return True
        else:
            LOG.info("Task %s still waiting on %s" %
                     (callback.task_id, waiting))
            return waiting

    def register_listener(self, data, callback):
        """Register callback as a listener for task or data
           :param data: Data/Task ID that callback is listening for
           :callback: Task to be executed upon data provided
        """

        listener = Listener(self._connection, data, callback,
                            self._check_requires)

        listener_t = threading.Thread(target=listener.run)
        listener_t.daemon = True
        listener_t.start()

        self._listeners[data].append((listener, callback))

    def notify_listeners(self, provides, results):
        """notify listeners of certain data
            :param provides: A set of what this task provides. The set
              contains either data this task provides, the task id
              (task provides itself) or both
            :param results: A dict or other data structure of what this
              task provides. If a dict is used, the client will attempt
              to pass on provided data in a key/value manner
              (result[results][provided] = provided_data)
        """
        # persist all data
        for provided in provides:
            if results['status'] == states.SUCCESS:
                # Is this data already provided?
                if self._is_provided.get(provided):
                    res = self._is_provided[provided]
                    LOG.error("WARNING!! %s Data is already provided,"
                              " and has value %s. OVERWRITING to value"
                              " %s" % (provided, res, results))
                self._is_provided[provided] = (results['results'])
            elif results['status'] == states.ERROR:
                LOG.error("Task has errored")

        # Once we have preserved all new data, notify all listeners
        for provided in provides:
            if results['status'] == states.SUCCESS:
                self._check_active(provided)
                _send_task_results(self._connection, provided,
                                   results['results'])

    def _check_active(self, queuename):
        # Make sure all consumers have had a chance to spin up
        # TODO(Jessica): Won't be a problem in large flows.
        # Maybe only activate loop for flows of certain length?
        for listener in self._listeners[queuename]:
            listener = listener[0]
            try_interval = 1
            while True:
                try_interval *= 2
                if try_interval >= 30:
                    raise Exception("Could not find Listener %s \
                                    for data %s" % (listener, queuename))
                if listener._consuming is False:
                    LOG.error("Listener %s for data %s is not active. \
                              Trying again in %s seconds"
                              % (listener, queuename, try_interval))
                    time.sleep(try_interval)
                else:
                    break
        return True

    def get_listeners(self, data):
        """Return all listeners for given data"""
        results = []

        for (_listener, callback) in self._listeners[data]:
            results.append(callback)

        return results

    def close_listeners(self):
        for listeners in self._listeners.values():
            for _listener, callback in listeners:
                _listener.should_stop = True


class Listener(mixins.ConsumerMixin):
    """Created when a task is registered for notification"""

    def __init__(self, connection, queuename, callback, check):
        self._queue = kombu.Queue(queuename, exchange=TASK_EXCHANGE,
                                  routing_key=queuename)
        self.connection = connection
        # TODO(Jessica): See if callback can be pulled from chain to
        # prevent passing of callback around
        self._callback = callback
        self._check = check
        self._consuming = False

    def on_consume_ready(self, connection, channel, consumers, **kwargs):
        self._consuming = True

    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=[self._queue],
                         callbacks=[self._do_callback])]

    def _do_callback(self, body, message):
        self._check(body, self._callback)
        message.ack()

    def on_consume_end(self, connection, channel):
        connection.release()


def _send_task_results(connection, queuename, results):
    """Send task results to task_id queue"""
    payload = results
    routing_key = queuename
    with kombu.pools.producers[connection].acquire(block=True) as producer:
        kombu.common.maybe_declare(TASK_EXCHANGE, producer.channel)
        producer.publish(payload, serializer='json',
                         exchange=TASK_EXCHANGE,
                         routing_key=routing_key)
