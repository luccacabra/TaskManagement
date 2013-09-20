# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2013 Rackspace Hosting Inc. All Rights Reserved.
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

import celery
import logging
import thread
import threading
import time

from celery import events

import taskflow.engines.dist_engine.celeryconfig as CELERY_CONFIG
from taskflow.engines.dist_engine import dist_translator
from taskflow.engines.dist_engine import dtclient
from taskflow import states

LOG = logging.getLogger(__name__)
celery = celery.Celery()
celery.config_from_object(CELERY_CONFIG)


class Events(threading.Thread):
    """Class to capture taskflow events"""

    def __init__(self, app, engine):
        threading.Thread.__init__(self)
        self.daemon = True
        self._celery_app = app
        self._engine = engine
        self._running = False
        self.stop_event = threading.Event()

    def start(self):
        threading.Thread.start(self)

    def run(self):
        """Start event listener"""
        try_interval = 1
        while self.stop_event.is_set() is False:
            try:
                try_interval *= 2

                with self._celery_app.connection() as conn:
                    handlers = {"task-succeeded": (self._engine.
                                                   _task_success_handler),
                                "task-failed": (self._engine.
                                                _task_failure_handler)}
                    recv = events.EventReceiver(conn,
                                                handlers=handlers,
                                                app=self._celery_app)
                    while self.stop_event.is_set() is False:
                        recv.capture(limit=None, timeout=None)
                        self._running = True

                try_interval = 1
            except (KeyboardInterrupt, SystemExit):
                thread.interrupt_main()
            except Exception as e:
                self._running = False
                LOG.error("Failed to capture events: '%s', "
                          "trying again in %s seconds."
                          % (e, try_interval))
                LOG.debug(e, exc_info=True)
                time.sleep(try_interval)

    def stop(self):
        if self.isAlive() is True:
            # set event to signal thread to terminate
            self.stop_event.set()
            # block calling thread until thread really has terminated
            self.join()


class DistributedEngine(object):
    """Distributed engine

       Runs flows based off of task relations, not patterns
    """

    def __init__(self, flow, storage, event_timeout=None):
        self.roots = []
        self.flow = flow
        self._events = None
        self._event_timeout = event_timeout
        self._auto_kill = True
        self.tasks = {}
        self._success_tasks = set([])
        self.storage = storage
        self.client = dtclient.DTClient(CELERY_CONFIG.BROKER_URL)
        self.celery_app = celery

    def _shut_down(self):
        self.client.close_listeners()
        self._events.stop()

    def _change_task_state(self, task_id, state):
        self.storage.set_task_state(task_id, state)

    def _update_task_status(self, task_id, status):
        self.storage.set_task_state(task_id, status)

    def _update_task_result(self, task_id, state, result=None):
        """Update result and change state."""
        if state == states.PENDING:
            self.storage.reset(task_id)
        else:
            self.storage.save(task_id, result, state)

    def _task_success_handler(self, event):
        """Notify all listeners that this task has succesfully
           completed and save results to DB
        """
        res = event['result']
        results = {'status': states.SUCCESS,
                   'results': res}

        uuid = event['uuid']
        task_id = self.client.mapper[uuid]

        self._update_task_result(task_id, states.SUCCESS, result=res)

        self._success_tasks.add(task_id)
        if len(self._success_tasks) is len(self.tasks):
            self._shut_down()
        else:
            self.client.notify_listeners(self.client.provides[task_id],
                                         results)

    def _task_failure_handler(self, event):
        # exc = event['exception']
        # tb = event['traceback']
        uuid = event['uuid']
        task_id = self.client.mapper[uuid]
        self._update_task_status(task_id, states.FAILURE)
        if self._auto_kill:
            self._shut_down()
        #TODO(jlucci): Can we save traceback and exc to storage?

    def _check_event_alive(self, time_out=50):
        running = False

        while running is False and time_out > 0:
            if self._events.isAlive() and self._events._running is True:
                running = True
            else:
                time_out -= 1

        return running

    def run(self, *args, **kwargs):
        # Initialize all task relations for notifications
        dist_translator.DistributedTranslator(self).translate(self.flow)
        self.celery_app.control.enable_events()
        self._events = Events(self.celery_app, self)
        self._events.start()

        running = self._check_event_alive()

        if running:
            for root in self.roots:
                res = root.celery_task.delay(provides=list(root.provides),
                                             *args, **kwargs)
                self.client.mapper[res.id] = root.task_id

            # Don't return until events are complete
            self._events.join(self._event_timeout)
            return

        else:
            LOG.error("Failed to capture events on queue %s"
                      % (CELERY_CONFIG.BROKER_URL))
