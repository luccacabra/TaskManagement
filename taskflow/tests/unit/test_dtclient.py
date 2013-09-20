# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2013 Rackspace Hosting, Inc. All Rights Reserved.
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
import mox
import os

from celery import result

from taskflow.engines.dist_engine import dist_translator
from taskflow.engines.dist_engine import dtclient

from taskflow import states
from taskflow import task
from taskflow import test

os.environ["EAGER"] = "True"
DT = dist_translator.DistributedTranslator(None)


class DTClientTest(test.TestCase):

    class someTask(task.Task):
        def execute(*args, **kwargs):
            print "Doing some work!"
            return True

    class someOtherTask(task.Task):
        def execute(*args, **kwargs):
            print "Doing some other work!"
            return True

    # Mock out celery functionality
    cel_task = celery.task.Task
    celery_app = celery.Celery()

    celery_mocker = mox.Mox()

    celery_mox = celery_mocker.CreateMock(celery_app)
    task = celery_mocker.CreateMock(cel_task)

    celery_mox.task(mox.IgnoreArg()).AndReturn(task)
    celery_mocker.StubOutWithMock(task, "delay")

    async_mox = celery_mocker.CreateMock(result.AsyncResult)
    async_mox.id = "some_id"
    task.delay(provides=['other', 5678], some='data').AndReturn(async_mox)

    some_task = someTask(provides=['some'])
    some_other_task = someOtherTask(provides=['other'], requires=['some'])

    some_task._id = 1234
    some_other_task._id = 5678

    some_task = DT.HybridTask(some_task, celery_mox)
    some_other_task = DT.HybridTask(some_other_task, celery_mox)

    some_other_task.celery_task = task
    celery_mocker.ReplayAll()

    def setUp(self):
        self.client = dtclient.DTClient("memory://")

    def tearDown(self):
        # Reset listeners
        self.client._listeners = None

    def test_data_register_listener(self):
        """Make sure data relations are correclty wired"""
        self.client.register_listener("some", self.some_other_task)

        listeners = self.client.get_listeners("some")

        self.assertEqual(1, len(listeners))
        self.assertEqual("taskflow.tests.unit.test_dtclient.someOtherTask",
                         listeners[0].name)

    def test_register_listener(self):
        """Make sure tasks get registered appropriately for callacks"""
        self.client.register_listener(self.some_task.task_id,
                                      self.some_other_task)

        listeners = self.client.get_listeners(self.some_task.task_id)

        self.assertEqual(1, len(listeners))
        self.assertEqual("taskflow.tests.unit.test_dtclient.someOtherTask",
                         listeners[0].name)

    def test_notify_listeners(self):
        """Make sure tasks get correct notifications"""

        self.client.register_listener(self.some_task.task_id,
                                      self.some_other_task)

        results = {'status': states.SUCCESS,
                   'results': "some_data"}
        self.client.notify_listeners(provides=[self.some_task.task_id],
                                     results=results)

        self.assertEqual(1, len(self.client._is_provided))
        self.assertEqual({1234: "some_data"}, self.client._is_provided)

    def test_check_requires(self):
        """Make sure tests only fire off when all their requires are met"""

        self.client.register_listener(self.some_task.task_id,
                                      self.some_other_task)

        waiting = self.client._check_requires({}, self.some_other_task)
        self.assertEqual(1, len(waiting))
        self.assertEqual(["some"], waiting)

        self.client._is_provided["some"] = "data"

        print "herp?"
        waiting = self.client._check_requires({}, self.some_other_task)
        self.assertEqual(True, waiting)
