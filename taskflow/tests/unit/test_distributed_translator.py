# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2013 Rackspace, Inc. All Rights Reserved.
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

from taskflow.engines.dist_engine import dist_translator
from taskflow.engines.dist_engine import distributed_engine
from taskflow.patterns import graph_flow
from taskflow.patterns import linear_flow
from taskflow.persistence.backends import impl_memory
from taskflow.persistence import logbook
from taskflow import storage as task_storage
from taskflow.utils import persistence_utils

from taskflow import task
from taskflow import test


class Test(test.TestCase):

    class someTask(task.Task):
        def execute(*args, **kwargs):
            print "Doing some work!"
            return True

    class someOtherTask(task.Task):
        def execute(*args, **kwargs):
            print "Doing some other work!"
            return True

    def test_linear_translate(self):
        wf = linear_flow.Flow("linear")
        one = self.someTask()
        two = self.someOtherTask()
        wf.add(one, two)

        backend = impl_memory.MemoryBackend({})
        lb = logbook.LogBook("my-log-book")
        fd = persistence_utils.create_flow_detail(wf, book=lb, backend=backend)
        storage = task_storage.Storage(fd)

        engine = distributed_engine.DistributedEngine(wf, storage)
        trans = dist_translator.DistributedTranslator(engine)
        trans.translate(wf)

        listeners = engine.client._listeners
        hybrid = listeners[listeners.keys()[0]][0][1]

        self.assertEqual(1, len(engine.roots))
        self.assertEqual(hybrid.name,
                         "taskflow.tests.unit.test_distributed_translator"
                         ".someOtherTask")

    def test_graph_translate(self):
        wf = graph_flow.Flow("graph")
        one = self.someTask(provides=['data'])
        two = self.someTask(requires=['data'])
        three = self.someTask()
        wf.add(one, two, three)
        wf.link(two, three)

        backend = impl_memory.MemoryBackend({})
        lb = logbook.LogBook("my-log-book")
        fd = persistence_utils.create_flow_detail(wf, book=lb, backend=backend)
        storage = task_storage.Storage(fd)

        engine = distributed_engine.DistributedEngine(wf, storage)
        trans = dist_translator.DistributedTranslator(engine)
        trans.translate(wf)

        self.assertEqual(3, len(engine.tasks))
        self.assertEqual(1, len(engine.roots))

        dict_listeners = engine.client._listeners
        actual = set([])

        for listeners in dict_listeners.values():
            for listener in listeners:
                actual.add(listener[1].flow_task)

        self.assertEquals(actual, set([two, three]))

    def test_multiple_root_translate(self):
        wf = graph_flow.Flow("mult-root")
        one = self.someTask()
        two = self.someTask()
        three = self.someOtherTask()

        wf.add(one, two, three)
        wf.link(one, three)
        wf.link(two, three)

        backend = impl_memory.MemoryBackend({})
        lb = logbook.LogBook("my-log-book")
        fd = persistence_utils.create_flow_detail(wf, book=lb, backend=backend)
        storage = task_storage.Storage(fd)

        engine = distributed_engine.DistributedEngine(wf, storage)
        trans = dist_translator.DistributedTranslator(engine)
        trans.translate(wf)

        self.assertEqual(2, len(engine.roots))
