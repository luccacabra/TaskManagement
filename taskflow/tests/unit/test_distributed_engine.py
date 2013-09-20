# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2012 Yahoo! Inc. All Rights Reserved.
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
import unittest

from taskflow.engines.dist_engine import distributed_engine

from taskflow.patterns import graph_flow
from taskflow.patterns import linear_flow

from taskflow.persistence.backends import impl_memory
from taskflow.persistence import logbook
from taskflow import storage as task_storage
from taskflow.utils import persistence_utils

from taskflow import task
from taskflow import test


# INSTALL CELERY TASK ON WORKER
@celery.task(name='test_distributed_engine.myTask')
def myTask(*args, **kwargs):
    print "doin' stuff"
    return {'my_data': True}


@celery.task(name='test_distributed_engine.myOtherTask')
def myOtherTask(*args, **kwargs):
    print "doin' other stuffs"
    return {'other_data': True}


@celery.task(name='test_distributed_engine.brokenTask')
def brokenTask(*args, **kwargs):
    raise Exception("I'm broken. ):")

# ALTERNATIVE INSTALLATION METHOD:
#
# myFlowTask = task.FunctorTask(execute=myTask)
# myOtherTask = task.FunctorTask(execute=myOtherTask)


class Test(unittest.TestCase):

    class myTask(task.Task):
        def execute(*args, **kwargs):
            print "doin' stuff"

    class myOtherTask(task.Task):
        def execute(*args, **kwargs):
            print "doin' other stuffs"

    class brokenTask(task.Task):
        def execute(*args, **kwargs):
            raise Exception("I'm broken. ):")

    def make_broken_flow(self):
        wf = linear_flow.Flow("linear")
        one = self.myTask()
        two = self.brokenTask()

        wf.add(one, two)

        return wf

    def make_linear_data_flow(self):
        wf = linear_flow.Flow("linear")
        one = self.myTask(provides=['data'])
        two = self.myOtherTask(requires=['data'], provides=['other_data'])
        three = self.myTask(requires=['other_data'])

        wf.add(one, two, three)

        return wf

    def make_linear_link_flow(self):
        wf = linear_flow.Flow("linear")
        one = self.myTask()
        two = self.myOtherTask()
        three = self.myTask()

        wf.add(one, two, three)

        return wf

    def make_linear_mix_flow(self):
        wf = linear_flow.Flow("linear")
        one = self.myTask(provides=['data'])
        two = self.myTask(requires=['data'])
        three = self.myTask()

        wf.add(one, two, three)

        return wf

    def make_graph_data_flow(self):
        wf = graph_flow.Flow("graph-data")
        one = self.myTask(provides=['data'])
        two = self.myTask(requires=['data'], provides=['other_data'])
        three = self.myTask(requires=['data'], provides=['other_other_data'])
        four = self.myTask(requires=['other_data', 'other_other_data'])

        wf.add(one, two, three, four)

        return wf

    def make_graph_link_flow(self):
        wf = graph_flow.Flow("graph-link")
        one = self.myTask()
        two = self.myOtherTask()
        three = self.myOtherTask()
        four = self.myTask()

        wf.add(one, two, three, four)
        wf.link(one, two)
        wf.link(one, three)
        wf.link(three, four)
        wf.link(two, four)

        return wf

    def make_graph_mix_flow(self):
        wf = graph_flow.Flow("graph-mix")
        one = self.myTask(provides=['data'])
        two = self.myOtherTask(requires=['data'])
        three = self.myOtherTask(requires=['data'])
        four = self.myTask()

        wf.add(one, two, three, four)
        wf.link(two, four)
        wf.link(three, four)

        return wf

    def test_linear_data_run(self):
        backend = impl_memory.MemoryBackend({})
        flow = self.make_linear_data_flow()
        lb = logbook.LogBook("my-log-book")
        fd = persistence_utils.create_flow_detail(flow,
                                                  book=lb, backend=backend)
        storage = task_storage.Storage(fd)
        distributed_engine.DistributedEngine(flow, storage)

        # engine.run()

    def test_broken_flow(self):
        backend = impl_memory.MemoryBackend({})
        flow = self.make_broken_flow()
        lb = logbook.LogBook("my-log-book")
        fd = persistence_utils.create_flow_detail(flow,
                                                  book=lb, backend=backend)
        storage = task_storage.Storage(fd)
        distributed_engine.DistributedEngine(flow, storage)

        # engine.run()
