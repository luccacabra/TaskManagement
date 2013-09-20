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


from taskflow.openstack.common import uuidutils
from taskflow import states
from taskflow.utils import flow_utils
from taskflow.utils import graph_utils


class DistributedTranslator(object):

    def __init__(self, engine, config=None):
        self.engine = engine

    class HybridTask(object):
        """Task to combine taskflow task and celery task in a way that
           the distributed engine can read. Each flow gets assigned:

           client: mechanism in charge of event notifications
           root tasks: set of root tasks to be fired off synchronously
           celery app: (temporary until pluggable)
        """

        def __init__(self, flow_task, celery):
            self.flow_task = flow_task
            flow_func = flow_task.execute

            #THIS LOGIC TO BE REMOVED ONCE IDS ARE AUTO-GENERATED
            if getattr(flow_task, '_id', None) is None:
                flow_task._id = "%s.%s" % (flow_task.name,
                                           uuidutils.generate_uuid())

            self.name = flow_task.name
            self.celery_task = celery.task(flow_func, name=flow_task.name)
            self.task_id = flow_task._id
            self.celery_task.task_id = self.task_id
            self.requires = flow_task.requires
            self.provides = flow_task.provides
            self.provides.add(self.task_id)

    def _register_callback(self, task):
        """Sets up task to be notified each time one of its' requires
         is satisfied
        """
        self.engine.client.provides[task.task_id].update(task.provides)
        self.engine.client.requires[task.task_id].update(task.requires)

        # If task had already been registered, don't want to set
        # up two requires
        for require in task.requires:
            if task not in self.engine.client.get_listeners(require):
                self.engine.client.register_listener(require, task)

    def _add(self, task):
        """Add independent task to workflow and set up provides/requires
           relations
        """
        self._register_callback(task)

    def _link(self, task, callback):
        """Register a listener for a task"""
        task.provides.add(callback.task_id)
        callback.requires.add(task.task_id)

        # If task already registered, new provides/requires will
        # overwrite old, outdated provides/requires
        self._add(task)
        self._add(callback)

        # self.engine.client.register_listener(task.task_id, callback)

    def _manually_advance(self, provides, data):
        """In the case that a task has failed and it's provides are
            externally (from the flow) are satisfied, allow the
            option to manually advance task

            :param provides: A set of what data/tasks are now provided
            :param data: Data structure of what now is provided
        """

        #TODO(Jessica): Assert provides/results acceptable data structs

        results = {'status': states.SUCCESS,
                   'results': data}

        self.engine.client.notify_listeners(provides, results)

    def translate(self, flow):
        graph = flow_utils.flatten(flow)

        #TODO(jlucci): Logic to be re-written once task_id logic decided on
        for node in graph.nodes():
            if getattr(node, '_id', None) is None:
                node._id = "%s.%s" % (node.name, uuidutils.generate_uuid())
            self.engine.tasks[node._id] = (self.HybridTask(node,
                                           self.engine.celery_app))
            self.engine.storage.add_task(node._id, node.name)

        for (u, v) in graph.edges_iter():
            self._add(self.engine.tasks[u._id])
            self._add(self.engine.tasks[v._id])
            if not (u.provides).intersection(v.requires):
                self._link(self.engine.tasks[u._id],
                           self.engine.tasks[v._id])

        roots = graph_utils.get_no_predecessors(graph)
        for root in roots:
            self.engine.roots.append(self.engine.tasks[root._id])
        return graph
