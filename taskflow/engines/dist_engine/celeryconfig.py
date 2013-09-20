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

import logging
import os


LOG = logging.getLogger(__name__)


CELERY_IMPORTS = (os.environ.get('IMPORTS') or
                  ['taskflow.engines.dist_engine.distributed_engine',
                   'taskflow.tests.unit.test_distributed_engine'])
BROKER_URL = (os.environ.get('BROKER_URL') or
              "amqp://guest:guest@localhost:5672//")
CELERY_RESULT_BACKEND = "database"
CELERY_RESULT_DBURI = "sqlite:///"
BROKER_HEARTBEAT = 6
BROKER_HEARTBEAT_CHECKRATE = 2
CELERY_ALWAYS_EAGER = os.environ.get('EAGER') or False
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
