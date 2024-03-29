# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2013 Yahoo! Inc. All Rights Reserved.
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

import contextlib
import mock

from taskflow import exceptions
from taskflow.persistence.backends import impl_memory
from taskflow import states
from taskflow import storage
from taskflow import test
from taskflow.utils import persistence_utils as p_utils


class StorageTest(test.TestCase):
    def setUp(self):
        super(StorageTest, self).setUp()
        self.backend = impl_memory.MemoryBackend(conf={})

    def _get_storage(self):
        lb, flow_detail = p_utils.temporary_flow_detail(self.backend)
        return storage.Storage(backend=self.backend, flow_detail=flow_detail)

    def tearDown(self):
        super(StorageTest, self).tearDown()
        with contextlib.closing(self.backend) as be:
            with contextlib.closing(be.get_connection()) as conn:
                conn.clear_all()

    def test_non_saving_storage(self):
        _lb, flow_detail = p_utils.temporary_flow_detail(self.backend)
        s = storage.Storage(flow_detail=flow_detail)  # no backend
        s.add_task('42', 'my task')
        self.assertEquals(s.get_uuid_by_name('my task'), '42')

    def test_add_task(self):
        s = self._get_storage()
        s.add_task('42', 'my task')
        self.assertEquals(s.get_task_state('42'), states.PENDING)

    def test_save_and_get(self):
        s = self._get_storage()
        s.add_task('42', 'my task')
        s.save('42', 5)
        self.assertEquals(s.get('42'), 5)
        self.assertEquals(s.fetch_all(), {})
        self.assertEquals(s.get_task_state('42'), states.SUCCESS)

    def test_save_and_get_other_state(self):
        s = self._get_storage()
        s.add_task('42', 'my task')
        s.save('42', 5, states.FAILURE)
        self.assertEquals(s.get('42'), 5)
        self.assertEquals(s.get_task_state('42'), states.FAILURE)

    def test_get_non_existing_var(self):
        s = self._get_storage()
        s.add_task('42', 'my task')
        with self.assertRaises(exceptions.NotFound):
            s.get('42')

    def test_reset(self):
        s = self._get_storage()
        s.add_task('42', 'my task')
        s.save('42', 5)
        s.reset('42')
        self.assertEquals(s.get_task_state('42'), states.PENDING)
        with self.assertRaises(exceptions.NotFound):
            s.get('42')

    def test_reset_unknown_task(self):
        s = self._get_storage()
        s.add_task('42', 'my task')
        self.assertEquals(s.reset('42'), None)

    def test_fetch_by_name(self):
        s = self._get_storage()
        s.add_task('42', 'my task')
        name = 'my result'
        s.set_result_mapping('42', {name: None})
        s.save('42', 5)
        self.assertEquals(s.fetch(name), 5)
        self.assertEquals(s.fetch_all(), {name: 5})

    def test_fetch_unknown_name(self):
        s = self._get_storage()
        with self.assertRaisesRegexp(exceptions.NotFound,
                                     "^Name 'xxx' is not mapped"):
            s.fetch('xxx')

    def test_fetch_result_not_ready(self):
        s = self._get_storage()
        s.add_task('42', 'my task')
        name = 'my result'
        s.set_result_mapping('42', {name: None})
        with self.assertRaises(exceptions.NotFound):
            s.get(name)
        self.assertEquals(s.fetch_all(), {})

    def test_save_multiple_results(self):
        s = self._get_storage()
        s.add_task('42', 'my task')
        s.set_result_mapping('42', {'foo': 0, 'bar': 1, 'whole': None})
        s.save('42', ('spam', 'eggs'))
        self.assertEquals(s.fetch_all(), {
            'foo': 'spam',
            'bar': 'eggs',
            'whole': ('spam', 'eggs')
        })

    def test_mapping_none(self):
        s = self._get_storage()
        s.add_task('42', 'my task')
        s.set_result_mapping('42', None)
        s.save('42', 5)
        self.assertEquals(s.fetch_all(), {})

    def test_inject(self):
        s = self._get_storage()
        s.inject({'foo': 'bar', 'spam': 'eggs'})
        self.assertEquals(s.fetch('spam'), 'eggs')
        self.assertEquals(s.fetch_all(), {
            'foo': 'bar',
            'spam': 'eggs',
        })

    def test_fetch_meapped_args(self):
        s = self._get_storage()
        s.inject({'foo': 'bar', 'spam': 'eggs'})
        self.assertEquals(s.fetch_mapped_args({'viking': 'spam'}),
                          {'viking': 'eggs'})

    def test_fetch_not_found_args(self):
        s = self._get_storage()
        s.inject({'foo': 'bar', 'spam': 'eggs'})
        with self.assertRaises(exceptions.NotFound):
            s.fetch_mapped_args({'viking': 'helmet'})

    def test_set_and_get_task_state(self):
        s = self._get_storage()
        state = states.PENDING
        s.add_task('42', 'my task')
        s.set_task_state('42', state)
        self.assertEquals(s.get_task_state('42'), state)

    def test_get_state_of_unknown_task(self):
        s = self._get_storage()
        with self.assertRaisesRegexp(exceptions.NotFound, '^Unknown'):
            s.get_task_state('42')

    def test_task_by_name(self):
        s = self._get_storage()
        s.add_task('42', 'my task')
        self.assertEquals(s.get_uuid_by_name('my task'), '42')

    def test_unknown_task_by_name(self):
        s = self._get_storage()
        with self.assertRaisesRegexp(exceptions.NotFound,
                                     '^Unknown task name:'):
            s.get_uuid_by_name('42')

    def test_get_flow_state(self):
        lb, fd = p_utils.temporary_flow_detail(backend=self.backend)
        fd.state = states.INTERRUPTED
        with contextlib.closing(self.backend.get_connection()) as conn:
            fd.update(conn.update_flow_details(fd))
        s = storage.Storage(flow_detail=fd, backend=self.backend)
        self.assertEquals(s.get_flow_state(), states.INTERRUPTED)

    def test_set_and_get_flow_state(self):
        s = self._get_storage()
        s.set_flow_state(states.SUCCESS)
        self.assertEquals(s.get_flow_state(), states.SUCCESS)

    @mock.patch.object(storage.LOG, 'warning')
    def test_result_is_checked(self, mocked_warning):
        s = self._get_storage()
        s.add_task('42', 'my task')
        s.set_result_mapping('42', {'result': 'key'})
        s.save('42', {})
        mocked_warning.assert_called_once_with(
            mock.ANY, 'my task', 'key', 'result')
        with self.assertRaisesRegexp(exceptions.NotFound,
                                     '^Unable to find result'):
            s.fetch('result')

    @mock.patch.object(storage.LOG, 'warning')
    def test_empty_result_is_checked(self, mocked_warning):
        s = self._get_storage()
        s.add_task('42', 'my task')
        s.set_result_mapping('42', {'a': 0})
        s.save('42', ())
        mocked_warning.assert_called_once_with(
            mock.ANY, 'my task', 0, 'a')
        with self.assertRaisesRegexp(exceptions.NotFound,
                                     '^Unable to find result'):
            s.fetch('a')

    @mock.patch.object(storage.LOG, 'warning')
    def test_short_result_is_checked(self, mocked_warning):
        s = self._get_storage()
        s.add_task('42', 'my task')
        s.set_result_mapping('42', {'a': 0, 'b': 1})
        s.save('42', ['result'])
        mocked_warning.assert_called_once_with(
            mock.ANY, 'my task', 1, 'b')
        self.assertEquals(s.fetch('a'), 'result')
        with self.assertRaisesRegexp(exceptions.NotFound,
                                     '^Unable to find result'):
            s.fetch('b')
