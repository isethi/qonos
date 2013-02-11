# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright 2013 Rackspace
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

import uuid
import webob.exc

from qonos.api.v1 import schedule_metadata
from qonos.db.simple import api as db_api
from qonos.tests.unit import utils as unit_utils
from qonos.tests import utils as test_utils


class TestScheduleMetadataApi(test_utils.BaseTestCase):

    def setUp(self):
        super(TestScheduleMetadataApi, self).setUp()
        self.controller = schedule_metadata.\
            ScheduleMetadataController(db_api=db_api)
        self._create_schedules()

    def tearDown(self):
        super(TestScheduleMetadataApi, self).tearDown()
        db_api.reset()

    def _create_schedules(self):
        fixture = {
            'id': unit_utils.SCHEDULE_UUID1,
            'tenant_id': unit_utils.TENANT1,
            'action': 'snapshot',
            'minute': '30',
            'hour': '2',
        }
        self.schedule_1 = db_api.schedule_create(fixture)
        fixture = {
            'id': unit_utils.SCHEDULE_UUID2,
            'tenant_id': unit_utils.TENANT2,
            'action': 'snapshot',
            'minute': '30',
            'hour': '2',
        }
        self.schedule_2 = db_api.schedule_create(fixture)

    def test_create_meta(self):
        request = unit_utils.get_fake_request(method='POST')
        key = 'key1'
        fixture = {'meta': {key: 'value1'}}
        meta = self.controller.create(request, self.schedule_1['id'], fixture)
        self.assertTrue(key in meta['meta'])
        self.assertEqual(meta['meta'][key], fixture['meta'][key])

    def test_create_meta_duplicate(self):
        request = unit_utils.get_fake_request(method='POST')
        key = 'key1'
        fixture = {'meta': {key: 'value1'}}
        meta = self.controller.create(request, self.schedule_1['id'], fixture)
        # Same schedule ID and key conflict
        fixture = {'meta': {key: 'value2'}}
        self.assertRaises(webob.exc.HTTPConflict, self.controller.create,
                          request, self.schedule_1['id'], fixture)

    def test_list_meta(self):
        request = unit_utils.get_fake_request(method='POST')
        fixture = {'meta': {'key1': 'value1'}}
        self.controller.create(request, self.schedule_1['id'], fixture)
        fixture2 = {'meta': {'key2': 'value2'}}
        self.controller.create(request, self.schedule_1['id'], fixture2)
        request = unit_utils.get_fake_request(method='GET')
        metadata = self.controller.list(request, self.schedule_1['id'])
        self.assertEqual(2, len(metadata['metadata']))
        self.assertMetaInList(metadata['metadata'], fixture['meta'])
        self.assertMetaInList(metadata['metadata'], fixture2['meta'])

    def test_get_meta(self):
        request = unit_utils.get_fake_request(method='POST')
        key = 'key1'
        fixture = {'meta': {key: 'value1'}}
        self.controller.create(request, self.schedule_1['id'], fixture)
        request = unit_utils.get_fake_request(method='GET')
        meta = self.controller.get(request, self.schedule_1['id'], 'key1')
        self.assertEqual(1, len(meta['meta']))
        self.assertTrue(key in meta['meta'])
        self.assertEqual(meta['meta'][key], fixture['meta'][key])

    def test_get_meta_schedule_not_found(self):
        request = unit_utils.get_fake_request(method='GET')
        schedule_id = uuid.uuid4()
        self.assertRaises(webob.exc.HTTPNotFound, self.controller.get,
                          request, schedule_id, 'key1')

    def test_get_meta_key_not_found(self):
        request = unit_utils.get_fake_request(method='GET')
        self.assertRaises(webob.exc.HTTPNotFound, self.controller.get,
                          request, self.schedule_1['id'], 'key1')

    def test_delete_meta(self):
        request = unit_utils.get_fake_request(method='POST')
        fixture = {'meta': {'key1': 'value1'}}
        self.controller.create(request, self.schedule_1['id'], fixture)
        request = unit_utils.get_fake_request(method='DELETE')
        self.controller.delete(request, self.schedule_1['id'], 'key1')
        request = unit_utils.get_fake_request(method='GET')
        self.assertRaises(webob.exc.HTTPNotFound, self.controller.get,
                          request, self.schedule_1['id'], 'key1')

    def test_delete_meta_schedule_not_found(self):
        request = unit_utils.get_fake_request(method='DELETE')
        schedule_id = uuid.uuid4()
        self.assertRaises(webob.exc.HTTPNotFound, self.controller.delete,
                          request, schedule_id, 'key1')

    def test_delete_meta_key_not_found(self):
        request = unit_utils.get_fake_request(method='DELETE')
        self.assertRaises(webob.exc.HTTPNotFound, self.controller.delete,
                          request, self.schedule_1['id'], 'key1')

    def test_update_metadata(self):
        request = unit_utils.get_fake_request(method='PUT')
        expected = {'metadata': {'key1': 'value1'}}
        actual = self.controller.update(request, self.schedule_1['id'],
                                        expected)

        self.assertEqual(expected, actual)

    def test_update_metadata_empty(self):
        request = unit_utils.get_fake_request(method='PUT')
        expected = {'metadata': {}}
        actual = self.controller.update(request, self.schedule_1['id'],
                                        expected)

        self.assertEqual(expected, actual)

    def test_update_meta_schedule_not_found(self):
        request = unit_utils.get_fake_request(method='PUT')
        schedule_id = uuid.uuid4()
        fixture = {'metadata': {'key1': 'value1'}}
        self.assertRaises(webob.exc.HTTPNotFound, self.controller.update,
                          request, schedule_id, fixture)
