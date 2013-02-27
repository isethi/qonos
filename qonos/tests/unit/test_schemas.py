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

from qonos.common import exception
import qonos.schemas
from qonos.tests import utils as test_utils


class TestSchema(test_utils.BaseTestCase):

    def setUp(self):
        super(TestSchema, self).setUp()
        properties = {
            'ham': {'type': 'string', 'description': ('name of ham')},
            'eggs': {'type': 'string'},
            'order_size': {'type': 'integer', 'minimum': 1, 'maximum': 10}
        }
        self.schema = qonos.schemas.Schema('basic', properties)

    def test_validate_passes(self):
        obj = {'ham': 'no', 'eggs': 'scrambled', 'order_size': 10}
        self.schema.validate(obj)  # No exception raised

    def test_validate_fails_on_bad_type(self):
        obj = {'eggs': 2}
        self.assertRaises(exception.Invalid, self.schema.validate, obj)

    def test_validate_fails_not_in_range(self):
        obj = {'order_size': 15}
        self.assertRaises(exception.Invalid, self.schema.validate, obj)

    def test_raw_json_schema(self):
        expected = {
            'name': 'basic',
            'properties': {
                'ham': {'type': 'string', 'description': ('name of ham')},
                'eggs': {'type': 'string'},
                'order_size': {'type': 'integer', 'minimum': 1, 'maximum': 10}
            },
        }
        self.assertEqual(self.schema.raw(), expected)
