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

import copy
import webob.exc

from qonos.api.v1 import api_utils
from qonos.common import exception
from qonos.common import timeutils
from qonos.common import utils
import qonos.db
from qonos.openstack.common.gettextutils import _
from qonos.openstack.common import wsgi
from qonos import schemas


class SchedulesController(object):

    def __init__(self, db_api=None, schemas=None):
        self.db_api = db_api or qonos.db.get_api()
        self.schema = schemas or get_schema()

    def _get_request_params(self, request):
        filter_args = {}
        params = request.params
        if params.get('next_run_after') is not None:
            next_run_after = params['next_run_after']
            next_run_after = timeutils.parse_isotime(next_run_after)
            next_run_after = timeutils.normalize_time(next_run_after)
            filter_args['next_run_after'] = next_run_after

        if params.get('next_run_before') is not None:
            next_run_before = params['next_run_before']
            next_run_before = timeutils.parse_isotime(next_run_before)
            next_run_before = timeutils.normalize_time(next_run_before)
            filter_args['next_run_before'] = next_run_before

        if request.params.get('tenant') is not None:
            filter_args['tenant'] = request.params['tenant']

        filter_args['limit'] = params.get('limit')
        filter_args['marker'] = params.get('marker')

        for filter_key in params.keys():
            if filter_key not in filter_args:
                filter_args[filter_key] = params[filter_key]

        return filter_args

    def list(self, request):
        filter_args = self._get_request_params(request)
        try:
            filter_args = utils.get_pagination_limit(filter_args)
            limit = filter_args['limit']
        except exception.Invalid as e:
            raise webob.exc.HTTPBadRequest(explanation=str(e))
        try:
            schedules = self.db_api.schedule_get_all(filter_args=filter_args)
            if len(schedules) != 0 and len(schedules) == limit:
                next_page = '/v1/schedules?marker=%s' % schedules[-1].get('id')
            else:
                next_page = None
        except exception.NotFound:
            msg = _('The specified marker could not be found')
            raise webob.exc.HTTPNotFound(explanation=msg)
        for sched in schedules:
            utils.serialize_datetimes(sched),
            api_utils.serialize_schedule_metadata(sched)
        links = [{'rel': 'next', 'href': next_page}]
        return {'schedules': schedules, 'schedules_links': links}

    def create(self, request, body=None):
        if not body:
            msg = _('The request body must not be empty')
            raise webob.exc.HTTPBadRequest(explanation=msg)
        if not 'schedule' in body:
            msg = _('The request body must contain a "schedule" entity')
            raise webob.exc.HTTPBadRequest(explanation=msg)

        try:
            self.schema.validate(body['schedule'])
        except exception.Invalid as e:
            raise webob.exc.HTTPBadRequest(explanation=str(e))

        api_utils.deserialize_schedule_metadata(body['schedule'])
        values = {}
        values.update(body['schedule'])
        values['next_run'] = api_utils.schedule_to_next_run(body['schedule'])
        schedule = self.db_api.schedule_create(values)

        utils.serialize_datetimes(schedule)
        api_utils.serialize_schedule_metadata(schedule)
        return {'schedule': schedule}

    def get(self, request, schedule_id):
        try:
            schedule = self.db_api.schedule_get_by_id(schedule_id)
            utils.serialize_datetimes(schedule)
            api_utils.serialize_schedule_metadata(schedule)
        except exception.NotFound:
            msg = _('Schedule %s could not be found.') % schedule_id
            raise webob.exc.HTTPNotFound(explanation=msg)
        return {'schedule': schedule}

    def delete(self, request, schedule_id):
        try:
            self.db_api.schedule_delete(schedule_id)
        except exception.NotFound:
            msg = _('Schedule %s could not be found.') % schedule_id
            raise webob.exc.HTTPNotFound(explanation=msg)

    def update(self, request, schedule_id, body):
        if not body:
            msg = _('The request body must not be empty')
            raise webob.exc.HTTPBadRequest(explanation=msg)
        if not 'schedule' in body:
            msg = _('The request body must contain a "schedule" entity')
            raise webob.exc.HTTPBadRequest(explanation=msg)

        try:
            api_utils.deserialize_schedule_metadata(body['schedule'])
            values = {}
            values.update(body['schedule'])
            values = api_utils.check_read_only_properties(values)
            schedule = self.db_api.schedule_update(schedule_id, values)
            # NOTE(ameade): We must recalculate the schedules next_run time
            # since the schedule has changed
            time_keys = ['minute', 'hour', 'month', 'day_of_week',
                         'day_of_month']
            update_next_run = False
            for key in time_keys:
                if key in values:
                    update_next_run = True
                    break

            if update_next_run:
                values = {}
                values['next_run'] = api_utils.schedule_to_next_run(schedule)
                schedule = self.db_api.schedule_update(schedule_id, values)
        except exception.NotFound:
            msg = _('Schedule %s could not be found.') % schedule_id
            raise webob.exc.HTTPNotFound(explanation=msg)
        except exception.Forbidden as e:
            raise webob.exc.HTTPForbidden(explanation=unicode(e))

        utils.serialize_datetimes(schedule)
        api_utils.serialize_schedule_metadata(schedule)
        return {'schedule': schedule}

_BASE_PROPERTIES = {
    'id': {
        'type': 'string',
        'description': _('An identifier for schedule'),
        'pattern': ('^([0-9a-fA-F]){8}-([0-9a-fA-F]){4}-([0-9a-fA-F]){4}'
                            '-([0-9a-fA-F]){4}-([0-9a-fA-F]){12}$'),
    },
    'tenant': {
        'type': 'string',
        'description': _('An identifier for tenant. Can be name or id'),
        'maxLength': 255,
    },
    'action': {
        'type': 'string',
        'description': _('Type of action'),
    },
    'minute': {
        'type': 'integer',
        'description': _('Minute of schedule'),
        'minimum': 0,
        'maximum': 59,
    },
    'hour': {
        'type': 'integer',
        'description': _('Hour of schedule'),
        'minimum': 0,
        'maximum': 23,
    },
    'day_of_month': {
        'type': 'integer',
        'description': _('Day of month of schedule'),
        'minimum': 1,
        'maximum': 31,
    },
    'month': {
        'type': 'integer',
        'description': _('Month of schedule'),
        'minimum': 1,
        'maximum': 12,
    },
    'day_of_week': {
        'type': 'integer',
        'description': _('Day of week of schedule'),
        'minimum': 0,
        'maximum': 7,
    },
    'last_run': {
        'type': 'string',
        'description': _('The last run time of schedule'),
    },
    'next_run': {
        'type': 'string',
        'description': _('The next run time of schedule'),
    }
}


def get_schema():
    properties = copy.deepcopy(_BASE_PROPERTIES)
    schema = schemas.Schema('schedule', properties)
    return schema


def create_resource():
    """QonoS resource factory method."""
    return wsgi.Resource(SchedulesController())
