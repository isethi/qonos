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

import webob.exc

from qonos.api.v1 import api_utils
from qonos.common import exception
from qonos.common import utils
import qonos.db
from qonos.openstack.common.gettextutils import _
from qonos.openstack.common import timeutils
from qonos.openstack.common import wsgi


class SchedulesController(object):

    def __init__(self, db_api=None):
        self.db_api = db_api or qonos.db.get_api()

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
        if (body is None or type(body).__name__ != 'dict' or
            body.get('schedule') is None):
            raise webob.exc.HTTPBadRequest()

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
        if body is None or body.get('schedule') is None:
            raise webob.exc.HTTPBadRequest()

        try:
            api_utils.deserialize_schedule_metadata(body['schedule'])
            values = {}
            values.update(body['schedule'])
            schedule = self.db_api.schedule_update(schedule_id,
                                                   values)
            utils.serialize_datetimes(schedule)
            api_utils.serialize_schedule_metadata(schedule)
        except exception.NotFound:
            msg = _('Schedule %s could not be found.') % schedule_id
            raise webob.exc.HTTPNotFound(explanation=msg)
        return {'schedule': schedule}


def create_resource():
    """QonoS resource factory method."""
    return wsgi.Resource(SchedulesController())
