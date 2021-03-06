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

import datetime
from operator import itemgetter
import random

from qonos.common import config
from qonos.common import timeutils
import qonos.db
from qonos.openstack.common import cfg
from qonos.openstack.common import wsgi
from qonos.qonosclient import client
from qonos.qonosclient import exception as client_exc
from qonos.tests import utils as utils


CONF = cfg.CONF

TENANT1 = '6838eb7b-6ded-434a-882c-b344c77fe8df'
TENANT2 = '2c014f32-55eb-467d-8fcb-4bd706012f81'
WORKER = '12345678-9abc-def0-fedc-ba9876543210'


class TestApi(utils.BaseTestCase):

    def setUp(self):
        super(TestApi, self).setUp()
        CONF.paste_deploy.config_file = './etc/qonos/qonos-api-paste.ini'
        self.port = random.randint(50000, 60000)
        self.service = wsgi.Service()
        app = config.load_paste_app('qonos-api')
        self.service.start(app, self.port)
        self.client = client.Client("localhost", self.port)
        self.db_api = qonos.db.get_api()

    def tearDown(self):
        super(TestApi, self).tearDown()

        jobs = self.client.list_jobs()
        for job in jobs:
            self.client.delete_job(job['id'])

        schedules = self.client.list_schedules()
        for schedule in schedules:
            self.client.delete_schedule(schedule['id'])

        workers = self.client.list_workers()
        for worker in workers:
            self.client.delete_worker(worker['id'])

        self.service.stop()
        self.db_api = None

    def test_workers_workflow(self):
        workers = self.client.list_workers()
        self.assertEqual(len(workers), 0)

        # create worker
        worker = self.client.create_worker('hostname')
        self.assertTrue(worker['id'])
        self.assertEqual(worker['host'], 'hostname')

        # get worker
        worker = self.client.get_worker(worker['id'])
        self.assertTrue(worker['id'])
        self.assertEqual(worker['host'], 'hostname')

        # list workers
        workers = self.client.list_workers()
        self.assertEqual(len(workers), 1)
        self.assertEqual(workers[0], worker)

        # get job for worker no jobs for action
        job = self.client.get_next_job(worker['id'], 'snapshot')
        self.assertEqual(job['job'], None)

        # (setup) create schedule
        meta1 = {'key': 'key1', 'value': 'value1'}
        meta2 = {'key': 'key2', 'value': 'value2'}
        request = {
            'schedule':
            {
                'tenant': TENANT1,
                'action': 'snapshot',
                'minute': '30',
                'hour': '12',
                'metadata': {
                    meta1['key']: meta1['value'],
                    meta2['key']: meta2['value'],
                }
            }
        }
        schedule = self.client.create_schedule(request)
        meta_fixture1 = {meta1['key']: meta1['value']}
        meta_fixture2 = {meta2['key']: meta2['value']}

        # (setup) create job
        self.client.create_job(schedule['id'])

        job = self.client.get_next_job(worker['id'], 'snapshot')
        next_job = job['job']
        self.assertNotEqual(next_job.get('id'), None)
        self.assertEqual(next_job['schedule_id'], schedule['id'])
        self.assertEqual(next_job['tenant'], schedule['tenant'])
        self.assertEqual(next_job['action'], schedule['action'])
        self.assertEqual(next_job['status'], 'queued')
        self.assertMetadataInList(next_job['metadata'], meta_fixture1)
        self.assertMetadataInList(next_job['metadata'], meta_fixture2)

        # get job for worker no jobs left for action
        job = self.client.get_next_job(worker['id'], 'snapshot')
        self.assertEqual(job['job'], None)

        # delete worker
        self.client.delete_worker(worker['id'])

        # make sure worker no longer exists
        self.assertRaises(client_exc.NotFound, self.client.get_worker,
                          worker['id'])

    def test_schedule_workflow(self):
        schedules = self.client.list_schedules()
        self.assertEqual(len(schedules), 0)

        # create invalid schedule
        request = {'not a schedule': 'yes'}

        self.assertRaises(client_exc.BadRequest, self.client.create_schedule,
                          request)

        # create malformed schedule
        request = 'not a schedule'

        self.assertRaises(client_exc.BadRequest, self.client.create_schedule,
                          request)

        # create schedule with no body
        self.assertRaises(client_exc.BadRequest, self.client.create_schedule,
                          None)
        # create schedule
        request = {
            'schedule':
            {
                'tenant': TENANT1,
                'action': 'snapshot',
                'minute': 30,
                'hour': 12,
                'metadata': {'instance_id': 'my_instance_1'},
            }
        }
        schedule = self.client.create_schedule(request)
        self.assertTrue(schedule['id'])
        self.assertEqual(schedule['tenant'], TENANT1)
        self.assertEqual(schedule['action'], 'snapshot')
        self.assertEqual(schedule['minute'], 30)
        self.assertEqual(schedule['hour'], 12)
        self.assertTrue('metadata' in schedule)
        metadata = schedule['metadata']
        self.assertEqual(1, len(metadata))
        self.assertEqual(metadata['instance_id'], 'my_instance_1')

        # get schedule
        schedule = self.client.get_schedule(schedule['id'])
        self.assertTrue(schedule['id'])
        self.assertEqual(schedule['tenant'], TENANT1)
        self.assertEqual(schedule['action'], 'snapshot')
        self.assertEqual(schedule['minute'], 30)
        self.assertEqual(schedule['hour'], 12)

        #list schedules
        schedules = self.client.list_schedules()
        self.assertEqual(len(schedules), 1)
        self.assertEqual(schedules[0], schedule)

        #list schedules, next_run filters
        filters = {}
        filters['next_run_after'] = schedule['next_run']
        filters['next_run_before'] = schedule['next_run']
        schedules = self.client.list_schedules(filter_args=filters)
        self.assertEqual(len(schedules), 1)
        self.assertEqual(schedules[0], schedule)

        filters['next_run_after'] = schedule['next_run']
        after_datetime = timeutils.parse_isotime(schedule['next_run'])
        before_datetime = (after_datetime - datetime.timedelta(seconds=1))
        filters['next_run_before'] = timeutils.isotime(before_datetime)
        schedules = self.client.list_schedules(filter_args=filters)
        self.assertEqual(len(schedules), 0)

        #list schedules, next_run_before filters
        filters = {}
        filters['next_run_before'] = schedule['next_run']
        schedules = self.client.list_schedules(filter_args=filters)
        self.assertEqual(len(schedules), 1)
        self.assertEqual(schedules[0], schedule)

        #list schedules, next_run_after filters
        filters = {}
        filters['next_run_after'] = schedule['next_run']
        schedules = self.client.list_schedules(filter_args=filters)
        self.assertEqual(len(schedules), 1)
        self.assertEqual(schedules[0], schedule)

        #list schedules, tenant filters
        filters = {}
        filters['tenant'] = TENANT1
        schedules = self.client.list_schedules(filter_args=filters)
        self.assertEqual(len(schedules), 1)
        self.assertEqual(schedules[0], schedule)
        filters['tenant'] = 'aaaa-bbbb-cccc-dddd'
        schedules = self.client.list_schedules(filter_args=filters)
        self.assertEqual(len(schedules), 0)

        #list schedules, instance_id filters
        filters = {}
        filters['instance_id'] = 'my_instance_1'
        schedules = self.client.list_schedules(filter_args=filters)
        self.assertEqual(len(schedules), 1)
        self.assertEqual(schedules[0], schedule)
        filters['instance_id'] = 'aaaa-bbbb-cccc-dddd'
        schedules = self.client.list_schedules(filter_args=filters)
        self.assertEqual(len(schedules), 0)

        #update schedule
        request = {'schedule': {'hour': 14}}
        updated_schedule = self.client.update_schedule(schedule['id'], request)
        self.assertEqual(updated_schedule['id'], schedule['id'])
        self.assertEqual(updated_schedule['tenant'], schedule['tenant'])
        self.assertEqual(updated_schedule['action'], schedule['action'])
        self.assertEqual(updated_schedule['minute'], schedule['minute'])
        self.assertEqual(updated_schedule['hour'], request['schedule']['hour'])
        self.assertNotEqual(updated_schedule['hour'], schedule['hour'])

        #update schedule metadata
        request = {'schedule': {
                'metadata': {
                    'instance_id': 'my_instance_2',
                    'retention': '3',
                }
            }
        }
        updated_schedule = self.client.update_schedule(schedule['id'], request)
        self.assertEqual(updated_schedule['id'], schedule['id'])
        self.assertEqual(updated_schedule['tenant'], schedule['tenant'])
        self.assertEqual(updated_schedule['action'], schedule['action'])
        self.assertTrue('metadata' in updated_schedule)
        metadata = updated_schedule['metadata']
        self.assertEqual(2, len(metadata))
        self.assertEqual(metadata['instance_id'], 'my_instance_2')
        self.assertEqual(metadata['retention'], '3')

        # delete schedule
        self.client.delete_schedule(schedule['id'])

        # make sure schedule no longer exists
        self.assertRaises(client_exc.NotFound, self.client.get_schedule,
                          schedule['id'])

    def test_schedule_meta_workflow(self):

        # (setup) create schedule
        request = {
            'schedule':
            {
                'tenant': TENANT1,
                'action': 'snapshot',
                'minute': '30',
                'hour': '12'
            }
        }
        schedule = self.client.create_schedule(request)

        metadata_fixture = {'key1': 'value1'}

        #update schedule metadata
        updated_value = self.client.update_schedule_metadata(schedule['id'],
                                                             metadata_fixture)
        self.assertEqual(updated_value, metadata_fixture)

        #list schedule metadata
        updated_value = self.client.list_schedule_metadata(schedule['id'])
        self.assertEqual(updated_value, metadata_fixture)

        #update schedule metadata value
        metadata_fixture = {'key1': 'value2'}
        updated_value = self.client.update_schedule_metadata(schedule['id'],
                                                             metadata_fixture)
        self.assertEqual(updated_value, metadata_fixture)

        #list schedule metadata
        updated_value = self.client.list_schedule_metadata(schedule['id'])
        self.assertEqual(updated_value, metadata_fixture)

        #add schedule metadata
        metadata_fixture = {'key1': 'value2', 'key2': 'value2'}
        updated_value = self.client.update_schedule_metadata(schedule['id'],
                                                             metadata_fixture)
        self.assertEqual(updated_value, metadata_fixture)

        #list schedule metadata
        updated_value = self.client.list_schedule_metadata(schedule['id'])
        self.assertEqual(updated_value, metadata_fixture)

        #remove schedule metadata item
        metadata_fixture = {'key2': 'value2'}
        updated_value = self.client.update_schedule_metadata(schedule['id'],
                                                             metadata_fixture)
        self.assertEqual(updated_value, metadata_fixture)

        #list schedule metadata
        updated_value = self.client.list_schedule_metadata(schedule['id'])
        self.assertEqual(updated_value, metadata_fixture)

    def test_job_workflow(self):

        # (setup) create schedule
        meta1 = {'key': 'key1', 'value': 'value1'}
        meta2 = {'key': 'key2', 'value': 'value2'}
        request = {
            'schedule':
            {
                'tenant': TENANT1,
                'action': 'snapshot',
                'minute': '30',
                'hour': '12',
                'metadata': {
                    meta1['key']: meta1['value'],
                    meta2['key']: meta2['value'],
                }
            }
        }
        schedule = self.client.create_schedule(request)
        meta_fixture1 = {meta1['key']: meta1['value']}
        meta_fixture2 = {meta2['key']: meta2['value']}
        # create job

        new_job = self.client.create_job(schedule['id'])
        self.assertNotEqual(new_job.get('id'), None)
        self.assertEqual(new_job['schedule_id'], schedule['id'])
        self.assertEqual(new_job['tenant'], schedule['tenant'])
        self.assertEqual(new_job['action'], schedule['action'])
        self.assertEqual(new_job['status'], 'queued')
        self.assertEqual(new_job['worker_id'], None)
        self.assertNotEqual(new_job.get('timeout'), None)
        self.assertNotEqual(new_job.get('hard_timeout'), None)
        self.assertMetadataInList(new_job['metadata'], meta_fixture1)
        self.assertMetadataInList(new_job['metadata'], meta_fixture2)

        # ensure schedule was updated
        updated_schedule = self.client.get_schedule(schedule['id'])
        self.assertTrue(updated_schedule.get('last_scheduled'))

        # list jobs
        jobs = self.client.list_jobs()
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]['id'], new_job['id'])
        self.assertEqual(jobs[0]['schedule_id'], new_job['schedule_id'])
        self.assertEqual(jobs[0]['status'], new_job['status'])
        self.assertEqual(jobs[0]['retry_count'], new_job['retry_count'])

        # get job
        job = self.client.get_job(new_job['id'])
        self.assertEqual(job['id'], new_job['id'])
        self.assertEqual(job['schedule_id'], new_job['schedule_id'])
        self.assertEqual(job['status'], new_job['status'])
        self.assertEqual(job['retry_count'], new_job['retry_count'])

        # list job metadata
        metadata = self.client.list_job_metadata(new_job['id'])
        self.assertMetadataInList(metadata, meta_fixture1)
        self.assertMetadataInList(metadata, meta_fixture2)

        # update job metadata
        new_meta = {'foo': 'bar'}
        new_meta.update(meta_fixture1)
        metadata = self.client.update_job_metadata(new_job['id'], new_meta)
        self.assertMetadataInList(metadata, meta_fixture1)
        self.assertMetadataInList(metadata, new_meta)

        # list job metadata
        metadata = self.client.list_job_metadata(new_job['id'])
        self.assertMetadataInList(metadata, meta_fixture1)
        self.assertMetadataInList(metadata, new_meta)

        # update status without timeout
        self.client.update_job_status(job['id'], 'processing')
        status = self.client.get_job(job['id'])['status']
        self.assertNotEqual(status, new_job['status'])
        self.assertEqual(status, 'PROCESSING')

        # update status with timeout
        timeout = '2010-11-30T17:00:00Z'
        self.client.update_job_status(job['id'], 'done', timeout)
        updated_job = self.client.get_job(new_job['id'])
        self.assertNotEqual(updated_job['status'], new_job['status'])
        self.assertEqual(updated_job['status'], 'DONE')
        self.assertNotEqual(updated_job['timeout'], new_job['timeout'])
        self.assertEqual(updated_job['timeout'], timeout)

        # update status with error
        error_message = 'ermagerd! errer!'
        self.client.update_job_status(job['id'], 'error',
                                      error_message=error_message)
        status = self.client.get_job(job['id'])['status']
        self.assertNotEqual(status, new_job['status'])
        self.assertEqual(status, 'ERROR')
        job_fault = self.db_api.job_fault_latest_for_job_id(job['id'])
        self.assertNotEqual(job_fault, None)
        self.assertEqual(job_fault['job_id'], job['id'])
        self.assertEqual(job_fault['tenant'], job['tenant'])
        self.assertEqual(job_fault['schedule_id'], job['schedule_id'])
        self.assertEqual(job_fault['worker_id'],
                         job['worker_id'] or 'UNASSIGNED')
        self.assertEqual(job_fault['action'], job['action'])
        self.assertNotEqual(job_fault['job_metadata'], None)
        self.assertEqual(job_fault['message'], error_message)
        self.assertNotEqual(job_fault['created_at'], None)
        self.assertNotEqual(job_fault['updated_at'], None)
        self.assertNotEqual(job_fault['id'], None)

        # delete job
        self.client.delete_job(job['id'])

        # make sure job no longer exists
        self.assertRaises(client_exc.NotFound, self.client.get_job, job['id'])

    def test_job_meta_workflow(self):

        # (setup) create job
        request = {
            'schedule':
            {
                'tenant': TENANT1,
                'action': 'snapshot',
                'minute': '30',
                'hour': '12'
            }
        }
        schedule = self.client.create_schedule(request)
        # create job

        job = self.client.create_job(schedule['id'])
        self.assertNotEqual(job.get('id'), None)
        self.assertEqual(job['schedule_id'], schedule['id'])
        self.assertEqual(job['tenant'], schedule['tenant'])
        self.assertEqual(job['action'], schedule['action'])
        self.assertEqual(job['status'], 'queued')
        self.assertEqual(job['worker_id'], None)
        self.assertNotEqual(job.get('timeout'), None)
        self.assertNotEqual(job.get('hard_timeout'), None)

        metadata_fixture = {'key1': 'value1'}

        #update job metadata
        updated_value = self.client.update_job_metadata(job['id'],
                                                        metadata_fixture)
        self.assertEqual(updated_value, metadata_fixture)

        #list job metadata
        updated_value = self.client.list_job_metadata(job['id'])
        self.assertEqual(updated_value, metadata_fixture)

        #update job metadata value
        metadata_fixture = {'key1': 'value2'}
        updated_value = self.client.update_job_metadata(job['id'],
                                                             metadata_fixture)
        self.assertEqual(updated_value, metadata_fixture)

        #list job metadata
        updated_value = self.client.list_job_metadata(job['id'])
        self.assertEqual(updated_value, metadata_fixture)

        #add job metadata
        metadata_fixture = {'key1': 'value2', 'key2': 'value2'}
        updated_value = self.client.update_job_metadata(job['id'],
                                                             metadata_fixture)
        self.assertEqual(updated_value, metadata_fixture)

        #list job metadata
        updated_value = self.client.list_job_metadata(job['id'])
        self.assertEqual(updated_value, metadata_fixture)

        #remove job metadata item
        metadata_fixture = {'key2': 'value2'}
        updated_value = self.client.update_job_metadata(job['id'],
                                                             metadata_fixture)
        self.assertEqual(updated_value, metadata_fixture)

        #list job metadata
        updated_value = self.client.list_job_metadata(job['id'])
        self.assertEqual(updated_value, metadata_fixture)

    def test_pagination(self):

        # (setup) create schedule
        meta1 = {'key': 'key1', 'value': 'value1'}
        meta2 = {'key': 'key2', 'value': 'value2'}
        request = {
            'schedule':
            {
                'tenant': TENANT1,
                'action': 'snapshot',
                'minute': '30',
                'hour': '12',
                'schedule_metadata': [
                    meta1,
                    meta2,
                ]
            }
        }
        schedule_1 = self.client.create_schedule(request)
        schedule_2 = self.client.create_schedule(request)
        schedule_3 = self.client.create_schedule(request)
        schedule_4 = self.client.create_schedule(request)
        schedules = [schedule_1, schedule_2, schedule_3, schedule_4]
        schedules = sorted(schedules, key=itemgetter('id'))

        # create worker
        worker_1 = self.client.create_worker('hostname')
        worker_2 = self.client.create_worker('hostname')
        worker_3 = self.client.create_worker('hostname')
        worker_4 = self.client.create_worker('hostname')
        workers = [worker_1, worker_2, worker_3, worker_4]
        workers = sorted(workers, key=itemgetter('id'))

        # create job
        job_1 = self.client.create_job(schedule_1['id'])
        job_2 = self.client.create_job(schedule_2['id'])
        job_3 = self.client.create_job(schedule_3['id'])
        job_4 = self.client.create_job(schedule_4['id'])
        jobs = [job_1, job_2, job_3, job_4]
        jobs = sorted(jobs, key=itemgetter('id'))

        #list schedules
        response = self.client.list_schedules()
        self.assertEqual(len(response), 4)
        response_ids = set(r['id'] for r in response)
        schedule_ids = set(s['id'] for s in schedules)
        self.assertEqual(response_ids, schedule_ids)

        #list schedules with limit
        filter_args = {'limit': '2'}
        response = self.client.list_schedules(filter_args=filter_args)
        self.assertEqual(len(response), 2)
        response_ids = set(r['id'] for r in response)
        schedule_ids = set(s['id'] for s in schedules[0:2])
        self.assertEqual(response_ids, schedule_ids)

        #list schedules with marker
        filter_args = {'marker': schedules[0]['id']}
        response = self.client.list_schedules(filter_args=filter_args)
        self.assertEqual(len(response), 3)
        response_ids = set(r['id'] for r in response)
        schedule_ids = set(s['id'] for s in schedules[1:4])
        self.assertEqual(response_ids, schedule_ids)

        #list schedules with limit and marker
        filter_args = {'limit': '2', 'marker': schedules[0]['id']}
        response = self.client.list_schedules(filter_args=filter_args)
        self.assertEqual(len(response), 2)
        response_ids = set(r['id'] for r in response)
        schedule_ids = set(s['id'] for s in schedules[1:3])
        self.assertEqual(response_ids, schedule_ids)

        # list workers
        response = self.client.list_workers()
        self.assertEqual(len(response), 4)
        response_ids = set(r['id'] for r in response)
        worker_ids = set(w['id'] for w in workers)
        self.assertEqual(response_ids, worker_ids)

        # list workers with limit
        params = {'limit': '2'}
        response = self.client.list_workers(params=params)
        self.assertEqual(len(response), 2)
        response_ids = set(r['id'] for r in response)
        worker_ids = set(w['id'] for w in workers[0:2])
        self.assertEqual(response_ids, worker_ids)

        # list workers with marker
        params = {'marker': workers[0]['id']}
        response = self.client.list_workers(params=params)
        self.assertEqual(len(response), 3)
        response_ids = set(r['id'] for r in response)
        worker_ids = set(w['id'] for w in workers[1:4])
        self.assertEqual(response_ids, worker_ids)

        # list workers with limit and marker
        params = {'marker': workers[0]['id'], 'limit': '2'}
        response = self.client.list_workers(params=params)
        self.assertEqual(len(response), 2)
        response_ids = set(r['id'] for r in response)
        worker_ids = set(w['id'] for w in workers[1:3])
        self.assertEqual(response_ids, worker_ids)

        # list jobs
        response = self.client.list_jobs()
        self.assertEqual(len(response), 4)
        response_ids = set(r['id'] for r in response)
        job_ids = set(j['id'] for j in jobs)
        self.assertEqual(response_ids, job_ids)

        # list jobs with limit
        params = {'limit': '2'}
        response = self.client.list_jobs(params=params)
        self.assertEqual(len(response), 2)
        response_ids = set(r['id'] for r in response)
        job_ids = set(j['id'] for j in jobs[0:2])
        self.assertEqual(job_ids, response_ids)

        # list jobs with marker
        params = {'marker': jobs[0]['id']}
        response = self.client.list_jobs(params=params)
        self.assertEqual(len(response), 3)
        response_ids = set(r['id'] for r in response)
        job_ids = set(j['id'] for j in jobs[1:4])
        self.assertEqual(response_ids, job_ids)

        # list jobs with limit and marker
        params = {'limit': '2', 'marker': jobs[0]['id']}
        response = self.client.list_jobs(params=params)
        self.assertEqual(len(response), 2)
        response_ids = set(r['id'] for r in response)
        job_ids = set(j['id'] for j in jobs[1:3])
        self.assertEqual(job_ids, response_ids)
