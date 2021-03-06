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
import datetime
import mox

from qonos.common import timeutils
from qonos.openstack.common import uuidutils
from qonos.tests.unit.worker import fakes
from qonos.tests import utils as test_utils
from qonos.worker.snapshot import snapshot


IMAGE_ID = '66666666-6666-6666-6666-66666666'


class TestSnapshotProcessor(test_utils.BaseTestCase):

    def setUp(self):
        super(TestSnapshotProcessor, self).setUp()
        self.mox = mox.Mox()

        self.nova_client = MockNovaClient()
        self.nova_client.servers = self.mox.CreateMockAnything()
        self.nova_client.images = self.mox.CreateMockAnything()
        self.worker = self.mox.CreateMockAnything()
        self.snapshot_meta = {
            "org.openstack__1__created-by": "scheduled_images_service"
            }

    def tearDown(self):
        self.mox.UnsetStubs()
        super(TestSnapshotProcessor, self).tearDown()

    def _create_images_list(self, instance_id, image_count):
        images = []
        base_time = timeutils.utcnow()
        one_day = datetime.timedelta(days=1)
        for i in range(image_count):
            images.append(self._create_image(instance_id, base_time))
            base_time = base_time - one_day

        return images

    def _create_image(self, instance_id, created, image_id=None,
                      scheduled=True):
        image_id = image_id or uuidutils.generate_uuid()

        image = MockImage(image_id, created, instance_id)

        if scheduled:
            image.metadata['org.openstack__1__created_by'] =\
                'scheduled_images_service'

        return image

    def test_process_job_should_succeed_immediately(self):
        timeutils.set_time_override()
        self.nova_client.servers.create_image(mox.IsA(str),
            mox.IsA(str), self.snapshot_meta).AndReturn(IMAGE_ID)
        self.nova_client.images.get(IMAGE_ID).AndReturn(
            MockImageStatus('ACTIVE'))
        self.nova_client.servers.get(mox.IsA(str)).AndReturn(
            MockServer())
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING',
                               timeout=mox.IsA(datetime.datetime),
                               error_message=None)
        self.worker.update_job(fakes.JOB_ID, 'DONE', timeout=None,
                               error_message=None)
        self.mox.ReplayAll()

        processor = TestableSnapshotProcessor(self.nova_client)
        processor.init_processor(self.worker)

        processor.process_job(fakes.JOB['job'])

        self.mox.VerifyAll()

    def test_process_job_should_succeed_after_multiple_tries(self):
        timeutils.set_time_override()
        self.nova_client.servers.create_image(mox.IsA(str),
            mox.IsA(str), self.snapshot_meta).AndReturn(IMAGE_ID)
        self.nova_client.images.get(IMAGE_ID).AndReturn(
            MockImageStatus('QUEUED'))
        self.nova_client.images.get(IMAGE_ID).AndReturn(
            MockImageStatus('SAVING'))
        self.nova_client.images.get(IMAGE_ID).AndReturn(
            MockImageStatus('SAVING'))
        self.nova_client.images.get(IMAGE_ID).AndReturn(
            MockImageStatus('ACTIVE'))
        self.nova_client.servers.get(mox.IsA(str)).AndReturn(
            MockServer())
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING',
                               timeout=mox.IsA(datetime.datetime),
                               error_message=None)
        self.worker.update_job(fakes.JOB_ID, 'DONE', timeout=None,
                               error_message=None)
        self.mox.ReplayAll()

        processor = TestableSnapshotProcessor(self.nova_client)
        processor.init_processor(self.worker)

        processor.process_job(fakes.JOB['job'])

        self.mox.VerifyAll()

    def test_process_job_should_update_status_only(self):
        base_time = timeutils.utcnow()
        time_seq = [
            base_time,
            base_time,
            base_time + datetime.timedelta(seconds=305),
            base_time + datetime.timedelta(seconds=605),
            base_time + datetime.timedelta(seconds=905),
            ]
        timeutils.set_time_override_seq(time_seq)

        job = copy.deepcopy(fakes.JOB['job'])
        job['timeout'] = base_time + datetime.timedelta(minutes=60)

        self.nova_client.servers.create_image(mox.IsA(str),
            mox.IsA(str), self.snapshot_meta).AndReturn(IMAGE_ID)
        self.nova_client.images.get(IMAGE_ID).AndReturn(
            MockImageStatus('QUEUED'))
        self.nova_client.images.get(IMAGE_ID).AndReturn(
            MockImageStatus('SAVING'))
        self.nova_client.images.get(IMAGE_ID).AndReturn(
            MockImageStatus('SAVING'))
        self.nova_client.images.get(IMAGE_ID).AndReturn(
            MockImageStatus('ACTIVE'))
        self.nova_client.servers.get(mox.IsA(str)).AndReturn(
            MockServer())
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING',
                               timeout=mox.IsA(datetime.datetime),
                               error_message=None)
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING', timeout=None,
                               error_message=None)
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING', timeout=None,
                               error_message=None)
        self.worker.update_job(fakes.JOB_ID, 'DONE', timeout=None,
                               error_message=None)
        self.mox.ReplayAll()

        processor = TestableSnapshotProcessor(self.nova_client)
        processor.init_processor(self.worker)

        processor.process_job(job)

        self.mox.VerifyAll()

    def test_process_job_should_update_status_and_timestamp(self):
        base_time = timeutils.utcnow()
        time_seq = [
            base_time,
            base_time,
            base_time + datetime.timedelta(seconds=305),
            base_time + datetime.timedelta(minutes=60, seconds=5),
            base_time + datetime.timedelta(minutes=60, seconds=305),
            ]
        timeutils.set_time_override_seq(time_seq)

        job = copy.deepcopy(fakes.JOB['job'])
        job['timeout'] = base_time + datetime.timedelta(minutes=60)

        self.nova_client.servers.create_image(mox.IsA(str),
            mox.IsA(str), self.snapshot_meta).AndReturn(IMAGE_ID)
        self.nova_client.images.get(IMAGE_ID).AndReturn(
            MockImageStatus('QUEUED'))
        self.nova_client.images.get(IMAGE_ID).AndReturn(
            MockImageStatus('SAVING'))
        self.nova_client.images.get(IMAGE_ID).AndReturn(
            MockImageStatus('SAVING'))
        self.nova_client.images.get(IMAGE_ID).AndReturn(
            MockImageStatus('ACTIVE'))
        self.nova_client.servers.get(mox.IsA(str)).AndReturn(
            MockServer())
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING',
                               timeout=mox.IsA(datetime.datetime),
                               error_message=None)
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING',
                               timeout=mox.IsA(datetime.datetime),
                               error_message=None)
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING', timeout=None,
                               error_message=None)
        self.worker.update_job(fakes.JOB_ID, 'DONE', timeout=None,
                               error_message=None)
        self.mox.ReplayAll()

        processor = TestableSnapshotProcessor(self.nova_client)
        processor.init_processor(self.worker)

        processor.process_job(job)

        self.mox.VerifyAll()

    def test_process_job_should_update_status_timestamp_no_retries(self):
        base_time = timeutils.utcnow()
        time_seq = [
            base_time,
            base_time,
            base_time + datetime.timedelta(minutes=5, seconds=5),
            base_time + datetime.timedelta(minutes=60, seconds=5),
            base_time + datetime.timedelta(minutes=120, seconds=5),
            base_time + datetime.timedelta(minutes=180, seconds=5),
            base_time + datetime.timedelta(minutes=240, seconds=5),
            ]
        print "Time_seq: %s" % str(time_seq)
        timeutils.set_time_override_seq(time_seq)

        job = copy.deepcopy(fakes.JOB['job'])
        job['timeout'] = base_time + datetime.timedelta(minutes=60)

        self.nova_client.servers.create_image(mox.IsA(str),
            mox.IsA(str), self.snapshot_meta).AndReturn(IMAGE_ID)
        self.nova_client.images.get(IMAGE_ID).AndReturn(
            MockImageStatus('QUEUED'))
        self.nova_client.images.get(IMAGE_ID).MultipleTimes().AndReturn(
            MockImageStatus('SAVING'))

        self.worker.update_job(fakes.JOB_ID, 'PROCESSING',
                               timeout=mox.IsA(datetime.datetime),
                               error_message=None)
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING',
                               timeout=mox.IsA(datetime.datetime),
                               error_message=None)
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING',
                               timeout=mox.IsA(datetime.datetime),
                               error_message=None)
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING',
                               timeout=mox.IsA(datetime.datetime),
                               error_message=None)
        self.worker.update_job(fakes.JOB_ID, 'TIMED_OUT', timeout=None,
                               error_message=None)

        self.mox.ReplayAll()

        processor = TestableSnapshotProcessor(self.nova_client)
        processor.init_processor(self.worker)

        processor.process_job(job)

        self.mox.VerifyAll()

    def _do_test_process_job_should_update_image_error(self, error_status):
        base_time = timeutils.utcnow()
        time_seq = [
            base_time,
            base_time,
            base_time + datetime.timedelta(seconds=305),
            base_time + datetime.timedelta(seconds=605),
            base_time + datetime.timedelta(seconds=905),
            base_time + datetime.timedelta(seconds=1205),
            base_time + datetime.timedelta(seconds=1505),
            ]
        timeutils.set_time_override_seq(time_seq)

        job = copy.deepcopy(fakes.JOB['job'])
        job['timeout'] = base_time + datetime.timedelta(minutes=60)

        self.nova_client.servers.create_image(mox.IsA(str),
            mox.IsA(str), self.snapshot_meta).AndReturn(IMAGE_ID)
        self.nova_client.images.get(IMAGE_ID).AndReturn(
            MockImageStatus('QUEUED'))
        self.nova_client.images.get(IMAGE_ID).AndReturn(
            MockImageStatus('SAVING'))
        self.nova_client.images.get(IMAGE_ID).AndReturn(
            MockImageStatus('SAVING'))
        self.nova_client.images.get(IMAGE_ID).AndReturn(
            MockImageStatus('SAVING'))
        self.nova_client.images.get(IMAGE_ID).AndReturn(
            error_status)

        self.worker.update_job(fakes.JOB_ID, 'PROCESSING',
                               timeout=mox.IsA(datetime.datetime),
                               error_message=None)
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING', timeout=None,
                               error_message=None)
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING', timeout=None,
                               error_message=None)
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING', timeout=None,
                               error_message=None)
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING', timeout=None,
                               error_message=None)
        self.worker.update_job(fakes.JOB_ID, 'ERROR', timeout=None,
                               error_message=mox.IsA(str))

        self.mox.ReplayAll()

        processor = TestableSnapshotProcessor(self.nova_client)
        processor.init_processor(self.worker)

        processor.process_job(job)

        self.mox.VerifyAll()

    def test_process_job_should_update_image_error(self):
        status = MockImageStatus('ERROR')
        self._do_test_process_job_should_update_image_error(status)

    def test_process_job_should_update_image_killed(self):
        status = MockImageStatus('KILLED')
        self._do_test_process_job_should_update_image_error(status)

    def test_process_job_should_update_image_deleted(self):
        status = MockImageStatus('DELETED')
        self._do_test_process_job_should_update_image_error(status)

    def test_process_job_should_update_image_pending_delete(self):
        status = MockImageStatus('PENDING_DELETE')
        self._do_test_process_job_should_update_image_error(status)

    def test_process_job_should_update_image_none(self):
        self._do_test_process_job_should_update_image_error(None)

    def test_doesnt_delete_images_less_than_retention(self):
        timeutils.set_time_override()
        self.nova_client.servers.create_image(mox.IsA(str),
            mox.IsA(str), self.snapshot_meta).AndReturn(IMAGE_ID)
        self.nova_client.images.get(IMAGE_ID).AndReturn(
            MockImageStatus('ACTIVE'))
        mock_server = MockServer(retention=3)
        self.nova_client.servers.get(mox.IsA(str)).AndReturn(mock_server)
        image_list = self._create_images_list(mock_server.id, 3)
        self.nova_client.images.list(detailed=True).AndReturn(image_list)
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING',
                               timeout=mox.IsA(datetime.datetime),
                               error_message=None)
        self.worker.update_job(fakes.JOB_ID, 'DONE', timeout=None,
                               error_message=None)
        self.mox.ReplayAll()

        processor = TestableSnapshotProcessor(self.nova_client)
        processor.init_processor(self.worker)

        processor.process_job(fakes.JOB['job'])

        self.mox.VerifyAll()

    def test_deletes_images_more_than_retention(self):
        timeutils.set_time_override()
        instance_id = fakes.JOB['job']['metadata']['instance_id']
        self.nova_client.servers.create_image(mox.IsA(str),
            mox.IsA(str), self.snapshot_meta).AndReturn(IMAGE_ID)
        self.nova_client.images.get(IMAGE_ID).AndReturn(
            MockImageStatus('ACTIVE'))
        mock_server = MockServer(instance_id=instance_id, retention=3)
        self.nova_client.servers.get(mox.IsA(str)).AndReturn(mock_server)
        image_list = self._create_images_list(mock_server.id, 5)
        self.nova_client.images.list(detailed=True).AndReturn(image_list)
        # The image list happens to be in descending created order
        self.nova_client.images.delete(image_list[-2].id)
        self.nova_client.images.delete(image_list[-1].id)
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING',
                               timeout=mox.IsA(datetime.datetime),
                               error_message=None)
        self.worker.update_job(fakes.JOB_ID, 'DONE', timeout=None,
                               error_message=None)
        self.mox.ReplayAll()

        processor = TestableSnapshotProcessor(self.nova_client)
        processor.init_processor(self.worker)

        processor.process_job(fakes.JOB['job'])

        self.mox.VerifyAll()

    def test_doesnt_delete_images_from_another_instance(self):
        timeutils.set_time_override()
        instance_id = fakes.JOB['job']['metadata']['instance_id']
        self.nova_client.servers.create_image(mox.IsA(str),
            mox.IsA(str), self.snapshot_meta).AndReturn(IMAGE_ID)
        self.nova_client.images.get(IMAGE_ID).AndReturn(
            MockImageStatus('ACTIVE'))
        mock_server = MockServer(instance_id=instance_id, retention=3)
        self.nova_client.servers.get(mox.IsA(str)).AndReturn(mock_server)
        image_list = self._create_images_list(mock_server.id, 5)
        to_delete = image_list[3:]
        image_list.extend(self._create_images_list(
                uuidutils.generate_uuid(), 3))
        self.nova_client.images.list(detailed=True).AndReturn(image_list)
        # The image list happens to be in descending created order
        self.nova_client.images.delete(to_delete[0].id)
        self.nova_client.images.delete(to_delete[1].id)
        self.worker.update_job(fakes.JOB_ID, 'PROCESSING',
                               timeout=mox.IsA(datetime.datetime),
                               error_message=None)
        self.worker.update_job(fakes.JOB_ID, 'DONE', timeout=None,
                               error_message=None)
        self.mox.ReplayAll()

        processor = TestableSnapshotProcessor(self.nova_client)
        processor.init_processor(self.worker)

        processor.process_job(fakes.JOB['job'])

        self.mox.VerifyAll()


class MockNovaClient(object):
    def __init__(self):
        self.servers = None
        self.images = None


class MockImageStatus(object):
    def __init__(self, status):
        self.status = status


class MockImage(object):
    def __init__(self, image_id, created, instance_id):
        self.id = image_id
        self.created = created
        self.metadata = {
            'instance_uuid': instance_id,
            }


class MockServer(object):
    def __init__(self, instance_id=None, retention=0):
        self.id = instance_id or uuidutils.generate_uuid()
        self.metadata = {}
        if retention:
            self.metadata["org.openstack__1__retention"] = str(retention)


class TestableSnapshotProcessor(snapshot.SnapshotProcessor):
    def __init__(self, nova_client):
        super(TestableSnapshotProcessor, self).__init__()
        self.nova_client = nova_client

    def _get_nova_client(self):
        return self.nova_client

    def _get_utcnow(self):
        now = timeutils.utcnow()
        print "Returning NOW: %s" % str(now)
        return now
