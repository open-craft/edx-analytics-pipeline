"""Test course blocks data tasks."""

import os
import json
import shutil
import tempfile
import datetime
import logging
import httpretty
from ddt import ddt, data, unpack
from mock import patch

from edx.analytics.tasks.util.edx_rest_api import (
    EdxRestApiTask, EdxRestApiTaskException,
)
from edx.analytics.tasks.tests import unittest


log = logging.getLogger(__name__)


@ddt
class EdxRestApiTaskTest(unittest.TestCase):
    """Tests the EdxRestApiTask output"""

    task_class = EdxRestApiTask
    base_url = 'http://localhost:8000'
    api_url = 'http://localhost:8000/api/courses/v1/blocks/'
    auth_url = 'http://localhost:8000/oauth2/access_token/'
    api_resource = 'blocks'
    date = datetime.datetime.utcnow()
    valid_api_auth_response = '{"access_token": "token", "expires_in": 100}'

    def setUp(self):
        super(EdxRestApiTaskTest, self).setUp()
        self.setup_dirs()
        self.create_task()
        httpretty.reset()

    def create_task(self, **kwargs):
        """Create the task."""
        self.task = self.task_class(
            date=self.date,
            base_url=self.base_url,
            resource=self.api_resource,
            cache_root=self.cache_dir,
            **kwargs
        )
        return self.task

    def setup_dirs(self):
        """Create temp input and cache dirs."""
        self.temp_rootdir = tempfile.mkdtemp()
        self.cache_dir = os.path.join(self.temp_rootdir, "cache")
        os.mkdir(self.cache_dir)
        self.input_dir = os.path.join(self.temp_rootdir, "input")
        os.mkdir(self.input_dir)
        self.addCleanup(self.cleanup, self.temp_rootdir)

    def cleanup(self, dirname):
        """Remove the temp directory only if it exists."""
        if os.path.exists(dirname):
            shutil.rmtree(dirname)

    def mock_api_call(self, method, url, status_code=200, body='', **kwargs):
        """Register the given URL, and send data as a JSON string."""
        if isinstance(body, dict):
            body = json.dumps(body)

        log.debug('register_uri(%s, %s, %s, %s, %s)', method, url, body, status_code, kwargs)
        httpretty.enable()
        httpretty.register_uri(
            method, url, body=body, status=status_code, **kwargs
        )

    @data(
        (None, {}),
        ('{"page": 1, "page_size": 5}', dict(page=1, page_size=5)),
        (u'{"page": 2, "page_size": 20}', dict(page=2, page_size=20)),
        ({"page": 3, "page_size": 25}, dict(page=3, page_size=25)),
    )
    @unpack
    def test_arguments(self, arguments, expected_arguments):
        if arguments is not None:
            self.create_task(arguments=arguments)
        self.assertEquals(self.task.arguments, expected_arguments)

    @data(
        (dict(client_id='client_id', client_secret='client_secret'),
         'edx_rest_api_client.client.EdxRestApiClient.get_oauth_access_token'),
        (dict(client_id='client_id', client_secret='client_secret',
              oauth_username='username', oauth_password='password'),
         'edx.analytics.tasks.util.edx_rest_api.EdxRestApiTask.get_oauth_access_token'),
    )
    @unpack
    def test_invalid_auth(self, auth_args, mock_method):
        for (status_code, body, message) in (
                (400, dict(error="invalid_client"), 'Invalid client_id or client_secret'),
                (404, '<html><h1>Page not found <span>(404)</span></h1></html>', 'Invalid auth_url'),
        ):
            self.create_task(**auth_args)
            self.mock_api_call('POST', self.auth_url, status_code, body)

            # Ensure the correct access_token method is called
            with patch(mock_method, return_value=('client', 2)) as patched_method:
                self.task.get_client()
                patched_method.assert_called_once_with(url=self.auth_url, **auth_args)

            # Ensure that the real access_token method throws the correct exception
            with self.assertRaises(EdxRestApiTaskException) as context:
                self.task.get_client()
            self.assertIn(message, context.exception.message)

    @data(
        (dict(client_id='client_id', client_secret='client_secret'),
         'edx_rest_api_client.client.EdxRestApiClient',
         'get_oauth_access_token'),
        (dict(client_id='client_id', client_secret='client_secret',
              oauth_username='username', oauth_password='password'),
         'edx.analytics.tasks.util.edx_rest_api.EdxRestApiTask',
         'get_oauth_access_token'),
    )
    @unpack
    def test_access_token(self, auth_args, mock_method_class, mock_method):
        self.mock_api_call('POST', self.auth_url, body=dict(access_token='token', expires_in=200))
        self.create_task(**auth_args)
        auth_args['url'] = self.auth_url
        with patch(mock_method_class + '.' + mock_method, return_value=('client', 2)) as patched_method:
            self.task.get_client()
            patched_method.assert_called_once_with(**auth_args)

        # Ensure that the real access_token method returns the expected response
        expected_expires_at = datetime.datetime.utcnow() + datetime.timedelta(seconds=200)
        (client, expires_at) = self.task.get_client()
        self.assertIsNotNone(client)
        self.assertEquals(int(expires_at.strftime('%s')), int(expected_expires_at.strftime('%s')))

    @data(
        (400, dict(error="unauthorized")),
        (404, '<html><h1>Page not found <span>(404)</span></h1></html>'),
    )
    @unpack
    def test_raise_exceptions(self, status, body):

        # Return error page
        self.mock_api_call('POST', self.auth_url, body=dict(access_token='token', expires_in=2000))
        self.mock_api_call('GET', self.api_url, body=body, status_code=status, content_type='application/json')

        # Expected exception message
        exception = 'Error fetching API resource {}: Client Error {}: {}'.format(dict(page=1), status, self.api_url)

        # If raising exceptions, we can catch it
        self.create_task(raise_exceptions=True)
        with self.assertRaises(EdxRestApiTaskException) as context:
            self.task.requires()
        self.assertEquals(exception, context.exception.message)

        # If not raising exceptions, they should be stored in the cached data
        self.create_task(raise_exceptions=False)
        requirements = self.task.requires()
        self.assertEquals(len(requirements), 1)
        with requirements[0].output().open() as json_input:
            lines = json_input.readlines()
            self.assertEquals(len(lines), 1)
            self.assertEquals(json.loads(lines[0].strip()), dict(exception=exception))

    def test_cache_and_pagination(self):
        # The cache is clear, the manifest does not exist, and the task is not complete
        self.assertFalse(self.task.manifest_target.exists())
        self.assertFalse(self.task.complete())

        # Mock the API call with paginated and non-paginated URLs
        body_with_next = dict(blocks=[], pagination=dict(next=True))
        body = dict(blocks=[])
        pages = (1, 2, 3)
        for mock_api in (True, False):

            # First, we mock the API calls, to populate the cache
            if mock_api:
                self.mock_api_call('POST', self.auth_url, body=dict(access_token='token', expires_in=2000))

                # Final page is not paginated
                self.mock_api_call('GET', self.api_url, body=body, content_type='application/json')

                # But the first pages are
                for page in pages:
                    self.mock_api_call('GET', '{}?page={}'.format(self.api_url, page), body=body_with_next,
                                       match_querystring=True, content_type='application/json')

            # Next, we clear the API mocks, and create a new task, and read from the cache
            else:
                httpretty.reset()

                # Create a new task with the same arguments
                old_task = self.task
                new_task = self.create_task()

                # Even though the task instances are different, the manifest files will be the same,
                # because the (significant) parameters match.
                self.assertNotEquals(old_task, new_task)
                self.assertEquals(old_task.manifest, new_task.manifest)

            # Ensure the requirements reflect the number of pages with pagination
            requirements = self.task.requires()
            self.assertEquals(len(requirements), len(pages) + 1)

            for idx, required in enumerate(requirements):
                with required.output().open() as json_input:
                    lines = json_input.readlines()
                    self.assertEquals(len(lines), 1)
                    if idx + 1 in pages:
                        self.assertEquals(json.loads(lines[0].strip()), body_with_next)
                    else:
                        self.assertEquals(json.loads(lines[0].strip()), body)

            # Fetching the requirements creates the manifest file, and completes the task
            self.assertTrue(self.task.manifest_target.exists())
            self.assertTrue(self.task.complete())

    def test_extend_response(self):
        self.create_task(extend_response=dict(hi='there'))

        self.mock_api_call('POST', self.auth_url, body=dict(access_token='token', expires_in=200))
        self.mock_api_call('GET', self.api_url, body=dict(blocks=[]), content_type='application/json')
        requirements = self.task.requires()
        self.assertEquals(len(requirements), 1)

        # Ensure that the extend_response dict ends up in the cached data
        with requirements[0].output().open() as json_input:
            lines = json_input.readlines()
            self.assertEquals(len(lines), 1)
            self.assertEquals(json.loads(lines[0].strip()), dict(blocks=[], hi='there'))
