"""Test course blocks data tasks."""

import json
import logging
import datetime
import httpretty
from ddt import ddt, data, unpack

from edx.analytics.tasks.util.edx_rest_api import (
    EdxRestApiTask, EdxRestApiTaskException,
)
from edx.analytics.tasks.tests import unittest


log = logging.getLogger(__name__)


@ddt
class EdxRestApiTaskTest(unittest.TestCase):
    """Tests the EdxRestApiTask output"""

    task_class = EdxRestApiTask
    api_url = 'http://localhost:8000/api/courses/v1/blocks/'
    auth_url = 'http://localhost:8000/oauth2/access_token/'
    api_resource = 'blocks'
    valid_api_auth_response = '{"access_token": "token", "expires_in": 100}'

    def setUp(self):
        super(EdxRestApiTaskTest, self).setUp()
        self.create_task()

    def create_task(self):
        """Create the task."""
        self.task = self.task_class(
            resource=self.api_resource,
            date=datetime.datetime.utcnow(),
        )

    def mock_api_call(self, method, url, status_code, body):
        """Register the given URL, and send data as a JSON string."""
        if isinstance(body, dict):
            body = json.dumps(body)

        httpretty.enable()
        httpretty.register_uri(
            method, url, body=body, status=status_code,
        )

    @data(
        (400, dict(error="invalid_client"), 'Invalid client_id or client_secret'),
        (404, '<html><h1>Page not found <span>(404)</span></h1></html>', 'Invalid auth_url'),
    )
    @unpack
    def test_invalid_auth(self, status_code, body, message):
        self.mock_api_call('POST', self.auth_url, status_code, body)
        with self.assertRaises(EdxRestApiTaskException) as context:
            print self.task.requires()
        self.assertIn(message, context.exception.message)
