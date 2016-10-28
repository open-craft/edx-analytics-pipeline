"""Test course blocks tasks."""

import os
import json
import shutil
import tempfile
import logging
from urllib import urlencode
import luigi
from requests.exceptions import HTTPError
import httpretty
from ddt import ddt, data, unpack

from edx.analytics.tasks.course_blocks import (
    CourseBlocksApiDataTask, CourseBlocksPartitionTask, PullCourseBlocksApiData,
)
from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.map_reduce_mixins import MapperTestMixin, ReducerTestMixin
from edx.analytics.tasks.tests.fixtures.helpers import load_fixture


log = logging.getLogger(__name__)


class CourseBlocksTestMixin(object):
    """Common code between the the CourseBlocksApiDataTask reducer and mapper tests"""

    task_class = CourseBlocksApiDataTask
    course_id = 'course-v1:edX+DemoX+Demo_Course'

    def setUp(self):
        self.setup_dirs()
        super(CourseBlocksTestMixin, self).setUp()
        self.reduce_key = self.course_id

    def create_task(self):
        """Create the task"""
        self.task = self.task_class(
            course_ids=(self.course_id,),
            output_root=self.output_dir,
        )

    def setup_dirs(self):
        """Create temp input and output dirs."""
        self.temp_rootdir = tempfile.mkdtemp()
        self.output_dir = os.path.join(self.temp_rootdir, "output")
        os.mkdir(self.output_dir)
        self.addCleanup(self.cleanup, self.temp_rootdir)

    def cleanup(self, dirname):
        """Remove the temp directory only if it exists."""
        if os.path.exists(dirname):
            shutil.rmtree(dirname)


@ddt
class CourseBlocksApiDataTaskTest(CourseBlocksTestMixin, unittest.TestCase):
    """Tests the CourseBlocksApiDataTask basic functions. """

    def test_complete(self):
        self.task = self.task_class(
            output_root=self.output_dir,
            course_ids=None,
        )
        self.assertEquals(self.task.course_ids, ())
        self.assertFalse(self.task.complete())

        # Create the output_root/_SUCCESS file
        with open(os.path.join(self.output_dir, '_SUCCESS'), 'w') as success:
            success.write('')
        self.assertTrue(self.task.output().exists())
        self.assertTrue(self.task.complete())

    @data(
        ((), None),                          # empty
        (('abc',), ('abc',)),                # tuple
        (('abc', 'def'), ['abc', 'def']),    # array
        (('def', 'ghi'), '["def", "ghi"]'),  # JSON string
    )
    @unpack
    def test_requires(self, course_ids, course_ids_parameter):
        self.task = self.task_class(
            output_root=self.output_dir,
            course_ids=course_ids_parameter,
        )
        # Ensure the required task was created with the course_ids list
        required = self.task.requires()
        self.assertEqual(required.course_ids, course_ids)


@ddt
class CourseBlocksApiDataMapperTaskTest(CourseBlocksTestMixin, MapperTestMixin, unittest.TestCase):
    """Tests the CourseBlocksApiDataTask mapper output"""

    @data(
        '{"abc": "def"}',
        '{"blocks": {}}',
        '{"blocks": {}, "root": ""}',
        '{"blocks": {}, "root": "abc"}',
        '{"course_id": 1, "blocks": {}, "root": "abc"}',
    )
    def test_no_map_output(self, input_data):
        self.assert_no_map_output_for(input_data)

    @data(
        ('{"course_id": "course-v1:edX+DemoX+Demo_Course", "blocks": {"abc":{}}, "root": "abc"}',
         {"course_id": "course-v1:edX+DemoX+Demo_Course", "blocks": {"abc": {}}, "root": "abc"}),
    )
    @unpack
    def test_map_output(self, input_data, expected_output):
        self.assert_single_map_output_load_jsons(
            input_data,
            self.reduce_key,
            expected_output,
        )


@ddt
class CourseBlocksApiDataReducerTaskTest(CourseBlocksTestMixin, ReducerTestMixin, unittest.TestCase):
    """Tests the CourseBlocksApiDataTask reducer output"""

    # single, root-only block
    single_block_input_data = {
        "root": "abc",
        "blocks": {
            "abc": {
                'id': 'abc',
                'display_name': 'ABC',
                'type': 'course',
            },
        },
    }

    # multiple blocks, including an orphan, and one with multiple parents.
    multiple_block_input_data = {
        "root": "abc",
        "blocks": {
            "abc": {
                'id': 'abc',
                'display_name': 'ABC',
                'type': 'block',
                'children': ['def', 'stu'],
            },
            "def": {
                'id': 'def',
                'display_name': 'DEF',
                'type': 'block',
                'children': ['jkl', 'mno']
            },
            "ghi": {
                'id': 'ghi',
                'display_name': 'GHI',
                'type': 'block',
            },
            "jkl": {
                'id': 'jkl',
                'display_name': 'JKL',
                'type': 'block',
                'children': ['vwx'],
            },
            "mno": {
                'id': 'mno',
                'display_name': 'MNO',
                'type': 'block',
                'children': ['pqr']
            },
            "pqr": {
                'id': 'pqr',
                'display_name': 'PQR',
                'type': 'block',
                'children': ['jkl']
            },
            "stu": {
                'id': 'stu',
                'display_name': 'STU',
                'type': 'block',
            },
            "vwx": {
                'id': 'vwx',
                'display_name': 'VWX',
                'type': 'block',
            },
        },
    }

    # data tuple fields are given in this order:
    # (block_id,block_type,display_name,is_root,is_orphan,is_dag,parent_block_id,course_path,sort_idx)
    @data(
        ((('abc', 'course', 'ABC', '1', '0', '0', '\\N', '', '0'),), False),
        ((('abc', 'course', 'ABC', '1', '0', '0', '\\N', '', '0'),), True),
        ((('abc', 'block', 'ABC', '1', '0', '0', '\\N', '', '0'),
          ('def', 'block', 'DEF', '0', '0', '0', 'abc', 'ABC', '1'),
          ('stu', 'block', 'STU', '0', '0', '0', 'abc', 'ABC', '2'),
          ('jkl', 'block', 'JKL', '0', '0', '1', 'def', 'ABC / DEF', '3'),
          ('mno', 'block', 'MNO', '0', '0', '0', 'def', 'ABC / DEF', '4'),
          ('vwx', 'block', 'VWX', '0', '0', '0', 'jkl', 'ABC / DEF / JKL', '5'),
          ('pqr', 'block', 'PQR', '0', '0', '0', 'mno', 'ABC / DEF / MNO', '6'),
          ('ghi', 'block', 'GHI', '0', '1', '0', '\\N', '(Deleted block :)', '8')), False),
        ((('ghi', 'block', 'GHI', '0', '1', '0', '\\N', '(Deleted block :)', '-1'),
          ('abc', 'block', 'ABC', '1', '0', '0', '\\N', '', '0'),
          ('def', 'block', 'DEF', '0', '0', '0', 'abc', 'ABC', '1'),
          ('stu', 'block', 'STU', '0', '0', '0', 'abc', 'ABC', '2'),
          ('jkl', 'block', 'JKL', '0', '0', '1', 'def', 'ABC / DEF', '3'),
          ('mno', 'block', 'MNO', '0', '0', '0', 'def', 'ABC / DEF', '4'),
          ('vwx', 'block', 'VWX', '0', '0', '0', 'jkl', 'ABC / DEF / JKL', '5'),
          ('pqr', 'block', 'PQR', '0', '0', '0', 'mno', 'ABC / DEF / MNO', '6')), True),
    )
    @unpack
    def test_map_output(self, expected_tuples, sort_orphan_blocks_up):
        # Use single or multiple block input data
        input_data = self.single_block_input_data if len(expected_tuples) == 1 else self.multiple_block_input_data

        # Inject our course_id into the input_data, and expected_values tuples
        expected_tuples = tuple((values[0],) + (self.course_id,) + values[1:] for values in expected_tuples)

        self.task.sort_orphan_blocks_up = sort_orphan_blocks_up
        self._check_output_complete_tuple(
            (input_data,),
            expected_tuples,
        )

    def test_edx_demo_blocks(self):
        # Use a "real" input example, taken from the edX Demo course
        input_data = [json.loads(load_fixture('demo_course_blocks.json'))]
        expected_tuples = eval(load_fixture('demo_course_blocks_reduced.tuples'))  # pylint: disable=eval-used
        self._check_output_complete_tuple(
            input_data,
            expected_tuples,
        )


class CourseBlocksInputTask(luigi.Task):
    """Use for the CourseBlocksTask.input_task parameter."""
    output_root = luigi.Parameter()


@ddt
class CourseBlocksPartitionTaskTest(CourseBlocksTestMixin, unittest.TestCase):
    """Tests the CourseBlocksPartitionTask completion status."""
    task_class = CourseBlocksPartitionTask

    def setUp(self):
        super(CourseBlocksPartitionTaskTest, self).setUp()
        self.setup_dirs()
        self.create_task()

    def create_task(self, **kwargs):
        """Create the task"""
        self.task = self.task_class(
            warehouse_path=self.output_dir,
            input_root=self.input_file,
            **kwargs
        )

    def setup_dirs(self):
        """Create temp input and output dirs."""
        self.temp_rootdir = tempfile.mkdtemp()
        self.output_dir = os.path.join(self.temp_rootdir, "output")
        self.input_file = os.path.join(self.temp_rootdir, "input.txt")
        self.addCleanup(self.cleanup, self.temp_rootdir)

    def cleanup(self, dirname):
        """Remove the temp directory only if it exists."""
        if os.path.exists(dirname):
            shutil.rmtree(dirname)

    def create_input_file(self, course_ids, course_id_index):
        """Create a tab-separated file containing the given course_ids at the correct course_id_index location."""
        with open(self.input_file, 'w') as output:
            for course_id in course_ids:
                line = ['{}'.format(x) for x in range(course_id_index + 1)]
                line[course_id_index] = course_id
                output.write('{}\r\n'.format('\t'.join(line)))

    @data(
        (['abc', 'def', 'ghi'], ('abc', 'def', 'ghi'), 0),
        (['klm', 'nop', 'nop'], ('klm', 'nop'), 3),  # removes duplicates
        (['qrs'], ('qrs',), 8),
    )
    @unpack
    def test_requires_with_input_root(self, course_ids, expected_course_ids, course_id_index):

        # Test requires() with shifting course_id_index
        self.task.course_id_index = course_id_index

        # Initially, the CourseBlocksPartitionTask requires 1 task, the hive table task
        requirements = tuple(self.task.requires())
        self.assertEqual(len(requirements), 1)

        # We can activate the data_task requirement by creating the input_root file
        self.create_input_file(course_ids, course_id_index)

        # Now, the CourseBlocksPartitionTask's requirements are complete
        requirements = tuple(self.task.requires())
        self.assertEqual(len(requirements), 2)

        # And the course_ids have been passed along to the data task
        self.assertEqual(requirements[0].course_ids, expected_course_ids)

    @data(
        (['abc', 'def', 'ghi'], ('abc', 'def', 'ghi')),
        (('klm', 'nop'), ('klm', 'nop')),
        ('["qrs"]', ('qrs',)),
    )
    @unpack
    def test_requires_with_course_ids(self, course_ids, expected_course_ids):
        # Test requires() with a course_ids list
        self.create_task(course_ids=course_ids)

        # The CourseBlocksPartitionTask's requirements are complete
        requirements = tuple(self.task.requires())
        self.assertEqual(len(requirements), 2)

        # And the course_ids have been passed along to the data task
        self.assertEqual(requirements[0].course_ids, expected_course_ids)


@ddt
class PullCourseBlocksApiDataTest(unittest.TestCase):
    """Tests the PullCourseBlocksApiData task."""

    task_class = PullCourseBlocksApiData
    auth_url = 'http://localhost:8000/oauth2/access_token/'
    api_url = 'http://localhost:8000/api/courses/v1/blocks/'
    course_id = 'course-v1:edX+DemoX+Demo_Course'

    def setUp(self):
        super(PullCourseBlocksApiDataTest, self).setUp()
        self.setup_dirs()
        self.create_task()
        httpretty.reset()

    def create_task(self, **kwargs):
        """Create the task."""
        args = dict(
            api_root_url=self.api_url,
            warehouse_path=self.cache_dir,
            course_ids=(self.course_id,),
        )
        args.update(**kwargs)
        self.task = self.task_class(**args)
        return self.task

    def setup_dirs(self):
        """Create temp cache dir."""
        self.temp_rootdir = tempfile.mkdtemp()
        self.cache_dir = os.path.join(self.temp_rootdir, "cache")
        os.mkdir(self.cache_dir)
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
        (404, None),
        (403, HTTPError),
        (500, HTTPError),
    )
    @unpack
    def test_errors(self, status_code, expected_exception):
        course_ids = ('abc', 'def')
        self.create_task(course_ids=course_ids)
        self.mock_api_call('POST', self.auth_url, body=dict(access_token='token', expires_in=2000))
        params = dict(depth="all", requested_fields="children", all_blocks="true")

        # Mock a 200 API call
        params['course_id'] = course_ids[0]
        self.mock_api_call('GET', '{}?{}'.format(self.api_url, urlencode(params)),
                           body="{}",
                           status_code=200,
                           match_querystring=True,
                           content_type='application/json')

        # Mock the error API call
        params['course_id'] = course_ids[1]
        self.mock_api_call('GET', '{}?{}'.format(self.api_url, urlencode(params)),
                           body="{}",
                           status_code=status_code,
                           match_querystring=True,
                           content_type='application/json')

        if expected_exception:
            with self.assertRaises(expected_exception):
                self.task.run()
                self.assertFalse(self.task.complete())
        else:
            self.task.run()
            self.assertTrue(self.task.complete())
            with self.task.output().open() as json_input:
                lines = json_input.readlines()
                self.assertEquals(len(lines), 1)

    def test_cache(self):
        # The cache is clear, and the task is not complete
        self.assertFalse(self.task.complete())

        # Mock the API call
        body = dict(blocks={'abc': {}}, root='abc')
        for mock_api in (True, False):

            # First, we mock the API calls, to populate the cache
            if mock_api:
                params = {'course_id': self.course_id, 'all_blocks': 'true', 'depth': 'all',
                          'requested_fields': 'children'}
                self.mock_api_call('POST', self.auth_url, body=dict(access_token='token', expires_in=2000))

                # API results are not paginated
                self.mock_api_call('GET', '{}?{}'.format(self.api_url, urlencode(params)),
                                   body=body,
                                   content_type='application/json')

                self.task.run()
                self.assertTrue(self.task.complete())

            # Next, we clear the API mocks, and create a new task, and read from the cache
            else:
                httpretty.reset()

                # Create a new task with the same arguments - is already complete
                old_task = self.task
                new_task = self.create_task()
                self.assertTrue(new_task.complete())

        # Ensure the data returned by the first tasks matches the expected data
        with old_task.output().open() as json_input:
            lines = json_input.readlines()
            self.assertEquals(len(lines), 1)

            # Records are annotated with Course ID
            body['course_id'] = self.course_id
            self.assertEquals(json.loads(lines[0]), body)

        # Ensure the data returned by the two tasks is the same
        with new_task.output().open() as json_input:
            cache_lines = json_input.readlines()
            self.assertEquals(lines, cache_lines)
