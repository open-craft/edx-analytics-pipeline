"""Test course list tasks."""
import os
import shutil
import tempfile
from ddt import ddt, data, unpack

from edx.analytics.tasks.course_list import CourseListApiDataTask
from edx.analytics.tasks.tests.map_reduce_mixins import MapperTestMixin, ReducerTestMixin
from edx.analytics.tasks.tests import unittest


class CourseListTestMixin(object):
    """Common code between the the CourseList task tests"""

    task_class = CourseListApiDataTask

    def setUp(self):
        self.setup_dirs()
        super(CourseListTestMixin, self).setUp()

    def create_task(self, **kwargs):
        """Create the task"""
        self.task = self.task_class(
            output_root=self.output_dir,
            **kwargs
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
class CourseListApiDataTaskTest(CourseListTestMixin, unittest.TestCase):
    """Tests the CourseListApiDataTask basic functions. """

    @data(
        (None, dict(page_size=100)),
        (dict(abc='def', ghi='jkl'), dict(abc='def', ghi='jkl')),
        ('{"mno": "pqr", "stu": "vwx"}', dict(mno="pqr", stu="vwx")),
    )
    @unpack
    def test_requires(self, args, expected_args):
        # Ensure the arguments dict is parsed ok
        kwargs = {}
        if args is not None:
            kwargs['api_args'] = args
        self.create_task(**kwargs)
        self.assertEquals(self.task.api_args, expected_args)

        # .. and passed to the required task
        requirement = self.task.requires()
        self.assertEquals(self.task.api_args, requirement.arguments)


@ddt
class CourseListApiDataMapperTaskTest(CourseListTestMixin, MapperTestMixin, unittest.TestCase):
    """Tests the CourseListApiDataTask mapper output"""

    @data(
        '',
        None,
        'abc',
        '{"abc": "def"}',
        '{"results": []}',
        '{"results": [{}]}',
        '{"results": [{"abc": "def"}]}',
    )
    def test_no_map_output(self, input_data):
        self.assert_no_map_output_for(input_data)

    @data(
        ('{"results": [{"id": "def"}]}', 'def', dict(course_id='def')),  # dogwood API format
        ('{"results": [{"course_id": "abc"}]}', 'abc', dict(course_id='abc')),  # eucalyptus API format
    )
    @unpack
    def test_map_output(self, input_data, reduce_key, expected_output):
        self.assert_single_map_output_load_jsons(
            input_data,
            reduce_key,
            expected_output,
        )


@ddt
class CourseListApiDataReducerTaskTest(CourseListTestMixin, ReducerTestMixin, unittest.TestCase):
    """Tests the CourseListApiDataTask reducer output"""

    @data(
        (
            (), (),
        ),
        (
            (dict(
                course_id='abc',
                name='name',
                org='org',
                number='number',
                blocks_url='http://blocks.url',
                short_description='hi',
                enrollment_start='2016-08-24 01:01:12.0',
                enrollment_end='2017-08-24 01:01:12.0',
                start='2016-09-24 01:01:12.0',
                end='2017-09-24 01:01:12.0',
                start_display='soon',
                start_type='string',
                effort='lots',
                pacing='instructor',
            ), dict(
                course_id='def',
                name='name2',
                org='org2',
                number='number2',
                blocks_url='http://blocks.url2',
                short_description='hi2',
                enrollment_start='2016-08-24 02:02:22.0',
                enrollment_end='2017-08-24 02:02:22.0',
                start='2016-09-24 02:02:22.0',
                end='2017-09-24 02:02:22.0',
                start_display='2016-08-24',
                start_type='timestamp',
                effort='minimal',
                pacing='self',
            ),),
            (('abc', 'name', 'org', 'number', 'http://blocks.url', 'hi', '2016-08-24 01:01:12.000000',
              '2017-08-24 01:01:12.000000', '2016-09-24 01:01:12.000000', '2017-09-24 01:01:12.000000',
              'soon', 'string', 'lots', 'instructor'),
             ('def', 'name2', 'org2', 'number2', 'http://blocks.url2', 'hi2', '2016-08-24 02:02:22.000000',
              '2017-08-24 02:02:22.000000', '2016-09-24 02:02:22.000000', '2017-09-24 02:02:22.000000',
              '2016-08-24', 'timestamp', 'minimal', 'self')),
        )
    )
    @unpack
    def test_reducer_output(self, input_data, expected_tuples):
        self._check_output_complete_tuple(
            input_data,
            expected_tuples,
        )
