"""Test course blocks data tasks."""

import json
import logging
import datetime
import luigi
from ddt import ddt, data, unpack

from edx.analytics.tasks.course_blocks import (
    CourseBlocksApiDataTask, CourseIdTimestampPartitionMixin,
)
from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.map_reduce_mixins import MapperTestMixin, ReducerTestMixin
from edx.analytics.tasks.tests.fixtures.helpers import load_fixture


log = logging.getLogger(__name__)


class CourseBlocksApiDataTestMixin(object):
    """Common code between the the CourseBlocksApiDataTask reducer and mapper tests"""

    task_class = CourseBlocksApiDataTask
    reduce_key = course_id = 'course-v1:edX+DemoX+Demo_Course'
    output_root = '/tmp/course_blocks'

    def create_task(self):
        """Create the task"""
        self.task = self.task_class(
            course_id=self.course_id,
            output_root=self.output_root,
        )


@ddt
class CourseBlocksApiDataMapperTaskTest(CourseBlocksApiDataTestMixin, MapperTestMixin, unittest.TestCase):
    """Tests the CourseBlocksApiDataTask mapper output"""

    @data(
        '',
        None,
        'abc',
        '{"abc": "def"}',
        '{"blocks": {}}',
        '{"blocks": {}, "root": ""}',
        '{"blocks": {}, "root": "abc"}',
    )
    def test_no_map_output(self, input_data):
        self.assert_no_map_output_for(input_data)

    @data(
        ('{"blocks": {"abc":{}}, "root": "abc"}',
         {"blocks": {"abc": {}}, "root": "abc"}),
    )
    @unpack
    def test_map_output(self, input_data, expected_output):
        self.assert_single_map_output_load_jsons(
            input_data,
            self.reduce_key,
            expected_output,
        )


@ddt
class CourseBlocksApiDataReducerTaskTest(CourseBlocksApiDataTestMixin, ReducerTestMixin, unittest.TestCase):
    """Tests the CourseBlocksApiDataTask reducer output"""

    # data tuple fields are given in this order:
    # (block_id, block_type, display_name, is_root, has_multiple_parents, parent_block_id, course_path, sort_idx)
    @data(
        (
            [{
                "root": "abc",
                "blocks": {
                    "abc": {
                        'id': 'abc',
                        'display_name': 'ABC',
                        'type': 'course',
                    },
                },
            }],
            (('abc', 'course', 'ABC', 'True', 'False', '\\N', '', '0'),)
        ),
        (
            [{
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
                },
            }],
            (('abc', 'block', 'ABC', 'True', 'False', '\\N', '', '0'),
             ('def', 'block', 'DEF', 'False', 'False', 'abc', 'ABC', '1'),
             ('stu', 'block', 'STU', 'False', 'False', 'abc', 'ABC', '2'),
             ('mno', 'block', 'MNO', 'False', 'False', 'def', 'ABC / DEF', '4'),
             ('pqr', 'block', 'PQR', 'False', 'False', 'mno', 'ABC / DEF / MNO', '5'),
             ('jkl', 'block', 'JKL', 'False', 'True', '\\N', '(Multiple locations :)', '7'),
             ('ghi', 'block', 'GHI', 'False', 'False', '\\N', '(Deleted block :)', '7'),)
        ),
        # A "real" example, taken from the edX Demo course
        ([json.loads(load_fixture('demo_course_blocks.json'))],
         eval(load_fixture('demo_course_blocks_reduced.tuples'))),  # pylint: disable=eval-used
    )
    @unpack
    def test_map_output(self, input_data, expected_values):
        # Inject our course_id into the given expected_values tuples
        expected_tuples = tuple((values[0],) + (self.course_id,) + values[1:] for values in expected_values)
        self._check_output_complete_tuple(
            input_data,
            expected_tuples,
        )


class TestCourseIdTimestampPartitionTask(CourseIdTimestampPartitionMixin, luigi.Task):
    """Task class used to tests the CourseIdTimestampPartitionMixin."""
    pass


class CourseIdTimestampPartitionMixinTest(unittest.TestCase):
    """Tests the CourseIdTimestampPartitionMixin's formatted partition value."""

    course_id = 'course-v1:edX+DemoX+Demo_Course'
    filename_safe_course_id = 'edX_DemoX_Demo_Course'
    timestamp = datetime.datetime.utcnow()
    partition_format = '{course_id}_%Y%m%dT%H%M%S.%f'

    def test_partition_value(self):
        task = TestCourseIdTimestampPartitionTask(
            course_id=self.course_id,
            date=self.timestamp,
            partition_format=self.partition_format,
        )

        # Ensure that datetimes are not filtered down to dates alone, when determining partition value.
        datetime_format = self.timestamp.strftime(self.partition_format)
        expected_partition_value = datetime_format.format(course_id=self.filename_safe_course_id)
        self.assertEqual(type(task.partition_value), unicode)
        self.assertEqual(task.partition_value, expected_partition_value)
