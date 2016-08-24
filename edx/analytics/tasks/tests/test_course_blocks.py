"""Test course blocks tasks."""

import os
import json
import shutil
import tempfile
import luigi
from ddt import ddt, data, unpack

from edx.analytics.tasks.course_blocks import (
    CourseBlocksApiDataTask, CourseBlocksPartitionTask,
)
from edx.analytics.tasks.url import get_target_from_url
from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.map_reduce_mixins import MapperTestMixin, ReducerTestMixin
from edx.analytics.tasks.tests.fixtures.helpers import load_fixture


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
        requirements = self.task.requires()
        self.assertEqual(len(requirements), len(course_ids))
        for index, task in enumerate(requirements):
            # Ensure the task was created with course_id as an argument
            self.assertEqual(task.arguments['course_id'], course_ids[index])

            # .. and in the extend_response dict
            self.assertEqual(task.extend_response['course_id'], course_ids[index])


@ddt
class CourseBlocksApiDataMapperTaskTest(CourseBlocksTestMixin, MapperTestMixin, unittest.TestCase):
    """Tests the CourseBlocksApiDataTask mapper output"""

    @data(
        '',
        None,
        'abc',
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

    # data tuple fields are given in this order:
    # (block_id,block_type,display_name,is_root,is_orphan,is_dag,parent_block_id,course_path,sort_idx)
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
            (('abc', 'course', 'ABC', 'True', 'False', 'False', '\\N', '', '0'),)
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
            (('abc', 'block', 'ABC', 'True', 'False', 'False', '\\N', '', '0'),
             ('def', 'block', 'DEF', 'False', 'False', 'False', 'abc', 'ABC', '1'),
             ('stu', 'block', 'STU', 'False', 'False', 'False', 'abc', 'ABC', '2'),
             ('mno', 'block', 'MNO', 'False', 'False', 'False', 'def', 'ABC / DEF', '4'),
             ('pqr', 'block', 'PQR', 'False', 'False', 'False', 'mno', 'ABC / DEF / MNO', '5'),
             ('jkl', 'block', 'JKL', 'False', 'False', 'True', '\\N', '(Multiple locations :)', '7'),
             ('ghi', 'block', 'GHI', 'False', 'True', 'False', '\\N', '(Deleted block :)', '7'),)
        ),
        # A "real" example, taken from the edX Demo course
        ([json.loads(load_fixture('demo_course_blocks.json'))],
         eval(load_fixture('demo_course_blocks_reduced.tuples')),  # pylint: disable=eval-used
         False),
    )
    @unpack
    def test_map_output(self, input_data, expected_tuples, inject_course_id=True):
        # Inject our course_id into the input_data, and expected_values tuples
        if inject_course_id:
            expected_tuples = tuple((values[0],) + (self.course_id,) + values[1:] for values in expected_tuples)
        self._check_output_complete_tuple(
            input_data,
            expected_tuples,
        )

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
            (('abc', 'course', 'ABC', 'True', 'False', 'False', '\\N', '', '0'),)
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
            (('jkl', 'block', 'JKL', 'False', 'False', 'True', '\\N', '(Multiple locations :)', '-1'),
             ('ghi', 'block', 'GHI', 'False', 'True', 'False', '\\N', '(Deleted block :)', '-1'),
             ('abc', 'block', 'ABC', 'True', 'False', 'False', '\\N', '', '0'),
             ('def', 'block', 'DEF', 'False', 'False', 'False', 'abc', 'ABC', '1'),
             ('stu', 'block', 'STU', 'False', 'False', 'False', 'abc', 'ABC', '2'),
             ('mno', 'block', 'MNO', 'False', 'False', 'False', 'def', 'ABC / DEF', '4'),
             ('pqr', 'block', 'PQR', 'False', 'False', 'False', 'mno', 'ABC / DEF / MNO', '5'),)
        ),
    )
    @unpack
    def test_sort_unplaced_blocks(self, input_data, expected_values):

        self.task.sort_unplaced_blocks_up = True

        # Inject our course_id into the input_data, and expected_values tuples
        expected_tuples = tuple((values[0],) + (self.course_id,) + values[1:] for values in expected_values)
        self._check_output_complete_tuple(
            input_data,
            expected_tuples,
        )


class CourseBlocksInputTask(luigi.Task):
    """Use for the CourseBlocksTask.input_task parameter."""
    output_root = luigi.Parameter()

    def output(self):
        return get_target_from_url(self.output_root)

    def complete(self):
        return self.output().exists()


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

    def test_complete(self):
        self.assertFalse(self.task.complete())

        # Create the partition dir, and task reports complete
        os.makedirs(self.task.output_root)
        self.assertTrue(self.task.complete())

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
