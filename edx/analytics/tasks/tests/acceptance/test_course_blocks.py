"""
End to end test of the LoadAllCourseBlocksWorkflow task.
"""

import logging
import datetime
import json

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase
from edx.analytics.tasks.url import url_path_join, get_target_from_url

log = logging.getLogger(__name__)


class LoadAllCourseBlocksWorkflowAcceptanceTest(AcceptanceTestCase):
    """
    Tests the LoadAllCourseBlocksWorkflow.
    """

    DAILY_PARTITION_FORMAT = '%Y-%m-%d'
    DATE = datetime.date(2016, 9, 8)
    COURSE_IDS = (
        'course-v1:OpenCraft+DnDv2101x+2016',
        'course-v1:OpenCraft+PRDemo1+2016',
        'course-v1:OpenCraft+PRDemo2+2016',
        'course-v1:OpenCraft+VD101x+2016',
    )

    def setUp(self):
        """Copy the input data into place."""
        super(LoadAllCourseBlocksWorkflowAcceptanceTest, self).setUp()

        # Copy course list hive partition data into warehouse
        # Data is sourced from the course list acceptance task's expected output data.
        table_name = 'course_list'
        input_dir = url_path_join(self.data_dir, 'output', table_name)
        daily_partition = self.DATE.strftime(self.DAILY_PARTITION_FORMAT)
        for input_file_name in ('_SUCCESS', 'part-00000', 'part-00001'):
            src = url_path_join(input_dir, input_file_name)
            dst = url_path_join(self.warehouse_path, table_name, "dt=" + daily_partition, input_file_name)
            self.upload_file(src, dst)

        # Copy course blocks REST API data
        file_name = 'course_blocks.json'
        daily_partition = self.DATE.strftime(self.DAILY_PARTITION_FORMAT)
        self.upload_file(url_path_join(self.data_dir, 'input', file_name),
                         url_path_join(self.warehouse_path, 'course_blocks_raw', "dt=" + daily_partition, file_name))


    def test_partition_task(self):
        """Run the LoadAllCourseBlocksWorkflow and test its output."""
        date = self.DATE.strftime('%Y-%m-%d')
        self.task.launch([
            'LoadAllCourseBlocksWorkflow',
            '--date', date,
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
        ])

        self.maxDiff = None
        self.validate_hive()

    def validate_hive(self):
        """Ensure hive partition was created as expected."""

        table_name = 'course_blocks'
        output_dir = url_path_join(self.data_dir, 'output', table_name)
        daily_partition = self.DATE.strftime(self.DAILY_PARTITION_FORMAT)
        for file_name in ('_SUCCESS', 'part-00000', 'part-00001'):
            actual_output_file = url_path_join(self.warehouse_path, table_name, "dt=" + daily_partition, file_name)
            actual_output_target = get_target_from_url(actual_output_file)
            self.assertTrue(actual_output_target.exists(), '{} not created'.format(file_name))
            actual_output = actual_output_target.open('r').read()

            expected_output_file = url_path_join(output_dir, file_name)
            expected_output_target = get_target_from_url(expected_output_file)
            expected_output = expected_output_target.open('r').read()
            self.assertEqual(actual_output, expected_output)

