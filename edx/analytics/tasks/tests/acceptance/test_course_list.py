"""
End to end test of the course list hive partition task.
"""

import logging
import datetime

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase
from edx.analytics.tasks.url import url_path_join, get_target_from_url
from edx.analytics.tasks.util.edx_rest_api import EdxRestApiTask

log = logging.getLogger(__name__)


class CourseListPartitionTaskAcceptanceTest(AcceptanceTestCase):
    """
    Tests the CourseListPartitionTask.
    """

    DAILY_PARTITION_FORMAT = '%Y%m%d'
    DATE = datetime.date(2016, 9, 8)

    def setUp(self):
        """Copy the input data into place."""
        super(CourseListPartitionTaskAcceptanceTest, self).setUp()

        # Have to instantiate the EdxRestApiTask to figure out the manifest and json file names,
        # since these names are determined from the task input parameters.
        edx_rest_api_config = {}
        edx_rest_api_config.update(self.config.get('edx-rest-api', {}))
        edx_rest_api_config.update(self.task.default_config_override.get('edx-rest-api', {}))
        edx_rest_api_config['date'] = self.DATE.strftime('%Y-%m-%d')
        edx_rest_api_config['resource'] = 'courses'
        edx_rest_api_config['arguments'] = {u'page_size': 100}
        edx_rest_api_task = EdxRestApiTask(**edx_rest_api_config)
        manifest_dst = edx_rest_api_task.manifest
        task_hash = hash(edx_rest_api_task)

        # Copy course list REST API cache, and generate the manifest file content
        input_dir = url_path_join(self.data_dir, 'input', 'course_list')
        manifest = ''

        for num in range(1, 5):
            src = url_path_join(input_dir, '{num:d}.json'.format(num=num))
            dst = url_path_join(self.edx_rest_api_cache_root, '{hash}-{num:d}.json'.format(hash=task_hash, num=num))
            manifest += "{dst}\n".format(dst=dst)
            self.upload_file(src, dst)

        # Write manifest file
        self.upload_file_with_content(manifest_dst, manifest)

    def test_problem_response_report(self):
        """Run the CourseListPartitionTask and test its output."""
        date = self.DATE.strftime('%Y-%m-%d')
        self.task.launch([
            'CourseListPartitionTask',
            '--date', date,
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
        ])

        self.maxDiff = None
        self.validate_hive()

    def validate_hive(self):
        """Ensure hive partition was created as expected."""

        table_name = 'course_list'
        input_dir = url_path_join(self.data_dir, 'output')
        daily_partition = self.DATE.strftime(self.DAILY_PARTITION_FORMAT)
        for file_name in ('_SUCCESS', 'part-00000', 'part-00001'):
            actual_output_file = url_path_join(self.warehouse_path, table_name, "dt=" + daily_partition, file_name)
            actual_output_target = get_target_from_url(actual_output_file)
            self.assertTrue(actual_output_target.exists(), '{} not created'.format(file_name))
            actual_output = actual_output_target.open('r').read()

            expected_output_file = url_path_join(input_dir, table_name, file_name)
            expected_output_target = get_target_from_url(expected_output_file)
            expected_output = expected_output_target.open('r').read()
            self.assertEqual(actual_output, expected_output)
