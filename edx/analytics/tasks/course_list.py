"""
Store course details sourced from the Courses API into a hive table.

See the CourseListApiDataTask and CourseListPartitionTask for details.
"""

import datetime
import logging
import json
import luigi

from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.url import get_target_from_url, url_path_join
from edx.analytics.tasks.util.hive import (
    WarehouseMixin, BareHiveTableTask, HivePartitionTask,
)
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.record import Record, StringField, DateTimeField


log = logging.getLogger(__name__)


class CourseRecord(Record):
    """
    Represents a single course's details as fetched from the edX Courses REST API.
    """
    course_id = StringField(nullable=False, length=255, description='Course identifier.')
    name = StringField(nullable=False, length=255, description='Course name.')
    org = StringField(nullable=False, length=255, description='Course organization.')
    number = StringField(nullable=False, length=255, description='Course number.')
    blocks_url = StringField(nullable=False, description='URL of the course\'s blocks')
    short_description = StringField(nullable=True, length=255, description='Short course description.')
    enrollment_start = DateTimeField(nullable=True, description='Enrollment start date.')
    enrollment_end = DateTimeField(nullable=True, description='Enrollment end date.')
    start_date = DateTimeField(nullable=True, description='Course start date.')
    end_date = DateTimeField(nullable=True, description='Course end date.')
    start_display = StringField(nullable=True, length=255, description='Course start date description.')
    start_type = StringField(nullable=True, length=255, description='Description of effort required.')
    effort = StringField(nullable=True, length=255, description='Description of effort required.')
    pacing = StringField(nullable=True, length=255, description='Description of course pacing strategy.')


class TimestampPartitionMixin(object):
    """
    This mixin provides its task with a formatted date partition value property.

    The partition value is the `date` parameter, formatted by the `partition_date` parameter.

    It can be used by HivePartitionTasks and tasks which invoke downstream HivePartitionTasks.
    """
    date = luigi.DateParameter(
        default=datetime.datetime.utcnow().date(),
        description='Date/time for the data partition.  Default is UTC today.'
                    'Note that though this is a DateParameter, it also supports datetime objects, and so can '
                    'be used to create time-based data partitions.',
    )
    partition_format = luigi.Parameter(
        config_path={'section': 'course_list', 'name': 'partition_format'},
        default='%Y%m%d',
        description="Format string for the course list table partition's `date` parameter.n"
                    "Must result in a filename-safe string, or your partitions will fail to be created.\n"
                    "The default value of '%Y%m%d' changes daily, and so causes a new course partition to to be "
                    "created once a data.  Use '%Y%m%dT%H' to update hourly, though beware of load on the edX "
                    "REST API.  See strftime for options.",
    )

    @property
    def partition_value(self):
        """Partition based on the task's date and partition format strings."""
        return unicode(self.date.strftime(self.partition_format))


class CourseListDownstreamMixin(TimestampPartitionMixin, WarehouseMixin, MapReduceJobTaskMixin):
    """Common parameters used by the Course List Data and Partition tasks."""
    pass


class CourseListApiDataTask(CourseListDownstreamMixin, OverwriteOutputMixin, MapReduceJobTask):
    """
    This task fetches the courses list from the Courses edX REST API.  See the EdxRestApiTask to configure the REST API
    connection parameters.

    The `api_args` parameter defines the query string parameters passed to the REST API call.
    The `api_resource` parameter indicates that this task contacts the Courses API.

    The resulting courses are stored in partitions by task date.

    """
    output_root = luigi.Parameter(
        description='URL where the map reduce data should be stored.',
    )
    api_args = luigi.Parameter(
        config_path={'section': 'course-list', 'name': 'api_args'},
        default='{"page_size": 100}',
        description='JSON structure containing the arguments passed to the courses API.'
                    'Take care if altering these arguments, as many are critical for constructing the full '
                    'course list details. For options, see: '
                    'http://edx.readthedocs.io/projects/edx-platform-api/en/latest/courses/courses.html'
                    '#get-a-list-of-courses',
    )
    api_resource = luigi.Parameter(
        config_path={'section': 'course-list', 'name': 'api_resource'},
        default="courses",
        description='This task contacts the Courses API, which requires the "courses" resource. '
                    'Overwrite this parameter only if the Courses API gets moved or renamed.'
    )

    # Write the output directly to the final destination and rely on the _SUCCESS file to indicate
    # whether or not it is complete. Note that this is a custom extension to luigi.
    enable_direct_output = True

    def __init__(self, *args, **kwargs):
        super(CourseListApiDataTask, self).__init__(*args, **kwargs)
        self.api_args = json.loads(self.api_args)

    def requires(self):
        # Import EdxRestApiTask here so the EMR nodes don't need the edx_rest_api module, or its dependencies
        from edx.analytics.tasks.util.edx_rest_api import EdxRestApiTask
        return EdxRestApiTask(
            resource=self.api_resource,
            arguments=self.api_args,
            date=self.date,
        )

    def mapper(self, line):
        """
        Load each line of JSON-formatted Course data, and ensure the `results` field is present.
        Discard any invalid lines.

        Yields a 20element tuple containing the course's ID and the parsed JSON data as a dict.
        """
        if line is not None:
            try:
                data = json.loads(line)
                courses = data.get('results')
                for course in courses:
                    yield (course['id'], course)
            except ValueError:
                log.error('Unable to parse course API line as JSON: "%s"', line)

    def reducer(self, _key, values):
        """
        Takes the JSON course data and stores it as a CourseRecord.

        Yields the CourseRecord as a tuple.
        """
        if not values:
            return

        # Note that there should only ever by one record in the values list,
        # since the Course API returns one result per course.
        for course_data in values:
            fields = CourseRecord.get_fields()
            record_data = dict()
            for field_name, field_obj in fields.iteritems():

                # Had to rename these fields, since they're named with hive keywords.
                if field_name in ('end', 'start'):
                    field_name += '_date'

                field_value = course_data.get(field_name)
                if field_value is not None:
                    field_value = field_obj.deserialize_from_string(field_value)
                record_data[field_name] = field_value

            record = CourseRecord(**record_data)
            yield record.to_string_tuple()

    def output(self):
        """Expose the partition location target as the output."""
        return get_target_from_url(self.output_root)

    def complete(self):
        """
        The current task is complete if no overwrite was requested,
        and the output_root/_SUCCESS file is present.
        """
        if super(CourseListApiDataTask, self).complete():
            return get_target_from_url(url_path_join(self.output_root, '_SUCCESS')).exists()
        return False

    def run(self):
        """
        Clear out output if data is incomplete, or if overwrite requested.
        """
        if not self.complete():
            self.remove_output_on_overwrite()

        super(CourseListApiDataTask, self).run()


class CourseListTableTask(BareHiveTableTask):
    """Hive table containing the course data, partitioned by date."""

    @property
    def partition_by(self):
        return 'dt'

    @property
    def table(self):
        return 'course_list'

    @property
    def columns(self):
        return CourseRecord.get_hive_schema()

    @property
    def output_root(self):
        """Use the table location path for the output root."""
        return self.table_location

    def output(self):
        """Expose the partition location target as the output."""
        return get_target_from_url(self.output_root)


class CourseListPartitionTask(CourseListDownstreamMixin, HivePartitionTask):
    """A single hive partition of course data."""

    @property
    def hive_table_task(self):
        return CourseListTableTask(
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite,
        )

    @property
    def data_task(self):
        return CourseListApiDataTask(
            output_root=self.output_root,
            overwrite=self.overwrite,
            n_reduce_tasks=self.n_reduce_tasks,
        )

    @property
    def output_root(self):
        """Expose the partition location path as the output root."""
        return self.partition_location

    def output(self):
        """Expose the partition location target as the output."""
        return get_target_from_url(self.output_root)

