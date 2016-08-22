"""
Store course block details sourced from the Course Blocks API into a hive table.

See LoadAllCourseBlocksWorkflow for details.
"""

import logging
import json
import datetime
import luigi

from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.url import get_target_from_url, url_path_join
from edx.analytics.tasks.util.hive import (
    WarehouseMixin, BareHiveTableTask, HivePartitionTask,
)
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.record import Record, BooleanField, StringField, IntegerField, DEFAULT_NULL_VALUE
from edx.analytics.tasks.util.opaque_key_util import get_filename_safe_course_id
from edx.analytics.tasks.decorators import workflow_entry_point
from edx.analytics.tasks.course_list import TimestampPartitionMixin, CourseListPartitionTask


log = logging.getLogger(__name__)


class CourseBlockRecord(Record):
    """
    Represents a course block as fetched from the edX Course Blocks REST API, augmented with details about its
    position in the course.
    """
    block_id = StringField(length=255, nullable=False, description='Block identifier.')
    course_id = StringField(length=255, nullable=False, description='Identifier for the course containing the block.')
    block_type = StringField(length=255, nullable=False, description='Block type.')
    display_name = StringField(length=255, nullable=False, description='User-facing title of the block.')
    is_root = BooleanField(default=False, nullable=False,
                           description='True if the block is the course\'s root node.')
    has_multiple_parents = BooleanField(default=False, nullable=False,
                                        description='True if the block has more than one parent, making the course '
                                                    'a directed acyclic graph.  If True, then parent_block_id and '
                                                    'course_path will be null.')

    parent_block_id = StringField(length=255, nullable=True,
                                  description='Block identifier for the block\'s parent. If null, and '
                                              'is_root and has_multiple_parents are False, then this block has been '
                                              'removed from the course.')

    course_path = StringField(nullable=True,
                              description='Concatenated string of parent block display_name values, from '
                                          'the root node to the parent_block_id.  Will be null if '
                                          'has_multiple_parents.')
    sort_idx = IntegerField(nullable=True,
                            description='Number indicating the position that this block holds in a course-outline '
                                        'sorted list of blocks.   Will be null if has_multiple_parents.')


class CourseIdTimestampPartitionMixin(TimestampPartitionMixin):
    """
    This mixin augments the TimestampPartitionMixin by adding a course_id parameter to the partition value,
    property, allowing data to be partitioned on a combined index of course_id and a formatted datetime stamp.

    It can be used by HivePartitionTasks and tasks which invoke downstream HivePartitionTasks.
    """
    course_id = luigi.Parameter(
        description='Course identifier to include in the data partition value.',
    )
    partition_format = luigi.Parameter(
        config_path={'section': 'course_blocks', 'name': 'partition_format'},
        default='{course_id}_%Y%m%d',
        description="Format string for the course blocks table partition, combining the values of the course_id and "
                    "date/time task parameters.\n"
                    "Must result in a filename-safe string, or your partitions will fail to be created.\n"
                    "The format string includes these parts:\n"
                    "* {course_id}: a filename-safe version of the task's `course_id`\n"
                    "* datetime format string:  used to format the `date` parameter.\n"
                    "  The default value of '%Y%m%d' changes daily, and so causes a new course partition to to be \n"
                    "  created once a data.  Use '%Y%m%dT%H' to update hourly, though beware of load on the edX "
                    "  REST API.  See strftime for options.",
    )

    @property
    def partition_value(self):
        """Partition based on the given course_id, datetime and partition format strings."""
        # format the datetime first
        datetime_format = super(CourseIdTimestampPartitionMixin, self).partition_value

        # then apply the course_id
        return unicode(datetime_format.format(course_id=self.course_id))
        #FIXME return unicode(datetime_format.format(course_id=get_filename_safe_course_id(self.course_id)))


class CourseBlocksDownstreamMixin(CourseIdTimestampPartitionMixin, WarehouseMixin, MapReduceJobTaskMixin):
    """Common parameters used by the Course Blocks Data and Partition tasks."""
    pass


class CourseBlocksApiDataTask(CourseBlocksDownstreamMixin, OverwriteOutputMixin, MapReduceJobTask):
    """
    This task fetches the course blocks data from the Course Blocks edX REST API. See the EdxRestApiTask to configure
    REST API connection parameters.  Blocks are stored in partitions by course_id, and task date.

    The `api_args` parameter defines the query string parameters passed to the REST API call, and is set to a default
    which fetches all the course blocks, and a list of their children, which is required for determining the block's
    `course_path` and sort order.

    The resulting blocks are then sorted in "course tree traversal" order, and annotated with a `course_path` string,
    which lists all the block's parent block `display_name` values, Section, Subsection, and Unit.  These `display_name`
    values are delimited by the `path_delimiter` parameter.

    Blocks which have more than one parent (and create directed acyclic graphs, called DACs) are marked with
    `has_multiple_parents`, and given a `course_path` value configured by the `multiple_parent_blocks_path` parameter.

    Blocks which have no parents (orphans) are given a `course_path` value configured by the `deleted_blocks_path`
    parameter.

    Orphan and DAC blocks can be sorted to the top or bottom of the list by adjusting the `sort_unplaced_blocks_up`
    parameter.
    """

    output_root = luigi.Parameter(
        description='URL where the map reduce data should be stored.',
    )
    api_args = luigi.Parameter(
        config_path={'section': 'course-blocks', 'name': 'api_args'},
        default='{"depth": "all", "block_types_filter": "course,sequential,vertical", '
                '"requested_fields": "children", "all_blocks": "true"}',
        description='JSON structure containing the arguments passed to the course blocks API.  Course ID will be '
                    'added. Take care if altering these arguments, as many are critical for constructing the full '
                    'course block tree. For options, see: '
                    'http://edx.readthedocs.io/projects/edx-platform-api/en/latest/courses/blocks.html'
                    '#get-a-list-of-course-blocks-in-a-course',
    )
    api_resource = luigi.Parameter(
        config_path={'section': 'course-blocks', 'name': 'api_resource'},
        default="blocks",
        description='This task contacts the Course Blocks API, which requires the "blocks" resource. '
                    'Overwrite this parameter only if the Course Blocks API gets moved or renamed.'
    )

    path_delimiter = luigi.Parameter(
        config_path={'section': 'course-blocks', 'name': 'path_delimiter'},
        default=' / ',
        description='String used to delimit the course path sections when assembling the full block location.',
    )
    deleted_blocks_path = luigi.Parameter(
        config_path={'section': 'course-blocks', 'name': 'deleted_blocks_path'},
        default='(Deleted block :)',
        description='Mark deleted (unparented) blocks with this string in course_path.',
    )
    multiple_parent_blocks_path = luigi.Parameter(
        config_path={'section': 'course-blocks', 'name': 'multiple_parent_blocks_path'},
        default='(Multiple locations :)',
        description='Mark blocks with more than one parent with this string in course_path.',
    )
    sort_unplaced_blocks_up = luigi.BooleanParameter(
        config_path={'section': 'course-blocks', 'name': 'sort_unplaced_blocks_up'},
        default=False,
        description='If True, any deleted (orphan) or multi-parented blocks will be pushed to the top of the list '
                    '(in an indeterminate order).  If False, these blocks will be pushed to the bottom.'
    )

    # Write the output directly to the final destination and rely on the _SUCCESS file to indicate
    # whether or not it is complete. Note that this is a custom extension to luigi.
    enable_direct_output = True

    def __init__(self, *args, **kwargs):
        super(CourseBlocksApiDataTask, self).__init__(*args, **kwargs)
        self.api_args = json.loads(self.api_args)
        self.api_args['course_id'] = self.course_id

    def requires(self):
        # Import EdxRestApiTask here so the EMR nodes don't need the edx_rest_api module, or its dependencies
        from edx.analytics.tasks.util.edx_rest_api import EdxRestApiTask
        return EdxRestApiTask(
            resource=self.api_resource,
            arguments=self.api_args,
            date=self.date,
            raise_exceptions=False,
        )

    def mapper(self, line):
        """
        Load each line of JSON-formatted Course Blocks data, and ensure they have the `blocks` and `root` fields.
        Discard any invalid lines.

        Yields a 2-element tuple containing the course_id and the parsed JSON data as a dict.
        """
        if line is not None:
            try:
                data = json.loads(line)
                root = data.get('root')
                blocks = data.get('blocks', {})
                if root and root in blocks:
                    yield (self.course_id, data)
            except ValueError:
                log.error('Unable to parse course blocks API line as JSON: "%s"', line)

    def reducer(self, _key, values):
        """
        Takes the JSON course block data, grouped by course_id, and sorts it in course tree traversal order.

        Concatenates the `course_path` field from the block's parents, and creates a CourseBlock record for each block.

        Yields the CourseBlock record as a tuple.
        """
        if not values:
            return

        for course_data in values:
            root_id = course_data.get('root')
            blocks = course_data.get('blocks', {})
            self._index_children(root_id, blocks)

            # Sort unplaced (orphan, multi-parent) blocks towards the top or bottom, as configured
            if self.sort_unplaced_blocks_up:
                no_sort_idx = -1
            else:
                no_sort_idx = len(blocks.keys())

            def order_by_sort_idx(key, blocks=blocks, default_sort_idx=no_sort_idx):
                """Function to sort the blocks on sort_idx"""
                return blocks[key].get('sort_idx', default_sort_idx)

            for block_id in sorted(blocks.keys(), key=order_by_sort_idx):
                block = blocks[block_id]
                is_root = (block_id == root_id)
                parents = block.get('parents', [])
                has_multiple_parents = block.get('has_multiple_parents', False)
                course_path = u''
                sort_idx = block.get('sort_idx', no_sort_idx)

                if has_multiple_parents:
                    course_path = unicode(self.multiple_parent_blocks_path)
                elif not is_root:
                    if len(parents) == 0:
                        course_path = unicode(self.deleted_blocks_path)
                    else:
                        for parent_id in parents:
                            parent = blocks[parent_id]
                            if len(course_path) > 0:
                                course_path += unicode(self.path_delimiter)
                            course_path += parent['display_name']

                record = CourseBlockRecord(
                    course_id=self.course_id,
                    block_id=block.get('id'),
                    block_type=block.get('type'),
                    display_name=block.get('display_name'),
                    parent_block_id=block.get('parent_block_id'),
                    is_root=is_root,
                    has_multiple_parents=has_multiple_parents,
                    course_path=course_path,
                    sort_idx=sort_idx,
                )

                yield record.to_string_tuple()

    def output(self):
        return get_target_from_url(self.output_root)

    def complete(self):
        """
        The current task is complete if no overwrite was requested,
        and the output_root/_SUCCESS file is present.
        """
        if super(CourseBlocksApiDataTask, self).complete():
            return get_target_from_url(url_path_join(self.output_root, '_SUCCESS')).exists()
        return False

    def run(self):
        """
        Clear out output if data is incomplete, or if overwrite requested.
        """
        if not self.complete():
            self.remove_output_on_overwrite()

        super(CourseBlocksApiDataTask, self).run()

    def _index_children(self, root_id, blocks, sort_idx=0, more=None):
        """
        Applies a sort_idx and parents list to all the blocks in the list.
        Blocks are sorted in "tree traversal" order: https://en.wikipedia.org/wiki/Tree_traversal
        """
        root = blocks.get(root_id, {})

        if more is None:
            more = []

        children = root.get('children', [])
        more += children

        if not root.get('has_multiple_parents', False):
            root['sort_idx'] = sort_idx
            sort_idx += 1

        for child_id in children:
            child = blocks.get(child_id, {})

            # Child has multiple parents.  Abort!
            if 'parents' in child:
                child['has_multiple_parents'] = True
                del child['sort_idx']
                del child['parent_block_id']
                del child['parents']
            else:
                child['parents'] = root.get('parents', [])[:]
                child['parents'].append(root_id)
                child['parent_block_id'] = root_id

        if more:
            sort_idx = self._index_children(more[0], blocks, sort_idx, more[1:])

        return sort_idx


class CourseBlocksTableTask(BareHiveTableTask):
    """Hive table containing the sorted course block data, partitioned on course_id + date."""

    @property
    def partition_by(self):
        return 'courseid_dt'

    @property
    def table(self):
        return 'course_blocks'

    @property
    def columns(self):
        return CourseBlockRecord.get_hive_schema()

    @property
    def output_root(self):
        """Use the table location path for the output root."""
        return self.table_location

    def output(self):
        return get_target_from_url(self.output_root)


class CourseBlocksPartitionTask(CourseBlocksDownstreamMixin, HivePartitionTask):
    """A single hive partition of course block data."""

    @property
    def output_root(self):
        """Expose the partition location path as the output root."""
        return self.partition_location

    @property
    def hive_table_task(self):
        return CourseBlocksTableTask(
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite,
        )

    @property
    def data_task(self):
        return CourseBlocksApiDataTask(
            course_id=self.course_id,
            output_root=self.output_root,
            overwrite=self.overwrite,
            n_reduce_tasks=self.n_reduce_tasks,
        )


class LoadCourseBlocksTask(WarehouseMixin, OverwriteOutputMixin, MapReduceJobTaskMixin, luigi.Task):
    """
    Runs the `input_task', then reads the `course_id` field out of the hive records in `input_task.output_root`, and
    spawns a CourseBlocksPartitionTask for each course found.

    Because this task needs to spawn new luigi tasks, it has to to do so in the requires() method.  This means we
    can't take advantage of the uniqueness and sorting filters of map/reduce.

    This task writes a `marker` file on successful completion, with its name hashed to include this task's current
    parameters.  If the `marker` file exists, then the task won't re-process the data produced by `input_task`.

    """
    input_task = luigi.Parameter(
        description='Task which will produce the list of courses whose blocks need to be loaded.  Will be run by '
                    'this task\'s run() method.'
    )
    date = luigi.DateParameter(
        description='Upper bound date/time for the generated course blocks partitions.'
    )
    marker = luigi.Parameter(
        config_path={'section': 'course-blocks', 'name': 'load_blocks_marker'},
        significant=False,
        description='URL directory where marker files will be written on task completion.'
                    ' Note that the report task will not run if this marker file exists.',
    )
    course_id_index = luigi.IntParameter(
        config_path={'section': 'course-blocks', 'name': 'course_id_index'},
        default=0,
        description='Index of the course_id field in the given input_task.output_root records.  Tab-separated hive '
                    'records are read raw from the given input_task.output_root, and parsed into tuples.  This field '
                    'indicates which item in the tuple contains the course_id.',
    )

    def __init__(self, *args, **kwargs):
        super(LoadCourseBlocksTask, self).__init__(*args, **kwargs)
        marker_url = url_path_join(self.marker, str(hash(self)))
        self.marker_target = get_target_from_url(marker_url)

    def requires(self):
        # Input task must be run first
        log.error("** LoadCourseBlocksTask.requires")
        return self.input_task

    def complete(self):
        """
        The current task is complete if no overwrite was requested,
        and the marker file is present.
        """
        log.error("** LoadCourseBlocksTask.complete ")
        if super(LoadCourseBlocksTask, self).complete():
            log.error("** LoadCourseBlocksTask.complete : super true, this %s" % self.output().exists())
            return self.output().exists()
        log.error("** LoadCourseBlocksTask.complete super False")
        return False

    def output(self):
        """
        Use the marker location as an indicator of task "completeness".
        """
        log.error("** LoadCourseBlocksTask.output : %s" % self.marker_target)
        return self.marker_target

    '''
    def on_success(self):
        """
        Required tasks ran successfully, so write the marker file.
        """
        with self.output().open('w') as marker_file:
            log.error("** LoadCourseBlocksTask.success, writing %s" % self.output())
            marker_file.write('done')
    '''

    def run(self):
        """
        Clear out marker file if overwrite requested.
        """
        log.error("** LoadCourseBlocksTask.run removing %s" % self.output())
        self.remove_output_on_overwrite()

        """
        Parse the hive files in input_task.output_root to collect course_id values, and create CourseBlockPartitionTasks
        to load the course blocks for that course.
        #FIXME def _get_requirements(self):
        """
        loaded_courses = dict()
        input_target = get_target_from_url(self.input_task.output_root)
        for line in input_target.open('r'):

            # Each line is from a hive table, so it's tab-separated
            record = tuple(line.rstrip('\r\n').split('\t'))

            # If the record contains a potentially valid course_id,
            # create a task to load its course blocks.
            if len(record) > self.course_id_index:

                course_id = record[self.course_id_index]
                if course_id != DEFAULT_NULL_VALUE and course_id not in loaded_courses:

                    blocks_task = CourseBlocksPartitionTask(
                        course_id=course_id,
                        date=self.date,
                        mapreduce_engine=self.mapreduce_engine,
                        input_format=self.input_format,
                        lib_jar=self.lib_jar,
                        n_reduce_tasks=self.n_reduce_tasks,
                        remote_log_level=self.remote_log_level,
                        warehouse_path=self.warehouse_path,
                    )

                    log.error("** LoadCourseBlocksTask.yielding %s" % blocks_task)
                    yield blocks_task
                    loaded_courses[course_id] = blocks_task

        with self.output().open('w') as marker_file:
            log.error("** LoadCourseBlocksTask.success, writing %s" % self.output())
            marker_file.write('done')

        super(LoadCourseBlocksTask, self).run()


@workflow_entry_point
class LoadAllCourseBlocksWorkflow(WarehouseMixin, MapReduceJobTaskMixin, luigi.WrapperTask):
    """
    This task uses the edX Courses REST API to fetch all of the available courses, then uses that data to contact the
    edX Course Blocks REST API for each course, to load all the course blocks in each course.

    All of the tasks involved in this process create partitions and cache files, which are keyed off the `date`
    parameter.  As this `date` parameter changes, new data will be fetched and processed.  This can be a
    resource-intensive task, both for the pipeline and the edX REST API, so choose your `date`s with care.

    The tasks in this workflow write their data to hive tables, which can be overridden using `hive_overwrite`.
    See the CourseListPartitionTask and CourseBlocksPartitionTask for details on how the data is partitioned,
    and the CourseListApiDataTask and CourseBlocksApiDataTask for details on how the REST API data is cached.

    """
    date = luigi.DateParameter(
        default=datetime.datetime.utcnow().date(),
        description='Date/time for the course list and blocks partitions, cache and manifest files. '
                    'Default is UTC today.'
    )
    hive_overwrite = luigi.BooleanParameter(
        default=False,
        description='Whether or not to rebuild hive data from the REST API data.'
    )

    def requires(self):
        """
        Initialize the tasks in this workflow
        """
        # Args shared by all tasks
        kwargs = dict(
            date=self.date,
            mapreduce_engine=self.mapreduce_engine,
            input_format=self.input_format,
            lib_jar=self.lib_jar,
            n_reduce_tasks=self.n_reduce_tasks,
            remote_log_level=self.remote_log_level,
            overwrite=self.hive_overwrite,
        )

        # Initialize the course list partition task, partitioned on date
        course_list_task = CourseListPartitionTask(
            **kwargs
        )

        # Initialize the all course blocks data task, feeding
        # the output from the course_list_task in as input.
        all_course_blocks_task = LoadCourseBlocksTask(
            input_task=course_list_task,
            **kwargs
        )

        # Order is important here, and unintuitive:
        #  course_blocks depends on course_list, yet it goes last.
        yield (
            all_course_blocks_task,
        )
