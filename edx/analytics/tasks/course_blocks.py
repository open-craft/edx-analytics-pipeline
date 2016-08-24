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


class CourseBlocksDownstreamMixin(TimestampPartitionMixin, WarehouseMixin, MapReduceJobTaskMixin):
    """Common parameters used by the Course Blocks Data and Partition tasks."""

    course_ids = luigi.Parameter(
        default=None,
        description='List of course_id values to fetch course_block data for.  Accepts a list or JSON-formatted string '
                    'defining an array of course_id strings.  Note that the default value is None, but the task will '
                    'not be marked "complete" until a list of course_ids is provided.',
    )
    partition_format = luigi.Parameter(
        config_path={'section': 'course_blocks', 'name': 'partition_format'},
        default='%Y%m%d',
        description="Format string for the course blocks table partition's `date` parameter.n"
                    "Must result in a filename-safe string, or your partitions will fail to be created.\n"
                    "The default value of '%Y%m%d' changes daily, and so causes a new course partition to to be "
                    "created once a day.  For example, use '%Y%m%dT%H' to update hourly, though beware of load on the"
                    "edX REST API.  See strftime for options.",
    )


class CourseBlocksApiDataTask(CourseBlocksDownstreamMixin, OverwriteOutputMixin, MapReduceJobTask):
    """
    This task fetches the course blocks data from the Course Blocks edX REST API. See the EdxRestApiTask to configure
    REST API connection parameters.  Blocks are stored in partitions by task date.

    The `api_args` parameter defines the query string parameters passed to the REST API call which fetches all the
    course blocks, and a list of their children.

    The resulting blocks are then sorted in "course tree traversal" order, and annotated with a `course_path` string,
    which lists all the block's parent block `display_name` values, Section, Subsection, and Unit.  These `display_name`
    values are delimited by the `path_delimiter` parameter.

    There are 4 types of blocks:

    * root: The root block is flagged by the Course Blocks REST API.  They are marked by `is_root=True`.  Will
      also have `parent_block_id=None`, `course_path=''`, and `sort_idx=0`.
    * child: Blocks which have a single parent will have `parent_block_id not None`, and a `course_path` string which
      concatenates the parent block's display_name values.
    * DAC: Blocks which have more than one parent (and thus create directed acyclic graphs, aka DACs) are marked with
      `has_multiple_parents=True`.  Will also have `parent_block_id=None`, and have a `course_path` value configured by
      the `multiple_parent_blocks_path` parameter.
    * Orphan: Blocks which have no parents will have a `course_path` value configured by the `deleted_blocks_path`
      parameter.

    Orphan and DAC blocks can be sorted to the top or bottom of the list by adjusting the `sort_unplaced_blocks_up`
    parameter.

    """
    output_root = luigi.Parameter(
        description='URL where the map reduce data should be stored.',
    )
    api_args = luigi.Parameter(
        config_path={'section': 'course-blocks', 'name': 'api_args'},
        default='{"depth": "all", "requested_fields": "children", "all_blocks": "true"}',
        description='JSON structure containing the arguments passed to the course blocks API.  Course ID will be '
                    'added. Take care if altering these arguments, as many are critical for constructing the full '
                    'course block tree.\n'
                    'If using edx-platform release prior to eucalpytus, use: \n'
                    '    {"depth": "all", "requested_fields": "children", "all_blocks": "true", '
                    '"username": "<oauth_username>"}\n'
                    'For more options, see: '
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

    def requires(self):
        # Determine the list of course_ids to fetch.
        # NB: we don't do this in __init__, because they may be set later, see e.g. LoadCourseBlocksTask.requires()
        if self.course_ids is None:
            course_ids = []
        elif isinstance(self.course_ids, str):
            course_ids = json.loads(self.course_ids)
        else:
            course_ids = self.course_ids  # pylint: disable=redefined-variable-type

        # Import EdxRestApiTask here so the EMR nodes don't need the edx_rest_api module, or its dependencies
        from edx.analytics.tasks.util.edx_rest_api import EdxRestApiTask

        # Require a EdxRestApiTask for each course_id
        requirements = []
        for course_id in course_ids:
            arguments = self.api_args.copy()
            arguments['course_id'] = course_id
            requirements.append(EdxRestApiTask(
                resource=self.api_resource,
                arguments=arguments,
                extend_response=dict(course_id=course_id),
                date=self.date,
                raise_exceptions=False,
            ))

        return requirements

    def mapper(self, line):
        """
        Load each line of course blocks data, and ensure they have the `blocks` and `root` fields.
        Discard any invalid lines.

        Input is JSON-formatted data returned from the Course Blocks API.

        Yields a 2-element tuple containing the course_id and the parsed JSON data as a dict.
        """
        if line is not None:
            try:
                data = json.loads(line)
                root = data.get('root')
                blocks = data.get('blocks', {})
                course_id = data.get('course_id')
                if course_id is not None and root is not None and root in blocks:
                    yield (course_id, data)
                else:
                    log.error('Unable to read course blocks data from "%s"', line)
            except ValueError:
                log.error('Unable to parse course blocks API line as JSON: "%s"', line)

    def reducer(self, key, values):
        """
        Creates a CourseBlock record for each block, including:
        * `course_path` field concatenated from the block's parents' display_name values
        * `sort_idx` for the block, indicating where the block fits into the course in tree traversal order.

        Input is the course block values as a list of dicts, keyed by course_id.

        Yields each CourseBlock record as a tuple, sorted in course tree traversal order.
        """
        course_id = key

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
                    course_id=course_id,
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
        a list of course_ids was requested, and the output_root/_SUCCESS file is present.
        """
        if super(CourseBlocksApiDataTask, self).complete() and self.course_ids is not None:
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
    """Hive table containing the sorted course block data, partitioned on formatted date."""

    @property
    def partition_by(self):
        return 'dt'

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


class CourseBlocksPartitionTask(CourseBlocksDownstreamMixin, HivePartitionTask):
    """A single hive partition of course block data."""

    @property
    def hive_table_task(self):
        return CourseBlocksTableTask(
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite,
        )

    @property
    def data_task(self):
        return CourseBlocksApiDataTask(
            course_ids=self.course_ids,
            output_root=self.output_root,
            overwrite=self.overwrite,
            n_reduce_tasks=self.n_reduce_tasks,
        )

    @property
    def output_root(self):
        """Expose the partition location path as the output root."""
        return self.partition_location

    def output(self):
        """The output of partition tasks is the partition location."""
        return get_target_from_url(self.output_root)


class LoadCourseBlocksTask(WarehouseMixin, OverwriteOutputMixin, MapReduceJobTaskMixin, luigi.Task):
    """
    Reads the `course_id` field out of the hive records in `input_task.output_root`, and spawns a
    CourseBlocksPartitionTask using the list of course_ids.

    We accomplish this by making the `input_task` a required task for LoadCourseBlocksTask.  Once `input_task` is
    complete, then require a CourseBlocksPartitionTask for each course read from the `input_task.output_root`.
    Because `input_task` is a luigi.Task instance, this task cannot be called from the command line, and must be invoked
    via a workflow task like LoadAllCourseBlocksWorkflow.

    This task writes a `marker` file on successful completion, with its name hashed to include this task's current
    parameters.  If the `marker` file exists, then the task won't re-process the data in `input_task`.

    """
    date = luigi.DateParameter(
        description='Upper bound date/time for the generated course blocks partitions.'
    )
    input_task = luigi.Parameter(
        description='Luigi.task instance whose hive output contains a list of courses whose course_blocks we want '
                    'to load.  e.g. a CourseListPartitionTask instance')

    course_id_index = luigi.IntParameter(
        config_path={'section': 'course-blocks', 'name': 'course_id_index'},
        default=0,
        description='Index of the course_id field in records contained in input_task.output_root.  Tab-separated hive '
                    'records are read raw from the input_task.output_root, and parsed into tuples.  This field '
                    'indicates which item in the tuple contains the course_id.',
    )
    marker = luigi.Parameter(
        config_path={'section': 'course-blocks', 'name': 'load_blocks_marker'},
        significant=False,
        description='URL directory where marker files will be written on task completion.'
                    ' Note that the report task will not run if this marker file exists.',
    )

    def __init__(self, *args, **kwargs):
        super(LoadCourseBlocksTask, self).__init__(*args, **kwargs)
        marker_url = url_path_join(self.marker, str(hash(self)))
        self.marker_target = get_target_from_url(marker_url)

        # We'll require the course_blocks_ task once we've assembled the list of course_ids
        self.unique_course_ids = None
        self.course_blocks_task = CourseBlocksPartitionTask(
            date=self.date,
            mapreduce_engine=self.mapreduce_engine,
            input_format=self.input_format,
            lib_jar=self.lib_jar,
            n_reduce_tasks=self.n_reduce_tasks,
            remote_log_level=self.remote_log_level,
            warehouse_path=self.warehouse_path,
        )

    def requires(self):
        """
        Returns the tasks required to load the course blocks from the courses output by the input_task.

        Note:  Because we use an outdated luigi library, we are unable to take advantage of the dynamic dependencies
        feature which has been available since v1.2.1:

            https://github.com/spotify/luigi/blob/master/doc/tasks.rst#dynamic-dependencies

        So we have to trick luigi into allowing us to have dynamic dependencies by changing the requirements during the
        task run.  This is undesirable, but unfortunately necessary.  The luigi.worker throws and catches a
        RuntimeError: "Unfulfilled dependencies at run time" exception once, but then re-schedules the offending task to
        finish executing its dependent tasks.

        This method returns self.input_task, and self.course_blocks task as requirements.

        However, once self.input_task is complete, we reads its output to get a list of course_ids, which the
        CourseBlocksApiDataTask uses to generate its list of require() tasks.

        """
        if self.input_task.complete():
            if self.unique_course_ids is None:
                self.unique_course_ids = dict()

                input_target = get_target_from_url(self.input_task.output_root)
                for line in input_target.open('r'):

                    # Each line is from a hive table, so it's tab-separated
                    record = tuple(line.rstrip('\r\n').split('\t'))

                    # If the record contains a potentially valid course_id, add it to the list
                    if len(record) > self.course_id_index:

                        course_id = record[self.course_id_index]
                        if course_id != DEFAULT_NULL_VALUE:
                            self.unique_course_ids[course_id] = True

                self.course_blocks_task.course_ids = self.unique_course_ids.keys()

        return (self.input_task, self.course_blocks_task)

    @property
    def output_root(self):
        """Return the course_blocks_task output_root."""
        return self.course_blocks_task.output_root

    def output(self):
        """Return the course_blocks_task output."""
        return self.course_blocks_task.output()

    def run(self):
        """
        Clear out marker file if overwrite requested.
        """
        if not self.complete():
            self.remove_output_on_overwrite()

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
        # the course_list_task in as an input task.
        all_course_blocks_task = LoadCourseBlocksTask(
            input_task=course_list_task,
            **kwargs
        )

        # Here, we yield only the course_blocks task, because it includes course_list task as a required task.
        # See LoadCourseBlocksTask.requires() for details.
        yield all_course_blocks_task
