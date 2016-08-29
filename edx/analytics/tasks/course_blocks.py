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
    is_orphan = BooleanField(default=False, nullable=False,
                             description='True if the block has no parent nodes, but is not a root node.')
    is_dag = BooleanField(default=False, nullable=False,
                          description='True if the block has more than one parent, making the course '
                                      'a Directed Acyclic Graph.  If True, then parent_block_id is null.')
    parent_block_id = StringField(length=255, nullable=True,
                                  description='Block identifier for the block\'s parent, iff only one is found.')
    course_path = StringField(nullable=True,
                              description='Concatenated string of parent block display_name values, from '
                                          'the root node to the parent_block_id.  See `CourseBlocksApiDataTask` for '
                                          'details on how this value is set for different types of blocks.')
    sort_idx = IntegerField(nullable=True,
                            description='Number indicating the position that this block holds in a course-outline '
                                        'sorted list of blocks. See `CourseBlocksApiDataTask.sort_unplaced_blocks_up` '
                                        'for how this value is set for different types of blocks.')


class CourseBlocksDownstreamMixin(TimestampPartitionMixin, WarehouseMixin, MapReduceJobTaskMixin):
    """Common parameters used by the Course Blocks Data and Partition tasks."""

    course_ids = luigi.Parameter(
        description='List of course_id values to fetch course_block data for.  Accepts a list or JSON-formatted string '
                    'defining an array of course_id strings.'
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
    * DAG: Blocks which have more than one parent (and thus create directed acyclic graphs, aka DAGs) are marked with
      `is_dag=True`.  Will also have `parent_block_id=None`, and have a `course_path` value configured by
      the `multiple_parent_blocks_path` parameter.
    * Orphan: Blocks with no parents are marked with `is_orphan=True`.  Will have a `course_path` value configured by
      the `deleted_blocks_path` parameter.

    Orphan and DAG blocks can be sorted to the top or bottom of the list by adjusting the `sort_unplaced_blocks_up`
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
        description='Mark deleted (orphan) blocks with this string in course_path.',
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
        if isinstance(self.course_ids, basestring):
            self.course_ids = tuple(json.loads(self.course_ids))
        elif self.course_ids is None:
            self.course_ids = ()

    def requires(self):
        """Require a EdxRestApiTask for each course_id in the given list."""

        # Import EdxRestApiTask here so the EMR nodes don't need the edx_rest_api module, or its dependencies
        from edx.analytics.tasks.util.edx_rest_api import EdxRestApiTask

        # Require a EdxRestApiTask for each course_id
        requirements = []
        for course_id in self.course_ids:
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
                is_dag = block.get('is_dag', False)
                is_orphan = False
                course_path = u''
                sort_idx = block.get('sort_idx', no_sort_idx)

                if is_dag:
                    course_path = unicode(self.multiple_parent_blocks_path)
                elif not is_root:
                    if len(parents) == 0:
                        is_orphan = True
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
                    is_orphan=is_orphan,
                    is_dag=is_dag,
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
        return (super(CourseBlocksApiDataTask, self).complete() and
                get_target_from_url(url_path_join(self.output_root, '_SUCCESS')).exists())

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

        if not root.get('is_dag', False):
            root['sort_idx'] = sort_idx
            sort_idx += 1

        for child_id in children:
            child = blocks.get(child_id, {})

            # Child has multiple parents.  Abort!
            if 'parents' in child:
                child['is_dag'] = True
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
    """
    A single hive partition of course block data.

    If no `course_ids` list is provided, then this task reads the `course_id` field out of the hive records in
    `input_root`, and uses them to populate the CourseBlocksPartitionTask `data_task.course_ids` list.

    **WARNING**

    If no `course_ids` are provided, there is an implied requirement that the `input_root` exists so this task will run
    without errors.  Because the CourseBlocksPartitionTask must know its list of `course_ids` before it can determine
    its own requirements, this course_ids list constitutes a dynamic task dependency.  Because we use an outdated luigi
    library, we are unable to take advantage of the dynamic dependencies feature which has been available since v1.2.1:

        https://github.com/spotify/luigi/blob/master/doc/tasks.rst#dynamic-dependencies

    So we have to trick luigi into allowing us to have dynamic dependencies by changing the requirements during the
    task run.  The luigi.worker throws and catches a RuntimeError: "Unfulfilled dependencies at run time" exception
    once, but then re-schedules the offending task to finish executing its dependent tasks.

    To avoid this issue, you can ensure that the `input_root` exists by running the CourseListPartitionTask for the
    current date partition before running this task.
    """
    course_ids = luigi.Parameter(
        default=None,
        description='List of course_id values to fetch course_block data for.  Accepts a list or JSON-formatted string '
                    'defining an array of course_id strings.  Either `input_root` or a `course_ids` must be provided.'
    )
    input_root = luigi.Parameter(
        default=None,
        description='URL pointing to the course_list partition data, containing the list of courses to load. '
                    'Either `input_root` or `course_ids` must be provided.'
    )
    course_id_index = luigi.IntParameter(
        default=0,
        config_path={'section': 'course-blocks', 'name': 'course_id_index'},
        description='Index of the course_id field in records contained in input_root.  Tab-separated hive records are '
                    'read raw from the input_root, and parsed into tuples.  This field indicates which item in the '
                    'tuple contains the course_id.',
    )

    def __init__(self, *args, **kwargs):
        super(CourseBlocksPartitionTask, self).__init__(*args, **kwargs)
        assert self.input_root is not None or self.course_ids is not None, "Must provide input_root or course_ids."

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

    def requires(self):
        """
        Yields the hive_table_task, and once the course_ids list has been populated, the data_task.
        """
        if self.course_ids is None:
            input_target = get_target_from_url(self.input_root)
            if input_target.exists():
                unique_course_ids = dict()
                for line in input_target.open('r'):

                    # Each line is from a hive table, so it's tab-separated
                    record = tuple(line.rstrip('\r\n').split('\t'))

                    # If the record contains a potentially valid course_id, add it to the list
                    if len(record) > self.course_id_index:
                        course_id = record[self.course_id_index]
                        if course_id != DEFAULT_NULL_VALUE:
                            unique_course_ids[course_id] = True

                self.course_ids = sorted(unique_course_ids.keys())

        if self.course_ids is not None:
            yield self.data_task

        yield self.hive_table_task


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

    **WARNING**

    Running this task may result in a temporary RuntimeError: "Unfulfilled dependencies at run time" exception thrown by
    the luigi.worker, due to the dynamic dependency of CourseBlocksPartitionTask on CourseListPartitionTask.  The
    luigi.worker will handle this error and reschedule the missing dependencies, however the error will still be visible
    in the Traceback.

    See CourseBlocksPartitionTask for details.
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
        # in the course_list_task's output as input.
        course_blocks_task = CourseBlocksPartitionTask(
            input_root=course_list_task.output_root,
            **kwargs
        )

        # Order is important here, and unintuitive.
        # The course_blocks task requires the course_list task's output,
        # but we have to yield the course_blocks task first.
        yield course_blocks_task
        yield course_list_task
