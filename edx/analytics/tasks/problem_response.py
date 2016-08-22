"""
Luigi tasks for extracting the latest problem response data from tracking log files.
"""
import re
import csv
import ast
import json
import datetime
import logging
import luigi

from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin, MultiOutputMapReduceJobTask
from edx.analytics.tasks.pathutil import EventLogSelectionDownstreamMixin, EventLogSelectionMixin
from edx.analytics.tasks.url import get_target_from_url, url_path_join
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.decorators import workflow_entry_point
from edx.analytics.tasks.util.hive import BareHiveTableTask, HivePartitionTask
from edx.analytics.tasks.util.opaque_key_util import get_filename_safe_course_id
from edx.analytics.tasks.util.record import (
    Record, StringField, StringListField, IntegerField, DateTimeField, FloatField, BooleanField,
)
from edx.analytics.tasks.course_list import TimestampPartitionMixin
from edx.analytics.tasks.course_blocks import (
    CourseBlockRecord, CourseIdTimestampPartitionMixin, LoadCourseBlocksTask,
)
from edx.analytics.tasks.answer_dist import ProblemCheckEventMixin, get_problem_check_event

log = logging.getLogger(__name__)


class ProblemResponseRecord(Record):
    """
    Record containing the data for a single user's response to a problem, in a given date range.

    If there are multiple questions in a problem, they are spread over separate ProblemResponseRecords.

    Note that the course_id field is available from the partition string.
    """
    # Fields that provide the unique key for each record
    course_id = StringField(description='Course containing the problem.')
    answer_id = StringField(description='Learner\'s answer ID.')

    # Remaining data fields
    problem_id = StringField(description='Problem\'s block usage ID.')
    problem = StringField(description='Problem display name, at time of answering.')
    location = StringField(description='Problem location in the course, concatenated from Section, Subsection, Unit, '
                                       'and problem display name.')
    username = StringField(description='Learner\'s username.')
    question = StringField(description='Question\'s display name, at time of answering.')
    score = FloatField(description='Score achieved by the learner.')
    max_score = FloatField(description='Maximum possible score for the problem.')
    correct = BooleanField(nullable=True, description='True if all answers are correct; '
                                                      'False if any answers are not correct; '
                                                      'None if any answers have unknown correctness.')
    answer = StringListField(description='List of answers the user chose for the question.')
    total_attempts = IntegerField(description='Total number of attempts the user has made on the problem.')
    first_attempt_date = DateTimeField(description='date/time of the first attempt the user has made on the problem.')
    last_attempt_date = DateTimeField(description='date/time of the last attempt the user has made on the problem.')


class ProblemResponseTableMixin(TimestampPartitionMixin,
                                EventLogSelectionDownstreamMixin,
                                MapReduceJobTaskMixin):
    """
    Common parameters passed through the problem response workflow.
    """
    # Make the interval be optional:
    interval = luigi.DateIntervalParameter(
        default=None,
        description='The range of dates to export logs for. '
        'If not specified, `interval_start` and `interval_end` are used to construct the `interval`.',
    )

    # Define optional parameters, to be used if 'interval' is not defined.
    interval_start = luigi.DateParameter(
        config_path={'section': 'problem_response', 'name': 'interval_start'},
        default=datetime.date(2013, 5, 30),
        significant=False,
        description='The start date to export logs for.  Ignored if `interval` is provided.',
    )
    interval_end = luigi.DateParameter(
        default=datetime.datetime.utcnow(),
        significant=False,
        description='The end date to export logs for.  Ignored if `interval` is provided. '
        'Default is now, UTC.',
    )

    # Override this parameter so we can change the config_path and default value.
    partition_format = luigi.Parameter(
        config_path={'section': 'problem_response', 'name': 'partition_format'},
        default='%Y%m%d',
        description="Datetime format string for the table partition, which is applied to the configured course_id "
                    "and interval end parameters.  Must result in a filename-safe string, or your partitions will "
                    "fail to be created.  It results in a combined partition containing: \n"
                    "* {course_id}: a filename-safe version of the configured course_id\n"
                    "* datetime format string:  Adjust this portion to update the data more or less frequently.\n"
                    "  The default value of '%Y%m%d' changes daily, and so allows the data to update daily.\n"
                    "NB: Using time-based format strings with an interval string parameter (as opposed to setting "
                    "interval_start and interval_end) is not recommended, as the interval parsing logic can result "
                    "in an altered timestamp."

    )

    def __init__(self, *args, **kwargs):
        super(ProblemResponseTableMixin, self).__init__(*args, **kwargs)
        if not self.interval:
            self.interval = luigi.date_interval.Custom(self.interval_start, self.interval_end)
        # Use the end of the interval as the partition date
        self.date = self.interval.date_b


class ProblemResponseTableTask(ProblemResponseTableMixin, BareHiveTableTask):
    """
    A hive table containing the latest problem response data, partitioned on datetime.
    """
    @property
    def partition_by(self):
        return 'dt'

    @property
    def table(self):
        """Implement in subclass."""
        raise NotImplementedError

    @property
    def columns(self):
        return ProblemResponseRecord.get_hive_schema()

    @property
    def output_root(self):
        """Use the table location path for the output root."""
        return self.table_location

    def output(self):
        return get_target_from_url(self.output_root)


class LatestProblemResponseTableTask(ProblemResponseTableTask):
    """
    A hive table containing the latest problem response data, partitioned on a formatted datetime.
    """
    @property
    def table(self):
        return 'problem_response_latest'


class LatestProblemResponsePartitionTask(ProblemResponseTableMixin, HivePartitionTask):
    """The hive partition for this interval's problem response data."""

    @property
    def output_root(self):
        """Expose the partition location path as the output root."""
        return self.partition_location

    @property
    def hive_table_task(self):
        return LatestProblemResponseTableTask(
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite,
        )

    @property
    def data_task(self):
        return LatestProblemResponseDataTask(
            source=self.source,
            pattern=self.pattern,
            interval=self.interval,
            output_root=self.output_root,
            overwrite=self.overwrite,
            n_reduce_tasks=self.n_reduce_tasks,
        )


class ProblemResponseDataMixin(object):
    """
    Tasks that run the problem response data on the hadoop cluster need these extra modules.
    """
    def extra_modules(self):
        """
        Extra modules needed by the hadoop cluster when processing tracking logs.
        """
        import html5lib
        import six
        #FIXME JV DEV return [html5lib, six]
        return [html5lib, ]


class LatestProblemResponseDataTask(EventLogSelectionMixin,
                                    ProblemCheckEventMixin,
                                    ProblemResponseDataMixin,
                                    OverwriteOutputMixin,
                                    MapReduceJobTask):
    """
    Process the event log and gather the latest problem_check events.

    This emits one record for each answer in the latest problem_check event for each user,
    for each problem in a course.

    This task is intended to be run over all the tracking log data.
    """
    output_root = luigi.Parameter(
        description='URL pointing to the folder where the problem response data should be stored.',
    )
    clean_text_regex = luigi.Parameter(
        default=r'(?:<choicehint.*?</choicehint>)|(?:<choicehint.*?\>)',
        config_path={'section': 'problem-response', 'name': 'clean_text_regex'},
        description='Regex string used to strip unwanted HTML tags or other strings from text.',
    )

    # Write the output directly to the final destination and rely on the _SUCCESS file to
    # indicate whether or not it is complete. Note that this is a custom extension to luigi.
    enable_direct_output = True

    def __init__(self, *args, **kwargs):
        super(LatestProblemResponseDataTask, self).__init__(*args, **kwargs)
        if self.clean_text_regex is not None:
            self.clean_text_regex = re.compile(self.clean_text_regex)

    def mapper(self, line):
        """
        Generates output values for explicit problem_check events.

        Args:
            line: text line from a tracking event log.

        Yields:
            (course_id, problem_id, username), (timestamp, problem_check_info)

            See answer_dist.ProblemCheckEventMixin.mapper for details.

        Example:
                (DemoX-Demo_Course-T1, i4x://edX/DemoX/Demo_Course/problem/PS1_P1, dummy_username),
                (2013-09-10T00:01:05.123456, blah)
        """

        # Filter events on date interval
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, _date_string = value

        # Username is required
        username = event.get('username', '').strip()
        if not username:
            return

        # We are only interested in server events, not browser events.
        event_source = event.get('event_source')
        if event_source is None:
            log.error("encountered event with no event_source: %s", event)
            return
        if event_source != 'server':
            return

        # Parse the event as a problem_check event
        parsed_tuple_or_none = get_problem_check_event(event)
        if parsed_tuple_or_none is not None:
            yield parsed_tuple_or_none

    def reducer(self, key, values):
        """
        Calculate a ProblemResponseRecord from the most recently submitted
        response to a problem in a course.

        If the problem response contains multiple "submissions"
        (i.e. multiple questions), they will be split into separate
        ProblemResponseRecords.

        Args:
            key:  (course_id, problem_id, username)
            values:  iterator of (attempt_date, problem_check_json)

            See ProblemCheckEventMixin.mapper for details.

        Yields:
            A key/value tuple for each of the latest problem attempt
            "submissions", annotated with the aggregated total_attempts,
            first_attempt_date, and last_attempt_date.

            ((course_id, answer_id),
             (problem_id, problem, username, question, score, max_score, correct, answer,
              total_attempts, first_attempt_date, last_attempt_date))
        """
        # Parse the map key
        (course_id, problem_id, username) = key

        # Sort input values (by timestamp) to easily detect the first
        # and most recent answer to a problem by a particular user.
        # Note that this assumes the timestamp values (strings) are in
        # ISO representation, so that the tuples will be ordered in
        # ascending time value.
        values = sorted(values)
        if not values:
            return

        # Get the first entry.
        first_attempt_date, _first_response = values[0]

        # Get the last entry
        last_attempt_date, latest_response = values[-1]

        # Get attempt count
        total_attempts = len(values)

        # Generate a single response record from each answer submission
        for answer in self.get_answer_data(latest_response):
            latest_response_record = ProblemResponseRecord(
                course_id=course_id,
                answer_id=answer.get('answer_id'),
                problem_id=problem_id,
                problem=answer.get('problem', ''),
                location=answer.get('location', ''),
                username=username,
                question=answer.get('question', ''),
                score=answer.get('grade', 0),
                max_score=answer.get('max_grade', 0),
                correct=answer.get('correct', None),
                answer=answer.get('answer', ''),
                total_attempts=total_attempts,
                first_attempt_date=first_attempt_date,
                last_attempt_date=last_attempt_date
            )

            yield latest_response_record.to_string_tuple()

    def _clean_string(self, string):
        """Remove unwanted characters from the given string or list of strings."""

        # Handle lists of strings
        if isinstance(string, (list, tuple)):
            for idx, substring in enumerate(string):
                string[idx] = self._clean_string(substring)
            return string

        # Replace multiple whitespaces (including newlines) with a single space
        string = re.sub(r'\s+', r' ', string)

        # Remove any unwanted tags
        if self.clean_text_regex is not None:
            string = self.clean_text_regex.sub(r'', string)

        return string

    def get_answer_data(self, event_string):
        """
        Concatenate the answer text and "correctness" information for all
        the answers parsed from the given event_string.

        Args:
            event_string: a JSON-encoded string version of the event's data

        Yields:
            A record for each submitted answer, containing:

            * course_id: identifier for the course containing the question
            * problem: display text for the problem
            * question: display text for the question
            * grade: score given for the attempt
            * max_grade: maximum score given for the attempt
            * correct: A nullable, boolean representation of the "correctness" value:
                * True if "correct"
                * False if "incorrect"
                * None if unspecified or "unknown"
            * answer: concatenated string of answer values, with <choicehint> etc removed.
        """
        # Process each submitted answer for the current problem
        for answer in self._generate_answers(event_string, 'unused'):
            (course_id, answer_id), (_timestamp, answer_json) = answer
            answer_data = json.loads(answer_json)

            problem = answer_data.get('problem_display_name', '')
            question = answer_data.get('question', '')

            # Answers can be correct, incorrect, or unknown
            correct_map = answer_data.get('answer_correct_map', {})
            correctness = correct_map.get('correctness')
            if correctness == 'correct':
                correct = True
            elif correctness == 'incorrect':
                correct = False
            else:
                correct = None

            # Answer text may have been given, or maybe just the answer IDs
            answer = answer_data.get('answer', answer_data.get('answer_value_id', ''))

            # Yield each processed submission
            yield dict(
                course_id=course_id,
                answer_id=answer_id,
                problem=self._clean_string(problem),
                question=self._clean_string(question),
                grade=answer_data.get('grade'),
                max_grade=answer_data.get('max_grade'),
                correct=correct,
                answer=self._clean_string(answer),
            )

    def output(self):
        return get_target_from_url(self.output_root)

    def complete(self):
        """
        The current task is complete if no overwrite was requested,
        and the output_root/_SUCCESS file is present.
        """
        if super(LatestProblemResponseDataTask, self).complete():
            return get_target_from_url(url_path_join(self.output_root, '_SUCCESS')).exists()
        return False

    def run(self):
        """
        Clear out output if data is incomplete, or if overwrite requested.
        """
        if not self.complete():
            self.remove_output_on_overwrite()

        super(LatestProblemResponseDataTask, self).run()


class ProblemResponseLocationTableTask(ProblemResponseTableTask):
    """
    A hive table containing the latest problem response data, sorted by and joined with the course blocks location,
    partitioned on a formatted datetime.
    """
    @property
    def table(self):
        return 'problem_response_location'


class ProblemResponseLocationPartitionTask(ProblemResponseTableMixin, HivePartitionTask):
    """
    Partition which joins the LatestProblemResponse data with the Course Blocks course_path,
    and sorts on the block.sort_idx.

    Partitioned on course_id and formatted date.
    """

    @property
    def output_root(self):
        return self.partition_location

    @property
    def hive_table_task(self):
        return ProblemResponseLocationTableTask(
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite,
        )

    @property
    def data_task(self):
        return ProblemResponseLocationDataTask(
            interval=self.interval,
            interval_start=self.interval_start,
            interval_end=self.interval_end,
            source=self.source,
            pattern=self.pattern,
            output_root=self.output_root,
            overwrite=self.overwrite,
            n_reduce_tasks=self.n_reduce_tasks,
        )


class ProblemResponseLocationDataTask(ProblemResponseTableMixin, OverwriteOutputMixin, MapReduceJobTask):
    """
    Merges the data from the ProblemResponse and Course Blocks partitions into a single data set,
    sorted on course_blocks.sort_idx.
    """

    """ FIXME
    input_root = luigi.Parameter(
        description='URL pointing to the folder from where the problem response data should be read.',
    )
    """
    output_root = luigi.Parameter(
        description='URL pointing to the folder where the problem response data should be stored.',
    )

    enable_direct_output = True

    def requires_hadoop(self):
        kwargs = dict(
            mapreduce_engine=self.mapreduce_engine,
            lib_jar=self.lib_jar,
            n_reduce_tasks=self.n_reduce_tasks,
            remote_log_level=self.remote_log_level,
            input_format=self.input_format,
            overwrite=self.overwrite,
        )

        problem_responses = LatestProblemResponsePartitionTask(
            interval=self.interval,
            interval_start=self.interval_start,
            interval_end=self.interval_end,
            source=self.source,
            pattern=self.pattern,
            **kwargs
        )

        self.date = problem_responses.date

        course_blocks = LoadCourseBlocksTask(
            date=self.date,
            #FIXME input_root=self.input_root,
            input_root=problem_responses.output_root,
            **kwargs
        )

        yield (
            course_blocks,
            problem_responses,
        )

    def input_hadoop(self):
        return get_target_from_url(self.input_root)

    def mapper(self, line):
        """
        Tags the given input line as either a problem_response record or a course_block record.
        """
        if line is not None:
            content = line.split('\t')
            if len(content) > 1:
                record = None
                block_id = None

                # Try loading the line as a CourseBlockRecord
                try:
                    record = CourseBlockRecord.from_string_tuple(content)
                    block_id = record.block_id

                except Exception as e:
                    log.error(e)

                    # If that fails, try loading as a ProblemResponseRecord
                    try:
                        record = ProblemResponseRecord.from_string_tuple(content)
                        block_id = record.problem_id

                    except Exception as e:
                        log.error(e)
                finally:
                    if record is not None:
                        yield ((record.course_id, block_id), record)

    def reducer(self, _key, values):
        """
        Merge the CourseBlockRecord.course_path field as the ProblemResponseRecord.location,
        and sort on CourseBlockRecord.sort_idx.
        """
        if not values:
            return

        problem_responses = []
        course_block = None

        for record in values:
            if isinstance(record, ProblemResponseRecord):
                problem_responses.append(record)
            elif instance(record, CourseBlockRecord):
                # Should only be one
                course_block = record

        # Output a tuple for each problem response
        for response in problem_responses:
            # Include course block path if found
            if course_block:
                response.location = course_block.course_path
            yield response.to_string_tuple()

    def output(self):
        return get_target_from_url(self.output_root)

    def complete(self):
        """
        The current task is complete if no overwrite was requested,
        and the output_root/_SUCCESS file is present.
        """
        if super(ProblemResponseLocationDataTask, self).complete():
            return get_target_from_url(url_path_join(self.output_root, '_SUCCESS')).exists()
        return False

    def run(self):
        """
        Clear out output if data is incomplete, or if overwrite requested.
        """
        if not self.complete():
            self.remove_output_on_overwrite()

        super(ProblemResponseLocationDataTask, self).run()



class ProblemResponseReportTask(ProblemResponseDataMixin,
                                MultiOutputMapReduceJobTask):
    """
    Task which generates one report per course from the input problem response records.

    ProblemResponseRecords are mapped by course_id, and each course is written to a separate file.
    """
    input_root = luigi.Parameter(
        description="URL pointing to the folder of problem response records to include in the reports.",
    )
    report_filename_template = luigi.Parameter(
        config_path={'section': 'problem-response', 'name': 'report_filename_template'},
        default='{course_id}_problem_response.csv',
        description="Template for the report filename to be created under output_root.\n"
                    "  Template may use this variable, e.g.,\n"
                    "  * course_id: course key/identifier.  \n"
                    "    To make the course_id filename-safe, we replace with '_' everything "
                    "    that isn't an alphanumeric, underscore, period, or hyphen.\n",
    )
    report_fields = luigi.Parameter(
        default=None,
        config_path={'section': 'problem-response', 'name': 'report_fields'},
        description='JSON string containing a list of ProblemResponseRecord fields to include '
                    'in the report, and the order they should appear in. '
                    'If null, the full list of fields will be used, in record field order',
    )
    report_field_datetime_format = luigi.Parameter(
        default=None,
        config_path={'section': 'problem-response', 'name': 'report_field_datetime_format'},
        description='Format string to use for datetime fields in the CSV file.'
                    ' See strftime() for details.'
    )
    report_field_list_delimiter = luigi.Parameter(
        default=None,
        config_path={'section': 'problem-response', 'name': 'report_field_list_delimiter'},
        description='Delimiter string to use to join list fields in the CSV file. '
                    'Will be evaluated as a literal python string so configure using a quoted'
                    ' string, e.g. report_field_list_delimiter = "\\n" will output a field'
                    "containing the list ['a','b', 'c'] as: \na\nb\nc\n"
                    'If null, this field would simply be stringified, and output as: '
                    "\n[u'a', u'b', u'c'].",
    )

    def __init__(self, *args, **kwargs):
        super(ProblemResponseReportTask, self).__init__(*args, **kwargs)
        self.record_fields = ProblemResponseRecord.get_fields().keys()
        if self.report_fields is None:
            self.report_fields = self.record_fields
        else:
            self.report_fields = json.loads(self.report_fields)

        # Support raw strings in report_field_list_delimiter
        if self.report_field_list_delimiter is not None:
            self.report_field_list_delimiter = ast.literal_eval(self.report_field_list_delimiter)

    def input_hadoop(self):
        return get_target_from_url(self.input_root)

    def output(self):
        """
        Use the marker location as an indicator of task "completeness".
        """
        return get_target_from_url(self.marker)

    def output_path_for_key(self, course_id):
        """
        Match the course folder hierarchy that is expected by the Analytics API.

        The Analytics API expects the problem response files to be stored in a
        folder named by the course_id, so we sanitize it to create the filename.
        """

        if course_id:
            safe_course_id = get_filename_safe_course_id(course_id)
            filename = self.report_filename_template.format(course_id=safe_course_id)
            return url_path_join(self.output_root, filename)
        return None

    def mapper(self, line):
        """
        Splits the course_id (aka grouping key) out of the problem response line.

        Args: tab-delimited problem response values, with course_id first.

        Yields: the course_id, and a full tuple for the record:
            course_id, (course_id, answer_id, problem_id, ...)
        """

        if line is not None:
            content = line.split('\t')
            if len(content) > 1:
                yield content[0], tuple(content)

    def multi_output_reducer(self, _course_id, values, output_file):
        """
        Each entry should be written to the output file in csv format.

        This output is visible to instructors, so use an excel friendly format (csv).
        """
        # Write the CSV header
        writer = csv.DictWriter(output_file, self.report_fields)
        writer.writeheader()

        for record_values in values:
            # Decode the record from the tuple
            record = ProblemResponseRecord.from_string_tuple(record_values)

            # Write the CSV row
            row = self._record_to_string_dict(record)
            writer.writerow(row)

    def _record_to_string_dict(self, record):
        """Map the requested report field names to utf-8 encoded strings."""
        row = {}
        for field_name in self.report_fields:
            value = getattr(record, field_name, None)

            # Format datetime fields if configured
            if isinstance(value, datetime.datetime):
                if self.report_field_datetime_format is not None:
                    value = value.strftime(self.report_field_datetime_format)

            # Flatten list fields if configured
            elif isinstance(value, list):
                if self.report_field_list_delimiter is not None:
                    value = self.report_field_list_delimiter.join(value)

            encoded_value = unicode(value).encode('utf8')
            row[field_name] = encoded_value

        return row


@workflow_entry_point
class ProblemResponseReportWorkflow(ProblemResponseTableMixin,
                                    luigi.WrapperTask):
    """
    Workflow task that generates the problem response reports from the hive table.
    """
    output_root = luigi.Parameter(
        config_path={'section': 'problem-response', 'name': 'report_output_root'},
        description='Location where the report files will be stored.',
    )
    marker = luigi.Parameter(
        significant=False,
        description='URL directory where a marker file will be written on task completion.'
                    ' Note that the report task will not run if this marker file exists.',
    )
    hive_overwrite = luigi.BooleanParameter(
        default=False,
        description='Whether or not to rebuild hive data from tracking logs.'
    )

    def requires(self):
        """
        Initialize and yield the tasks in this workflow.
        """
        # Args shared by all tasks
        kwargs = dict(
            mapreduce_engine=self.mapreduce_engine,
            lib_jar=self.lib_jar,
            n_reduce_tasks=self.n_reduce_tasks,
            remote_log_level=self.remote_log_level,
            input_format=self.input_format,
        )

        # Initialize problem response table task
        problem_responses_task = LatestProblemResponsePartitionTask(
            interval=self.interval,
            interval_start=self.interval_start,
            interval_end=self.interval_end,
            source=self.source,
            pattern=self.pattern,
            **kwargs
        )

        # Initialize problem response + course location partition task
        problem_response_location_task = ProblemResponseLocationPartitionTask(
            interval=self.interval,
            interval_start=self.interval_start,
            interval_end=self.interval_end,
            source=self.source,
            pattern=self.pattern,
            overwrite=self.hive_overwrite,
            **kwargs
        )

        # Initialize report task
        # NB: its input_root is the problem_response_location_task's output_root
        report_task = ProblemResponseReportTask(
            input_root=problem_response_location_task.output_root,
            output_root=self.output_root,
            marker=self.marker,
            **kwargs
        )

        # Order is important here, and unintuitive: dependencies are yielded in reverse order.
        yield(
            report_task,
            problem_response_location_task,
            problem_responses_task,
        )