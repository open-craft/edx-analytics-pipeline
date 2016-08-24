"""
Task which uses the edx-rest-api-client to fetch and cache data from the edX REST API.
"""

import datetime
import logging
import json
import luigi
import requests

from edx.analytics.tasks.pathutil import PathSetTask
from edx.analytics.tasks.url import get_target_from_url, url_path_join, UncheckedExternalURL


log = logging.getLogger(__name__)


class EdxRestApiTaskException(Exception):
    """Exception class to make it easy to catch exceptions from the EdxRestApiTask."""

    def __init__(self, exception=None, *args, **kwargs):
        super(EdxRestApiTaskException, self).__init__(*args, **kwargs)
        # Store the original exception instance, if provided
        self.exception = exception


class EdxRestApiTask(PathSetTask):
    """
    Creates an authenticated connection to the edX REST API, and stores the response to a cache.

    Cache files are named using a hash of the task parameters, including the `date` field.  As the task parameters
    change, the cache is invalidated, and so a new call will be made to the REST API.  Typical usage will be to set the
    `date` parameter to the current date, so the cache data is updated daily.  However, note that the `date` field will
    accept a date or datetime instance, and so data may be refreshed more frequently if desired.  But beware of load on
    the REST API.

    Authenticates using `client_id` and `client_secret` (eucalptus+), or additionally using `oauth_username` and
    `oauth_password` (cypress/dogwood) if configured.

    To authenticate to the REST API this task POSTs credentials to the authorization URL, which is concatenated from the
    `base_url` and `auth_url' parameters, and retrieves a temporary `access_token`.

    OAuth2 clients can be created/updated using, e.g.,

      ./manage.py lms --settings=devstack create_oauth2_client  \
            http://localhost:9999  # URL doesn't matter \
            http://localhost:9999/complete/edx-oidc/  \
            confidential \
            --client_name "Analytics Pipeline" \
            --client_id oauth_id \
            --client_secret oauth_secret \
            --trusted


    Once the client is authenticated, then the REST API call is made.  The REST API URL is concatenated from the
    `base_url`, `base_path`, and `resource` parameters.  Query string arguments are passed from the `arguments`
    parameter.

    If the REST API data is paginated, and you wish to fetch all the pages, set the `pagination_key` parameter to the
    name of the field containing the pagination data.  The task will examine this data to determine if further pages can
    be fetched and cached.

    Once the REST API call has completed successfully, the task writes a `manifest` file named with a hash of the task's
    parameters, and containing the list of files cached for the call.  The REST API will not be contacted if the
    `manifest` file for those task parameters exists.  You can either change the task's parameters, or remove the
    `manifest` file to force a re-fetch of the data.
    """
    date = luigi.DateParameter(
        default=datetime.datetime.utcnow().date(),
        description='Date/time used to mark the cached API files.  Default is UTC today.'
                    'Take care when overriding this parameter, as every time this task is called with a different '
                    'date/time, it will hit the configured API endpoint.',
    )
    client_id = luigi.Parameter(
        config_path={'section': 'edx-rest-api', 'name': 'client_id'},
        description='OAuth client ID authorized to query the edX REST API.',
    )
    client_secret = luigi.Parameter(
        config_path={'section': 'edx-rest-api', 'name': 'client_secret'},
        description='OAuth secret, used with the client ID, to authenticate to the edX REST API.',
    )
    oauth_username = luigi.Parameter(
        config_path={'section': 'edx-rest-api', 'name': 'oauth_username'},
        default=None,
        description='OAuth username authorized to query the edX REST API (usually requires staff access).'
                    'Required when contacting edx-platform releases prior to eucalpytus.',
    )
    oauth_password = luigi.Parameter(
        config_path={'section': 'edx-rest-api', 'name': 'oauth_password'},
        default=None,
        description='OAuth password, used with the oauth_username, to authenticate to the edX REST API. '
                    'Required when contacting edx-platform releases prior to eucalpytus.',
    )
    base_url = luigi.Parameter(
        config_path={'section': 'edx-rest-api', 'name': 'base_url'},
        description='Base URL for the Course Blocks API, e.g. http://localhost:8000\n'
                    'The full API URL will be joined from {base_url}, {base_path}, {resource}.'
    )
    auth_path = luigi.Parameter(
        config_path={'section': 'edx-rest-api', 'name': 'auth_path'},
        default='/oauth2/access_token/',
        description='Appended to the base_url to get the OAuth2 access token.',
    )
    base_path = luigi.Parameter(
        config_path={'section': 'edx-rest-api', 'name': 'base_path'},
        default='/api/courses/v1/',
        description='Appended to the base_url to get the full REST API path.'
    )
    cache_root = luigi.Parameter(
        config_path={'section': 'edx-rest-api', 'name': 'cache_root'},
        description='Root location to store edX REST API output.  Output files are named using a hash of the '
                    'significant parameters for this task.',
    )
    timeout = luigi.Parameter(
        default=60,
        config_path={'section': 'edx-rest-api', 'name': 'timeout'},
        description='Number of seconds to wait before timing out from a REST API call.'
    )
    raise_exceptions = luigi.BooleanParameter(
        default=True,
        significant=False,
        config_path={'section': 'edx-rest-api', 'name': 'raise_exceptions'},
        description='If True, then re-raise 404 and 400 exceptions raised by REST API.'
                    'If False, then catch and log them, but do not re-raise.'
    )
    pagination_key = luigi.Parameter(
        config_path={'section': 'edx-rest-api', 'name': 'pagination_key'},
        default='pagination',
        description='Set to the name of the pagination element expected in the data returned from the API call. '
                    'If an element with this key is found, then the task will attempt to follow next links, and '
                    'multiple output files will be generated. If null, then no pagination links will be followed.'
    )
    resource = luigi.Parameter(
        description='Name of the edX REST API resource to get, e.g. "courses" or "blocks". '
                    'This name is appended to the base_path when contacting the REST API. '
                    'For options, see the specific section for your API resource: '
                    'http://edx.readthedocs.io/projects/edx-platform-api/'
    )
    arguments = luigi.Parameter(
        default={},
        description='Dictionary containing the arguments passed to the REST API call.'
                    'For options, see the specific section for your API resource: '
                    'http://edx.readthedocs.io/projects/edx-platform-api/'
    )
    extend_response = luigi.Parameter(
        default=None,
        description='Dictionary containing any data that should be added to the REST API call before storing, e.g. '
                    '/courses/v1/blocks API response doesn\'t contain the course_id, which is useful for parsing the '
                    'response cache later.'
    )

    # Override superclass to disable these parameters
    src = None
    include = None
    include_zero_length = None

    def __init__(self, *args, **kwargs):
        super(EdxRestApiTask, self).__init__(*args, **kwargs)

        # If oauth_username is set, then oauth_password must be too.
        assert self.oauth_password is not None if self.oauth_username else True, (
            "oauth_username requires oauth_password to be set.")

        self.url = url_path_join(self.base_url, self.base_path)
        self.auth_url = url_path_join(self.base_url, self.auth_path)
        self.manifest, self.manifest_target = self._get_cache_target(suffix='.manifest')

        log.debug('self.manifest: %s', self.manifest)
        log.debug('self.manifest_target: %s', self.manifest_target)

        if isinstance(self.arguments, basestring):
            self.arguments = json.loads(self.arguments)

    def requires(self):
        """
        Fetch or create the manifest file containing any cached files for this set of task parameters.

        If the manifest file, read it to determine the cache files to return.
        If the manifest file does not exist, it will be created, and the REST API files cached.
        """
        requirements = []
        for requirement in super(EdxRestApiTask, self).requires():

            # If we have a manifest file, read it and return its contents as our requirements.
            # (the parent class PathSetTask will just return the manifest file itself.)
            if self.manifest_target.exists():
                log.debug("reading manifest %s", self.manifest)
                with self.manifest_target.open('r') as manifest_file:
                    for external_url in manifest_file.readlines():
                        log.debug("adding requirement %s", external_url)
                        requirements.append(UncheckedExternalURL(external_url.strip()))

            # There should always be a manifest file, but just in case,
            # push the superclass's requirement onto a list, and return them as-is.
            else:
                requirements.append(requirement)

        return requirements

    def complete(self):
        """
        The current task is complete if the manifest file exists.
        """
        return super(EdxRestApiTask, self).complete() and self.manifest_target.exists()

    def generate_file_list(self):
        """
        Generate the manifest file list by locating the cache files, if present, or creating them from REST API
        responses.

        Will read and return all pages of data if configured to do so.
        """
        client = None
        expires_at = 0
        using_cache = False
        arguments = self.arguments.copy()
        page = arguments.get('page', 1)

        output_url, output_target = self._get_cache_target(page=page)
        log.info('preparing target %s', output_url)
        while output_target is not None:

            get_next_page = False

            # Use cache file if found
            if output_target.exists():
                using_cache = True
                log.info('using cached file %s', output_url)
                yield UncheckedExternalURL(output_url)

                # Get the next cached page if we're paginating
                get_next_page = (self.pagination_key is not None)

            # Don't fetch more pages if they're not already cached
            elif using_cache:
                output_target = None

            # Create the cache file from the API response
            else:
                # Create an authenticated API client, if none has been created,
                # or if the existing client is expired.
                now = datetime.datetime.utcnow()
                if client is None or expires_at <= now:
                    client, expires_at = self.get_client()

                # Get the API resource
                api_call = getattr(client, self.resource)
                arguments['page'] = page
                try:
                    response = api_call.get(**arguments)
                except Exception as exc:  # pylint: disable=broad-except
                    message = 'Error fetching API resource {}: {}'.format(arguments, exc)
                    log.error(message)

                    # If configured to raise exceptions, raise one.
                    if self.raise_exceptions:
                        raise EdxRestApiTaskException(exc, message)
                    else:
                        # Write an file placeholder, containing the exception message.
                        response = dict(exception=message)

                # Add in the arguments passed to the API, in case there's critical information in there.
                # E.g. /courses/v1/blocks response don't contain the course_id, which is necessary for parsing later.
                if self.extend_response is not None:
                    response.update(self.extend_response)

                # Serialize response object to a JSON string, and write to file
                log.debug("writing cache file %s", output_url)
                with output_target.open('w') as output_file:
                    output_file.write("{output}\n".format(output=json.dumps(response)))

                # Yield the fully qualified output_url target
                yield UncheckedExternalURL(output_url)

                # If there are more pages, follow on to the the next page
                if isinstance(response, dict):
                    get_next_page = (response.get(self.pagination_key, {}).get('next') is not None)

            if get_next_page:
                page += 1
                log.debug("fetching page %s", page)
                output_url, output_target = self._get_cache_target(page=page)

            else:
                # Break out of the loop
                output_target = None

    def _get_cache_target(self, page=0, suffix='.json'):
        """
        Returns a url and target file located in the cache_root.

        The file is named from a hash of this task's significant parameters, the given page number, and suffix.
        """
        filename = '{task_id}-{page}{suffix}'.format(task_id=str(hash(self)), page=page, suffix=suffix)
        url = url_path_join(self.cache_root, filename)
        return url, get_target_from_url(url)

    def get_client(self):
        """
        Create a new authenticated EdxRestApiClient instance by creating a new access token from the configured
        credentials.

        To authenticate to Eucalpytus+ releases of edx-platform, only these parameters need to be configured:
        * `client_id`
        * `client_secret`

        To authenticate to Dogwood releases of edx-platform, the parameters listed above must be configured, plus these:
        * `oauth_username`
        * `oauth_password`

        Returns a tuple containing the client instance, and the expires_at datetime.
        """
        # Import EdxRestApiClient here so the EMR nodes don't need the edx_rest_api module, or its dependencies
        from edx_rest_api_client.client import EdxRestApiClient

        # Default expires_at to one day from now
        try:
            if self.oauth_username and self.oauth_password:
                access_token, expires_at = self.get_oauth_access_token(
                    url=self.auth_url, client_id=self.client_id, client_secret=self.client_secret,
                    oauth_username=self.oauth_username, oauth_password=self.oauth_password,
                )
            else:
                access_token, expires_at = EdxRestApiClient.get_oauth_access_token(
                    url=self.auth_url, client_id=self.client_id, client_secret=self.client_secret,
                )

        except requests.RequestException as exc:
            raise EdxRestApiTaskException(exc, 'Invalid client_id or client_secret: {}. '
                                               'Please check your configuration.'.format(exc))
        except ValueError as exc:
            raise EdxRestApiTaskException(exc, 'Invalid auth_url {}: {}. '
                                               'Please check your configuration.'.format(self.auth_url, exc))
        client = EdxRestApiClient(
            self.url,
            oauth_access_token=access_token,
            timeout=self.timeout
        )
        return client, expires_at

    @staticmethod
    def get_oauth_access_token(url, client_id, client_secret, oauth_username, oauth_password):
        """
        Uses the configured client_id/client_secret + oauth_username/oauth_password to get an access token.
        This approach is required for edx-platform dogwood release.
        """
        now = datetime.datetime.utcnow()

        response = requests.post(
            url,
            data={
                'grant_type': 'password',
                'token_type': 'bearer',
                'client_id': client_id,
                'client_secret': client_secret,
                'username': oauth_username,
                'password': oauth_password,
            }
        )

        data = response.json()

        try:
            access_token = data['access_token']
            expires_in = data['expires_in']
        except KeyError:
            raise requests.RequestException(response=response)

        expires_at = now + datetime.timedelta(seconds=expires_in)

        return access_token, expires_at
