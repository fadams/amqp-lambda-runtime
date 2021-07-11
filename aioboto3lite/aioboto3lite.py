#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
"""
aioboto3 compatibility wrapper for aiohttp and aiosonic.

N.B. This is currently very much "proof of concept"/"prototype" software YMMV.

The premise of this library is that whilst botocore/boto3/aiobotocore/aioboto3
are incredibly useful and powerful SDKs a key design goal of those is to provide
a very general SDK that can be dynamically generated from a JSON model, and so
can support new AWS services almost as soon as they are available. The down-side
of that design goal is that in order to be general there are often many layers
of indirection and delegation between the SDK API methods like s3.get_object to
the underlying HTTP REST API call.

The aim of this library is to illustrate the performance difference that taking
a more "direct" route from the SDK method to API invocation might make. It also
aims to illustrate how different HTTP client libraries might make a difference.
With aiobotocore we only have the option of using aiohttp so with this library
we also include support for aiosonic.

In general the performance difference can be profound. Usin aiosonic put_object
appears to have more than 2.5x the throughput of aioboto3 and get_object appears
to have more than 4.5x the throughput.

In addition to increased performance this library has a tiny footprint when
compared with aioboto3 (and all of its dependencies)

The *significant* down-side of this approach, however, is "sustainability". That
is to say this proof of concept only supports a fairly limited set of S3 CRUD
operations at the moment and although it is relatively straightforward to add
new methods, and it would be possible to make things more generic and even
JSON model driven there's a danger that that could make things evolve into
a slightly "dodgy" boto clone.


Whilst the underlying premise of improving performance by optimising the path
from SDK method to API call is sound ultimately perhaps only a handful of
methods actually benefit from such an optimisation. Many SDK methods are used
for setup/teardown and relatively few (like s3.get_object/s3.put_oject/
sfn.start_execution/lambda.invoke etc.) are actually used on application
"critical path". So perhaps a better (more sustainable) approach might be to
start with aioboto3/aiobotocore and "inject" optimised SDK methods to replace
the handful that would really benefit from optimisation.
"""

import sys
assert sys.version_info >= (3, 8) # Bomb out if not running Python3.8

import asyncio, io, os, time, uuid

import aiohttp, aiosonic
import hashlib, hmac, re, urllib.parse
from datetime import datetime, timezone, timedelta

"""
Attempt to use ujson if available https://pypi.org/project/ujson/
"""
try:
    import ujson as json
except:  # Fall back to standard library json
    import json

"""
Attempt to use pybase64 libbase64 based codec if available
pip3 install pybase64
https://github.com/mayeut/pybase64
https://github.com/aklomp/base64
"""
try:
    import pybase64 as base64
except:  # Fall back to standard library base64
    import base64


to_pascal_case_pattern = re.compile(r"(?:^|_|-)(.)")
def to_pascal_case(string):
    # Convert from snake (or hyphen) case to upper camel (e.g. Pascal) case
    return to_pascal_case_pattern.sub(lambda m: m.group(1).upper(), string)

to_hyphen_case_pattern1 = re.compile(r"([A-Z]+)([A-Z][a-z])")
to_hyphen_case_pattern2 = re.compile(r"([a-z\d])([A-Z])")
def to_hyphen_case(string):
    # Convert from camel/PascalCase to hyphen-case
    string = to_hyphen_case_pattern1.sub(r'\1-\2', string)
    string = to_hyphen_case_pattern2.sub(r'\1-\2', string)
    return string.lower()

def parse_rfc3339_datetime(rfc3339):
    """
    Parse an RFC3339 (https://www.ietf.org/rfc/rfc3339.txt) format string into
    a datetime object which is essentially the inverse operation to
    datetime.now(timezone.utc).astimezone().isoformat()
    We primarily need this in the Wait state so we can compute timeouts etc.
    """
    rfc3339 = rfc3339.strip()  # Remove any leading/trailing whitespace
    if rfc3339[-1] == "Z":
        date = rfc3339[:-1]
        offset = "+00:00"
    else:
        date = rfc3339[:-6]
        offset = rfc3339[-6:]

    if "." not in date:
        date = date + ".0"
    raw_datetime = datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%f")
    delta = timedelta(hours=int(offset[-5:-3]), minutes=int(offset[-2]))
    if offset[0] == "-":
        delta = -delta
    return raw_datetime.replace(tzinfo=timezone(delta))

def XML_iterator(buffer):
    """
    This is a fast streaming XML parser that operates on binary sequence types.
    https://docs.python.org/3/library/stdtypes.html#binary-sequence-types-bytes-bytearray-memoryview
    it iterates over a memoryview, implementing a simple XML state machine
    triggered on the <!?/ > characters and yields a tuple of the form:
    tag, attributes, value, end
    The first three are the tag, attributes and value of the XML element and
    end is a boolean that is True if the tuple signifies the tag end else False.
    Using a memoryview avoids unnecessary copying and the tag, attributes, value
    string are constructed from memoryview slices, again to minimise copying.
    """
    INIT = 1
    SEEN_LT = 2                  # <
    SEEN_START_TAG = 3           # <tag>
    SEEN_END_TAG = 4             # </tag>
    SEEN_POSSIBLE_EMPTY_TAG = 5  # <tag/>
    SEEN_GT = 6                  # >
    COLLECTING_VALUE = 7

    state = INIT
    tag_start_idx = tag_end_idx = 0  # Indices to start & end of tag name.
    attributes_start_idx = attributes_end_idx = 0  # start & end of attribute.
    value_start_idx = value_end_idx = 0  # Indices to start & end of tag value.

    tag_stack = []  # Used to get tag names for end tags

    """
    Use memoryview to efficiently iterate over bytes in memory.
    """
    memview = memoryview(buffer)
    for i, ch in enumerate(memview):
        if state == INIT:
            if ch == ord("<"):
                state = SEEN_LT
        elif state == COLLECTING_VALUE:
            if ch == ord("<"):
                state = SEEN_LT;
                tag = bytes(memview[tag_start_idx:tag_end_idx + 1]).decode("utf-8")
                tag_stack.append(tag)
                attributes = bytes(memview[attributes_start_idx:attributes_end_idx + 1]).decode("utf-8")
                value = bytes(memview[value_start_idx:value_end_idx + 1]).decode("utf-8")
                yield tag, value, attributes, False
            else:
                value_end_idx = i
        elif state == SEEN_LT:
            if ch == ord("!") or ch == ord("?"):
                state = INIT
            elif ch == ord("/"):
                state = SEEN_END_TAG
            else:
                state = SEEN_START_TAG
                tag_start_idx = i
                attributes_start_idx = tag_start_idx
        elif state == SEEN_START_TAG:
            if ch == ord(">"):
                state = COLLECTING_VALUE
                value_start_idx = i + 1
            elif ch == ord("/"):
                state = SEEN_POSSIBLE_EMPTY_TAG
            elif ch == ord(" "):
                if attributes_start_idx > tag_start_idx:
                    attributes_end_idx = i
                else:
                    attributes_start_idx = i + 1
            else:
                if attributes_start_idx > tag_start_idx:
                    attributes_end_idx = i
                else:
                    tag_end_idx = i
        elif state == SEEN_END_TAG:
            if ch == ord(">"):
                state = INIT;
                yield tag_stack.pop(), "", "", True
        elif state == SEEN_POSSIBLE_EMPTY_TAG:
            if ch == ord(">"):  # It actually is an empty tag
                state = INIT
                tag = bytes(memview[tag_start_idx:tag_end_idx + 1]).decode("utf-8")
                attributes = bytes(memview[attributes_start_idx:attributes_end_idx + 1]).decode("utf-8")
                yield tag, "", attributes, True
            else:  # It's not an empty tag, just a / character in an attribute
                state = SEEN_START_TAG
                if attributes_start_idx > tag_start_idx:
                    attributes_end_idx = i

def deserialise_XML(buffer, template):
    """
    Used to transform raw XML API responses of the form described in the AWS docs
    https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
    into response dicts of the form described in the boto3 docs.
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.list_objects_v2

    This function operates on a buffer of binary sequence types.
    https://docs.python.org/3/library/stdtypes.html#binary-sequence-types-bytes-bytearray-memoryview
    it uses the XML_iterator to iterate over a memoryview, yielding a sequence
    of tag, value, attribute, end tuples, where end == True signifies an
    end/closing tag. The structure of the source XML document is used to create
    a dict, with literal values defaulting to strings and nested structures
    defaulting to nested dicts.

    To fully deserialise, a "type overrides" template is used of the form:

    template = {
        "int": {"/Contents/Size", "/KeyCount", "/MaxKeys"},
        "bool": {"/IsTruncated"},
        "object_list": {"/Contents", "/CommonPrefixes"},
        "datetime": {"/Contents/LastModified"},
    }

    This allows default str/dict for specified paths to be overridden. The int,
    float, bool, datetime overrides are fairly obvious whilst object_list may be
    used to transform repeating tags like <Contents>...</Contents><Contents>...</Contents>
    into e.g. "Contents": [{...}, {...}]
    """

    numeric_int = template.get("int", {})
    numeric_float = template.get("float", {})
    boolean = template.get("bool", {})
    object_list = template.get("object_list", {})
    time_datetime = template.get("datetime", {})

    tag_stack = [("root", "", "", False)]
    for tags in XML_iterator(buffer):
        tag, value, attribute, end = tags
        if end:  # Is the tag an end/closing tag?
            # Get actual tag/value off the stack. Slice gets first two items in tuple
            tag, value = tag_stack.pop()[:2]
            # After getting current tag the top of stack now holds the "container".
            container_name, container = tag_stack[-1][:2]
            if container == "":
                container = {}
                tag_stack[-1] = (container_name, container, "", False)

            # Compute a pseudo XPath used to match the template type overrides
            path = ""
            for t in tag_stack[2:]:
                path += "/" + t[0]
            path += "/" + tag
                        
            if path in object_list:
                if tag not in container:
                    container[tag] = [value]
                else:
                    container[tag].append(value)
            else:
                if path in boolean:
                    value = value.lower()
                    value = True if value == "true" else False
                elif path in numeric_int:
                    value = int(value)
                elif path in numeric_float:
                    value = float(value)
                elif path in time_datetime:
                    value = parse_rfc3339_datetime(value)
                container[tag] = value

        else:  # If not an end/closing tag add to stack
            tag_stack.append(tags)

    tag, value = tag_stack[0][:2]  # Slice gets first two items in tuple
    return value


#-------------------------------------------------------------------------------

class AioConfig():
    def __init__(self, *args, **kwargs):
        # Initialise with defaults
        config = {
            "region_name": None,
            "connect_timeout": 60,
            "read_timeout": 60,
            "max_pool_connections": 10
        }

        # Check supplied kwargs map to valid config keys
        for key, value in kwargs.items():
            # The key must exist in the available options
            if key not in config:
                raise TypeError(f"Got unexpected keyword argument \'{key}\'")

        config.update(kwargs)

        # Set attributes based on the config
        for key, value in config.items():
            setattr(self, key, value)


class OperationNotPageableError(Exception):
    def __init__(self, method):
        super().__init__(f"Operation cannot be paginated: {method}")

class PaginationError(Exception):
    pass

class ClientError(Exception):
    def __init__(self, code, method, message):
        super().__init__(f"An error occurred ({code}) when calling the {method} operation: {message}")
        # For compatibility with botocore.exceptions.ClientError
        self.response = {"Error": {"Code": code, "Message": message}}
        self.operation_name = method

class Exceptions():
    """
    Dynamically create exception classes based on their codes.
    The API calls use from_code and if a code is returned that we haven't
    already added at init time we add it then.
    """
    def __init__(self):
        codes = ["BucketAlreadyExists", "BucketAlreadyOwnedByYou", "BucketNotEmpty", "NoSuchBucket", "NoSuchKey"]
        for code in codes:
            setattr(self, code, type(code, (ClientError, ), {}))

    def from_code(self, code):
        exception = getattr(self, code, None)
        if not exception:
            exception = type(code, (ClientError, ), {})
            setattr(self, code, exception)
        return exception


#-------------------------------------------------------------------------------

class Client():
    """
    Base class for the service specific clients.
    The authentication/signing code is the main thing that is used by all
    clients, but things like paginators (TODO) might be too.
    """
    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None, aws_session_token=None, region_name=None, config=AioConfig()):
        self.AWS_ACCESS_KEY_ID = aws_access_key_id
        self.AWS_SECRET_ACCESS_KEY = aws_secret_access_key
        self.AWS_SESSION_TOKEN = aws_session_token
        self.REGION_NAME = region_name
        self.config = config
        self.exceptions = Exceptions()
        # self.ENDPOINT_URL & self.ENDPOINT_HOST are service specific, so set there

        self.request_key_cache = {}

    def aws_sig_v4_headers(self, access_key_id, secret_access_key, pre_auth_headers,
                           service, region, host, method, path, query, payload):
        """
        Generate AWS sigv4 headers needed for API requests. See also
        https://docs.aws.amazon.com/general/latest/gr/sigv4-signed-request-examples.html
        https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_use-resources.html
        If session token is used I think that it can be added either to the
        pre_auth_headers or query e.g. {"X-Amz-Security-Token": <AWS_SESSION_TOKEN>}
        TODO tidy up and also cache as much as is possible, e.g. the request_key
        for a given account, region and service can be keys with a YMD datestamp,
        which is constant for any given day.
        """
        algorithm = "AWS4-HMAC-SHA256"

        now = datetime.utcnow()
        amzdate = now.strftime("%Y%m%dT%H%M%SZ")
        datestamp = now.strftime("%Y%m%d")
        payload_hash = hashlib.sha256(payload).hexdigest()
        credential_scope = f"{datestamp}/{region}/{service}/aws4_request"

        pre_auth_headers_lower = {
            k.lower(): " ".join(v.split()) for k, v in pre_auth_headers.items()
        }
        required_headers = {
            "host": host,
            "x-amz-content-sha256": payload_hash,
            "x-amz-date": amzdate,
        }
        headers = {**pre_auth_headers_lower, **required_headers}
        header_keys = sorted(headers.keys())
        signed_headers = ";".join(header_keys)

        def signature():
            def canonical_request():
                canonical_uri = urllib.parse.quote(path, safe="/~")
                quoted_query = sorted(
                    (urllib.parse.quote(k, safe="~"), urllib.parse.quote(v, safe="~"))
                    for k, v in query.items()
                )
                canonical_querystring = "&".join(f"{k}={v}" for k, v in quoted_query)
                canonical_headers = "".join(f"{k}:{headers[k]}\n" for k in header_keys)

                return f"{method}\n{canonical_uri}\n{canonical_querystring}\n" + \
                       f"{canonical_headers}\n{signed_headers}\n{payload_hash}"

            def sign(key, msg):
                return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

            def create_request_key(key, datestamp, region, service):
                date_key = sign(("AWS4" + key).encode("utf-8"), datestamp)
                region_key = sign(date_key, region)
                service_key = sign(region_key, service)
                request_key = sign(service_key, "aws4_request")
                return request_key


            #print(canonical_request())

            # Add request_key for the current datestamp to cache. The cache only
            # needs an entry for current datestamp, so replace when it changes.
            request_key = self.request_key_cache.get(datestamp)
            if request_key == None:
                request_key = create_request_key(
                    secret_access_key, datestamp, region, service
                )
                self.request_key_cache = {datestamp: request_key}

            string_to_sign = f"{algorithm}\n{amzdate}\n{credential_scope}\n" + \
                             hashlib.sha256(canonical_request().encode("utf-8")).hexdigest()

            return sign(request_key, string_to_sign).hex()

        return {
            **pre_auth_headers,
            "x-amz-date": amzdate,
            "x-amz-content-sha256": payload_hash,
            "Authorization": f"{algorithm} Credential={access_key_id}/{credential_scope}, "
                             f"SignedHeaders={signed_headers}, Signature=" + signature(),
        }


    def create_signed_headers(self, method, path, headers, query, payload):
        return self.aws_sig_v4_headers(
            self.AWS_ACCESS_KEY_ID,
            self.AWS_SECRET_ACCESS_KEY,
            headers,
            self.SERVICE,
            self.REGION_NAME,
            self.ENDPOINT_HOST,
            method,
            path,
            query,
            payload
        )

    def get_paginator(self, method):
        """
        Create a paginator for a method.
        """
        if method not in self.pageable_methods:
            raise OperationNotPageableError(method)
        else:
            return Paginator(getattr(self, method), self.pageable_methods[method])



class StreamingBody():
    """
    aioboto3 wraps StreamingBody in context manager.
    This thin implementation doesn't yet clean anything on __aexit__ and is
    mostly just to provide API compatibility with aiobot3 at the moment, but the
    constructor is used to provide compatibility betweem aiohttp and aiosonic.
    https://aioboto3.readthedocs.io/en/latest/usage.html#streaming-download
    """
    def __init__(self, response):
        if isinstance(response, aiohttp.ClientResponse):
            self.stream = response.content
        else:
            self.stream = response

    def __await__(self):
        return self.stream.__await__()

    async def __aenter__(self):
        return self.stream

    async def __aexit__(self, *excinfo):
        pass


class HTTPClientWrapper():
    """
    This is a thin wrapper for the aiosonic.HTTPClient to make it more API
    compatible with aiohttp.ClientSession, so we can just swap between aiohttp
    and aiosonic without having to change the rest of the S3 Client code.
    """
    class RequestWrapper():
        """
        aiohttp wraps requests in a context manager but aiosonic does not. This
        trivial context manager is used to wrap aiosonic requests so that
        application code for aiohttp and aiosonic can remain the same.
        """
        def __init__(self, request):
            self.request = request

        def __await__(self):
            return self.request.__await__()

        async def __aenter__(self):
            return await self.request

        async def __aexit__(self, *excinfo):
            pass

    def __init__(self, pool_size):
        # Make timeouts broadly equivalent to aiohttp
        #sock_connect=5, pool_acquire=30, sock_read=30, request_timeout=260
        timeouts = aiosonic.timeout.Timeouts(sock_read=60, request_timeout=300)
        conn = aiosonic.connectors.TCPConnector(pool_size=pool_size, timeouts=timeouts)
        self.session = aiosonic.HTTPClient(conn)

    async def _request(self, *args, **kwargs):
        response = await self.session.request(*args, **kwargs)
        """
        Add attributes to aiosonic response to make compatible with aiohttp.
        N.B. aiohttp read and aiosonic aren't *really* equivalent, as the former
        exposes a StreamReader for retrieving chunked bodies:
        https://docs.aiohttp.org/en/stable/client_quickstart.html#streaming-response-content
        https://docs.aiohttp.org/en/stable/streams.html
        https://docs.aiohttp.org/en/stable/client_reference.html#aiohttp.ClientResponse.content
        On the other hand the aiosonic HttpResponse Object has mathods to read
        the entire body like content() exposed below, however for streaming it
        exposes an iterator read_chunks() rather than a stream reader, so work
        will be required to wrap read_chunks() as a stream reader to support
        the get_object streaming API fully.
        https://aiosonic.readthedocs.io/en/latest/reference.html#aiosonic.HttpResponse
        """
        response.status = response.status_code
        response.read = response.content
        return response

    def request(self, method, url, **kwargs):
        kwargs["method"] = method
        return self.RequestWrapper(self._request(url, **kwargs))

    def put(self, url, **kwargs):
        return self.request("PUT", url, **kwargs)

    def get(self, url, **kwargs):
        return self.request("GET", url, **kwargs)

    def delete(self, url, **kwargs):
        return self.request("DELETE", url, **kwargs)

    async def close(self):
        await self.session.shutdown()


#-------------------------------------------------------------------------------

class S3(Client):
    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None, aws_session_token=None, region_name=None, endpoint_url=None, config=None):
        super().__init__(aws_access_key_id=aws_access_key_id,
                         aws_secret_access_key=aws_secret_access_key,
                         aws_session_token=aws_session_token,
                         region_name=region_name, config=config)
        self.SERVICE = "s3"
        self.ENDPOINT_URL = endpoint_url if endpoint_url else f"http://s3-{self.REGION_NAME}.amazonaws.com"
        self.ENDPOINT_HOST = urllib.parse.urlparse(self.ENDPOINT_URL).netloc

        # Use onfig.http_client to Select between aiohttp and aiosonic
        if hasattr(config, "http_client") and config.http_client == "aiohttp":
            conn = aiohttp.TCPConnector(limit=config.max_pool_connections)
            self.session = aiohttp.ClientSession(connector=conn)
        else:  # Default to aiosonic
            self.session = HTTPClientWrapper(pool_size=config.max_pool_connections)

        # From https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
        # A map of HTTP response headers to response object keys and types.
        self.header_mapping = {
            "x-amz-delete-marker": ("DeleteMarker", bool),
            "accept-ranges": "AcceptRanges",
            "x-amz-expiration": "Expiration",
            "x-amz-restore": "Restore",
            "last-modified": ("LastModified", datetime),
            "content-length": ("ContentLength", int),
            "etag": "ETag",
            "x-amz-missing-meta": ("MissingMeta", int),
            "x-amz-version-id": "VersionId",
            "cache-control": "CacheControl",
            "content-disposition": "ContentDisposition",
            "content-encoding": "ContentEncoding",
            "content-language": "ContentLanguage",
            "content-range": "ContentRange",
            "content-type": "ContentType",
            "expires": ("Expires", datetime),
            "x-amz-website-redirect-location": "WebsiteRedirectLocation",
            "x-amz-server-side-encryption": "ServerSideEncryption",
            "x-amz-server-side-encryption-customer-algorithm": "SSECustomerAlgorithm",
            "x-amz-server-side-encryption-customer-key-MD5": "SSECustomerKeyMD5",
            "x-amz-server-side-encryption-aws-kms-key-id": "SSEKMSKeyId",
            "x-amz-server-side-encryption-bucket-key-enabled": ("BucketKeyEnabled", bool),
            "x-amz-storage-class": "StorageClass",
            "x-amz-request-charged": "RequestCharged",
            "x-amz-replication-status": "ReplicationStatus",
            "x-amz-mp-parts-count": ("PartsCount", int),
            "x-amz-tagging-count": ("TagCount", int),
            "x-amz-object-lock-mode": "ObjectLockMode",
            "x-amz-object-lock-retain-until-date": ("ObjectLockRetainUntilDate", datetime),
            "x-amz-object-lock-legal-hold": "ObjectLockLegalHoldStatus",
        }

        # TODO make these more generic/configurable and move into base Client class
        self.pageable_methods = {
            "list_objects": {
                "more_results": "IsTruncated",
                "limit_key": "MaxKeys",
                "output_token": ["NextMarker || Contents[-1].Key"],
                "input_token": ["Marker"],
                "result_key": ["Contents", "CommonPrefixes"]
            },
            "list_objects_v2": {
                "more_results": "IsTruncated",
                "limit_key": "MaxKeys",
                "output_token": ["NextContinuationToken"],
                "input_token": ["ContinuationToken"],
                "result_key": ["Contents", "CommonPrefixes"]
            }
        }

        # Templates for generating dict response objects from XML responses
        self.list_objects_template = {
            "int": {"/Contents/Size", "/KeyCount", "/MaxKeys"},
            "bool": {"/IsTruncated"},
            "object_list": {"/Contents", "/CommonPrefixes"},
            "datetime": {"/Contents/LastModified"}
        }

        self.delete_objects_template = {
            "bool": {"/Deleted/DeleteMarker"}
        }

    # Async context manager to allow auto session.close()
    async def __aenter__(self):
        return self

    async def __aexit__(self, *excinfo):
        await self.session.close()

    def parse_error_code(self, response):
        response_string = response.decode("utf-8")
        """
        Attempt to grok the error code and error message using simple
        string splitting rather than full XML parsing. It's cheap,
        dirty and fragile to Elements arriving in different orders, but
        works with minio and probably real S3 too.
        """
        code = response_string.rsplit("<Error><Code>")
        if len(code) == 2:
            message = code[1].split("</Code><Message>")
            code = message[0]
            message = message[1].split("</Message>")[0]
        return code, message

    async def handle_error(self, response, method):
        response = await response.read()
        if response == b"":
            if response.status == 400:
                code = "BadRequest"
                message = "The server could not understand the request due to invalid syntax."
            else:
                code = f"{response.status}"
                message = "Unknown HTTP error with no message body returned by server."
        else:
            code, message = self.parse_error_code(response)
        raise self.exceptions.from_code(code)(code, method, message)

    async def api_request(self, api_call, method, path, headers={}, query={}, data=None):
        """
        Invoke the underlying REST API in a fairly general way such that it
        may be reused by the various SDK API calls. Note that api_call is
        just a name used to create error messages and is not actually used in
        the real API call - TODO there may be a better way of passing this
        name, possibly by dynamically generating methods,
        """
        uri = self.ENDPOINT_URL + path
        headers = self.create_signed_headers(
            method, path, headers, query, data if data else b""
        )
                
        response = await self.session.request(
            method, uri, headers=headers, params=query, data=data
        )
        if response.status >= 300:
            await self.handle_error(response, api_call)
        else:
            return response

    def create_query_from_args(self, **kwargs):
        query={}
        for key, value in kwargs.items():
            if key != "Bucket" and key != "Key" and value != None:
                query[to_hyphen_case(key)] = str(value)
        return query

    async def put_object(self, Body=None, Bucket=None, Key=None):
        # Request data needs to be bytes, so convert it if Body is a str
        if isinstance(Body, str):
            Body = bytes(Body, "utf-8")

        await self.api_request("PutObject", "PUT", f"/{Bucket}/{Key}", data=Body)

    async def get_object(self, Bucket=None, Key=None,
                         MinimiseResponse=True, ResponseDatesAsStrings=True, **kwargs):
        """
        MinimiseResponse and ResponseDatesAsStrings are optimisations not part
        of the boto3 SDK API. If MinimiseResponse is True then only "Body" is
        set in the response object providing improved performance for use cases
        where only the object body is required. Alternatively where more response
        headers are required setting ResponseDatesAsStrings to True will suppress
        conversion of LastModified, Expires etc. to datetime, instead returning
        them as strings, which canimprove performance if datetime representation
        of those fields is not required.
        """
        response = await self.api_request("GetObject", "GET", f"/{Bucket}/{Key}")
        if response.status == 200:
            obj = {}
            if not MinimiseResponse:
                headers = response.headers
                # Map API header keys to SDK response dict keys using a table for speed.
                for key, value in headers.items():
                    key = self.header_mapping.get(key.lower())
                    if not key:
                        pass
                    elif isinstance(key, str):
                        obj[key] = value
                    elif key[1] == int or key[1] == float:
                        obj[key[0]] = key[1](value)
                    elif key[1] == bool:
                        value = value.lower()
                        value = True if value == "true" else False
                        obj[key[0]] = value
                    elif key[1] == datetime:
                        if ResponseDatesAsStrings:
                            obj[key[0]] = value
                        else:
                            """
                            Oddly the LastModified and Date values returned as headers
                            in GetObject are in "%a, %d %b %Y %H:%M:%S %Z" format
                            whereas LastModified in ListObjectsV2 XML is actually in
                            rfc3339. Try the observed format and fall back to rfc3339
                            in case this is just a quirk with minio.
                            """
                            try: 
                                time = datetime.strptime(value, "%a, %d %b %Y %H:%M:%S %Z")
                            except ValueError:
                                time = parse_rfc3339_datetime(value)
                            obj[key[0]] = time

            obj["Body"] = StreamingBody(response)
            return obj

    async def create_bucket(self, Bucket=None):
        await self.api_request("CreateBucket", "PUT", f"/{Bucket}")

    async def delete_bucket(self, Bucket=None):
        await self.api_request("DeleteBucket", "DELETE", f"/{Bucket}")

    async def list_objects(self, **kwargs):
        """
        Implements both ListObjects and ListObjectsV2.
        """
        query = self.create_query_from_args(**kwargs)
        api_call = "ListObjectsV2" if query.get("list-type") == "2" else "ListObjects"

        path = f"/{kwargs.get('Bucket', '')}"
        response = await self.api_request(api_call, "GET", path, query=query)
        if response.status == 200:
            response = await response.read()  # Read the response body
            value = deserialise_XML(response, self.list_objects_template)
            result = value["ListBucketResult"]
            return result

    async def list_objects_v2(self, **kwargs):
        kwargs["list-type"] = "2"
        return await self.list_objects(**kwargs)

    async def delete_objects(self, **kwargs):
        # https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
        #print("delete_objects")
        #print(kwargs)
        #print()

        delete = kwargs.get("Delete", {})
        objects = delete.get("Objects", [])
        quiet = delete.get("Quiet", False)

        rlist = [b'<Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/">']
        for object in objects:
            rlist.append(b'<Object><Key>')
            rlist.append(bytes(object["Key"], 'utf-8'))
            rlist.append(b'</Key>')

            version_id = object.get("VersionId")
            if version_id:
                rlist.append(b'<VersionId>')
                rlist.append(bytes(version_id, 'utf-8'))
                rlist.append(b'</VersionId>')
            rlist.append(b'</Object>')
        if quiet:
            rlist.append(b'<Quiet>true</Quiet>')
        rlist.append(b'</Delete>')
        request = b"".join(rlist)

        #print(request)

        # Content-Md5 Needs to be base64 encoded not hexdigest
        md5 = base64.b64encode(hashlib.md5(request).digest()).decode('utf-8')
        #print(md5)

        query = {"delete": ""}
        headers = {"Content-Md5": md5}
        path = f"/{kwargs.get('Bucket', '')}"
        response = await self.api_request(
            "DeleteObjects", "POST", path, query=query, headers=headers, data=request
        )
        if response.status == 200:
            response = await response.read()  # Read the response body
            value = deserialise_XML(response, self.delete_objects_template)
            result = value["DeleteResult"]
            #print(result)
            return result


#-------------------------------------------------------------------------------

class AsyncBytesIOWrapper():
    """
    This is a (very) thin wrapper to allow BytesIO read() to be awaited.
    """
    def __init__(self, initial_bytes):
        self.stream = io.BytesIO(initial_bytes)

    async def read(self, size=-1):
        return self.stream.read(size)

class RedisS3(Client):
    async def get_connection(full_url, pool_size=50):
        """
        get_connection() supports URLs of the form:
        redis://localhost:6379?connection_attempts=20&retry_delay=10
        Redis URLs don't actually have a connection_attempts/retry_delay
        but we add them as they are convenient and consistent with AMQP URLs
        """
        split = full_url.split("?")  # Get query part
        url = split[0]  # The main URL before the ?
        # Use list comprehension to create options dict by splitting on & then =
        options = dict([] if len(split) == 1 else [
            i.split("=") for i in split[1].split("&")
        ])

        # https://github.com/aio-libs/aioredis-py/issues/930
        # pip3 install aioredis==2.0.0a1
        from aioredis import Redis, BlockingConnectionPool, ConnectionPool, utils

        # Defaults are the same defaults that Pika uses for AMQP connections.
        connection_attempts = int(options.get("connection_attempts", "1"))
        retry_delay = float(options.get("retry_delay", "2.0"))

        if not utils.HIREDIS_AVAILABLE:
            print("Install hiredis for improved parser performance: pip3 install hiredis")

        for i in range(connection_attempts):
            """
            BlockingConnectionPool isn't really "blocking", rather it's
            awaitable and is more reliable than the default ConnectionPool with
            high levels of concurrency, as it awaits for available connections
            rather than throwing ConnectionError. This behaviour of
            BlockingConnectionPool is much closer to that of aiohttp or aiosonic.
            """
            pool = BlockingConnectionPool(max_connections=pool_size).from_url(url)
            #pool = ConnectionPool(max_connections=pool_size).from_url(url)
            connection = Redis(connection_pool=pool)
            #connection = Redis.from_url(url)
            try:
                await connection.ping()  # Check connection has succeeded
                return connection
            except Exception as e:
                err = e
            await asyncio.sleep(retry_delay)

        sys.exit(1)

    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None, aws_session_token=None, region_name=None, endpoint_url=None, config=None):
        super().__init__(aws_access_key_id=aws_access_key_id,
                         aws_secret_access_key=aws_secret_access_key,
                         aws_session_token=aws_session_token,
                         region_name=region_name, config=config)

        self.redis = None
        self.ENDPOINT_URL = endpoint_url 
        """
        # From https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
        # A map of HTTP response headers to response object keys and types.
        self.header_mapping = {
            "x-amz-delete-marker": ("DeleteMarker", bool),
            "accept-ranges": "AcceptRanges",
            "x-amz-expiration": "Expiration",
            "x-amz-restore": "Restore",
            "last-modified": ("LastModified", datetime),
            "content-length": ("ContentLength", int),
            "etag": "ETag",
            "x-amz-missing-meta": ("MissingMeta", int),
            "x-amz-version-id": "VersionId",
            "cache-control": "CacheControl",
            "content-disposition": "ContentDisposition",
            "content-encoding": "ContentEncoding",
            "content-language": "ContentLanguage",
            "content-range": "ContentRange",
            "content-type": "ContentType",
            "expires": ("Expires", datetime),
            "x-amz-website-redirect-location": "WebsiteRedirectLocation",
            "x-amz-server-side-encryption": "ServerSideEncryption",
            "x-amz-server-side-encryption-customer-algorithm": "SSECustomerAlgorithm",
            "x-amz-server-side-encryption-customer-key-MD5": "SSECustomerKeyMD5",
            "x-amz-server-side-encryption-aws-kms-key-id": "SSEKMSKeyId",
            "x-amz-server-side-encryption-bucket-key-enabled": ("BucketKeyEnabled", bool),
            "x-amz-storage-class": "StorageClass",
            "x-amz-request-charged": "RequestCharged",
            "x-amz-replication-status": "ReplicationStatus",
            "x-amz-mp-parts-count": ("PartsCount", int),
            "x-amz-tagging-count": ("TagCount", int),
            "x-amz-object-lock-mode": "ObjectLockMode",
            "x-amz-object-lock-retain-until-date": ("Objeself.configctLockRetainUntilDate", datetime),
            "x-amz-object-lock-legal-hold": "ObjectLockLegalHoldStatus",
        }
        """

        # TODO make these more generic/configurable and move into base Client class
        self.pageable_methods = {
            "list_objects": {
                "more_results": "IsTruncated",
                "limit_key": "MaxKeys",
                "output_token": ["NextMarker || Contents[-1].Key"],
                "input_token": ["Marker"],
                "result_key": ["Contents", "CommonPrefixes"]
            },
            "list_objects_v2": {
                "more_results": "IsTruncated",
                "limit_key": "MaxKeys",
                "output_token": ["NextContinuationToken"],
                "input_token": ["ContinuationToken"],
                "result_key": ["Contents", "CommonPrefixes"]
            }
        }


    async def session(self):
        if not self.redis:
            self.redis = await RedisS3.get_connection(
                self.ENDPOINT_URL, pool_size=self.config.max_pool_connections
            )
        return self.redis

    # Async context manager to allow auto session.close()
    async def __aenter__(self):
        # Establish connection pool when asyncio context is entered
        await self.session()
        return self

    async def __aexit__(self, *excinfo):
        redis = await self.session()
        await redis.close()

    async def put_object(self, Body=None, Bucket=None, Key=None):
        # Request data needs to be bytes, so convert it if Body is a str
        if isinstance(Body, str):
            Body = bytes(Body, "utf-8")

        redis = await self.session()
        await redis.set(f"{Bucket}/{Key}", Body)

    async def get_object(self, Bucket=None, Key=None,
                         MinimiseResponse=True, ResponseDatesAsStrings=True, **kwargs):
        redis = await self.session()
        data = await redis.get(f"{Bucket}/{Key}")
        stream = AsyncBytesIOWrapper(data)

        obj = {}
        obj["Body"] = StreamingBody(stream)
        return obj

        """
        MinimiseResponse and ResponseDatesAsStrings are optimisations not part
        of the boto3 SDK API. If MinimiseResponse is True then only "Body" is
        set in the response object providing improved performance for use cases
        where only the object body is required. Alternatively where more response
        headers are required setting ResponseDatesAsStrings to True will suppress
        conversion of LastModified, Expires etc. to datetime, instead returning
        them as strings, which canimprove performance if datetime representation
        of those fields is not required.
        """
        """
        response = await self.api_request("GetObject", "GET", f"/{Bucket}/{Key}")
        if response.status == 200:
            obj = {}
            if not MinimiseResponse:
                headers = response.headers
                # Map API header keys to SDK response dict keys using a table for speed.
                for key, value in headers.items():
                    key = self.header_mapping.get(key.lower())
                    if not key:
                        pass
                    elif isinstance(key, str):
                        obj[key] = value
                    elif key[1] == int or key[1] == float:
                        obj[key[0]] = key[1](value)
                    elif key[1] == bool:
                        value = value.lower()
                        value = True if value == "true" else False
                        obj[key[0]] = value
                    elif key[1] == datetime:
                        if ResponseDatesAsStrings:
                            obj[key[0]] = value
                        else:

                            Oddly the LastModified and Date values returned as headers
                            in GetObject are in "%a, %d %b %Y %H:%M:%S %Z" format
                            whereas LastModified in ListObjectsV2 XML is actually in
                            rfc3339. Try the observed format and fall back to rfc3339
                            in case this is just a quirk with minio.

                            try: 
                                time = datetime.strptime(value, "%a, %d %b %Y %H:%M:%S %Z")
                            except ValueError:
                                time = parse_rfc3339_datetime(value)
                            obj[key[0]] = time

            obj["Body"] = StreamingBody(response)
            return obj
        """

    async def create_bucket(self, Bucket=None):
        redis = await self.session()
        if await redis.sismember("s3://", Bucket):
            code = "BucketAlreadyOwnedByYou"
            message = "Your previous request to create the named bucket succeeded and you already own it."
            raise self.exceptions.from_code(code)(code, "CreateBucket", message)
        await redis.sadd("s3://", Bucket)  # Add the "Bucket"

    async def delete_bucket(self, Bucket=None):
        redis = await self.session()
        if not await redis.sismember("s3://", Bucket):
            code = "NoSuchBucket"
            message = "The specified bucket does not exist."
            raise self.exceptions.from_code(code)(code, "DeleteBucket", message)

        bucket_has_objects = False
        cursor, iterable = await redis.scan(cursor=0, match=f"{Bucket}/*")
        for key in iterable:
            bucket_has_objects = True
            break

        if bucket_has_objects:
            code = "BucketNotEmpty"
            message = "The bucket you tried to delete is not empty."
            raise self.exceptions.from_code(code)(code, "DeleteBucket", message)

        await redis.srem("s3://", Bucket)  # Remove the "Bucket"

    async def list_objects(self, **kwargs):
        """
        Implements both ListObjects and ListObjectsV2.
        """
        #api_call = "ListObjectsV2" if kwargs.get("list-type") == "2" else "ListObjects"

        bucket = kwargs.get("Bucket", "")
        max_keys = int(kwargs.get("MaxKeys", 1000))
        prefix = kwargs.get("Prefix", "")

        response = {"Name": bucket, "MaxKeys": max_keys, "Prefix": prefix}
        continuation_token = kwargs.get("ContinuationToken")
        if continuation_token:
            response["ContinuationToken"] = continuation_token
            cursor = continuation_token
        else:
            cursor = 0

        redis = await self.session()
        cursor, iterable = await redis.scan(
            cursor=cursor, count=max_keys, match=f"{bucket}/{prefix}*"
        )
        if cursor:
            response["IsTruncated"] = True
            response["NextContinuationToken"] = cursor
        else:
            response["IsTruncated"] = False

        """
        TODO how to get things like Size and LastModified metadata for objects
        without having to do a secondary look-up, as redis.scan() only recovers
        keys. One option is to cache the info client side and use server
        assisted client side caching to notify of any changes made to keys.
        """
        contents = []
        for key in iterable:
            contents.append({
                "Key": key.decode("utf-8")[len(bucket) + 1:],
                "StorageClass": "STANDARD"
            })

        response["Contents"] = contents
        response["KeyCount"] = len(contents)
        return response

    async def list_objects_v2(self, **kwargs):        
        kwargs["list-type"] = "2"
        return await self.list_objects(**kwargs)
        
    async def delete_objects(self, **kwargs):
        prefix = kwargs.get("Bucket", "")
        # Get "Objects" and "Quiet" from "Delete" field of request.
        delete = kwargs.get("Delete", {})
        # From docs: "When you add this element, you must set its value to true."
        quiet = True if "Quiet" in delete else False
        objects = delete.get("Objects", [])
        keys = []
        for obj in objects:
            key = prefix + "/" + obj.get("Key")
            keys.append(key)

        redis = await self.session()
        await redis.delete(*keys)

        """
        For now just assume successful deletion. Redis only returns a count of
        the number of deleted keys so there's no (obvious) way to identify which
        specific keys might have failed to delete.
        """
        response = {}
        if not quiet:
            response["Deleted"] = objects

        return response


#-------------------------------------------------------------------------------

def decode_token(token):
    """
    Decodes an "opaque" token string to a dictionary.
    """
    json_string = base64.b64decode(token.encode('utf-8')).decode('utf-8')
    return json.loads(json_string)

def encode_token(token):
    """
    Encodes dictionaries into an "opaque" string.
    """
    json_string = json.dumps(token)
    return base64.b64encode(json_string.encode('utf-8')).decode('utf-8')

def inject_token(dictionary, token):
    for key, value in token.items():
        if (value is not None) and (value != 'None'):
            dictionary[key] = value
        elif key in dictionary:
            del dictionary[key]

class Paginator(object):
    def __init__(self, method, config):
        self.method = method
        self.config = config

    def paginate(self, **kwargs):
        """
        Create paginator object for an operation.

        This returns an iterable object. Iterating over
        this object will yield a single page of a response
        at a time.
        """
        pagination_config = kwargs.pop("PaginationConfig", {})
        limit_key = self.config["limit_key"]
        max_items = pagination_config.get("MaxItems", None)
        max_items = int(max_items) if max_items else None
        starting_token = pagination_config.get("StartingToken", None)
        page_size = pagination_config.get("PageSize", None)
        if page_size and not limit_key:
            raise PaginationError("PageSize parameter is not supported for the " +
                                  "pagination interface for this operation.")
        page_size = int(page_size) if page_size else None
        return PageIterator(
            self.method,
            self.config["input_token"], self.config["output_token"],
            self.config["more_results"], self.config["result_key"], 
            self.config.get('non_aggregate_keys', []), limit_key,
            # From PaginationConfig
            max_items, starting_token, page_size,
            kwargs)
        
class PageIterator():
    def __init__(self, method, input_token, output_token, more_results,
                 result_keys, non_aggregate_keys, limit_key, max_items,
                 starting_token, page_size, method_kwargs):
        self.method = method
        self.input_token = input_token    # e.g. ["Marker"] or ["ContinuationToken"]
        self.output_token = output_token  # e.g. ["NextMarker", "Contents[-1].Key"]
        self.more_results = more_results  # e.g. "IsTruncated"
        self.result_keys = result_keys    # e.g. ["Contents", "CommonPrefixes"]
        self.max_items = max_items
        self.limit_key = limit_key        # e.g. "MaxKeys"
        self.starting_token = starting_token
        self.page_size = page_size
        self.method_kwargs = method_kwargs
        self.resume_token = None

    def __aiter__(self):
        return self.__anext__()

    async def __anext__(self):
        method_kwargs = self.method_kwargs
        previous_next_token = None
        next_token = {key: None for key in self.input_token}

        # The number of items from result_key we've seen so far.
        total_items = 0
        first_request = True
        primary_result_key = self.result_keys[0]
        starting_truncation = 0

        # If StartingToken has been specified inject it into method's kwargs.
        if self.starting_token is not None:
            # The starting token is a dict passed as a base64 encoded string.
            next_token = decode_token(self.starting_token)
            starting_truncation = next_token.pop("boto_truncate_amount", 0)
            inject_token(method_kwargs, next_token)
        if self.page_size is not None:
            # Set limit_key parameter name (e.g. MaxKeys) to page size if set
            method_kwargs[self.limit_key] = self.page_size

        #print(method_kwargs)
        #print()

        while True:
            # Actually invoke the paginated method e.g. list_objects_v2
            response = await self.method(**method_kwargs)

            if first_request:
                # The first request is handled differently.  We could
                # possibly have a resume/starting token that tells us where
                # to index into the retrieved page.
                if self.starting_token is not None:
                    self._handle_first_request(response, primary_result_key, starting_truncation)
                first_request = False

            current_response = response.get(primary_result_key, [])
            num_current_response = len(current_response)

            #print(current_response)
            #print(num_current_response)

            truncate_amount = 0
            if self.max_items is not None:
                truncate_amount = total_items + num_current_response - self.max_items
            if truncate_amount > 0:
                self._truncate_response(response, primary_result_key,
                                        truncate_amount, starting_truncation,
                                        next_token)
                yield response
                break
            else:
                yield response
                total_items += num_current_response

                next_token = {}
                # If more results (e.g. IsTruncated is True) create next token
                if self.more_results and response.get(self.more_results, False):
                    next_token = {}
                    # Get e.g. NextContinuationToken value and set ContinuationToken
                    for output, input in zip(self.output_token, self.input_token):
                        #print(f"output: {output}")
                        #print(f"input: {input}")
                        if output == "NextMarker || Contents[-1].Key":
                            token = response.get("NextMarker", response.get("Contents"))
                            if isinstance(token, list):
                                token = token[-1].get("Key")
                        else:
                            token = response.get(output)
                        #print(f"token: {token}")
                        # Don't include empty strings as tokens, treat them as None.
                        if token:
                            next_token[input] = token
                        else:
                            next_token[input] = None

                #print(f"next_token: {next_token}")

                if all(t is None for t in next_token.values()):
                    break  # If the next token has no values we're done iterating
                if self.max_items is not None and total_items == self.max_items:
                    #print("On a page boundary")
                    # We're on a page boundary so we can set the current
                    # next token to be the resume token.
                    if "boto_truncate_amount" in value:
                        token_keys = sorted(self._input_token + ["boto_truncate_amount"])
                    else:
                        token_keys = sorted(self._input_token)
                    dict_keys = sorted(next_token.keys())

                    if token_keys == dict_keys:
                        self.resume_token = encode_token(next_token)
                    else:
                        raise ValueError("Bad starting token: {next_token}")

                    break
                if previous_next_token is not None and previous_next_token == next_token:
                    raise PaginationError(
                        f"The same next token was received twice: {next_token}"
                    )

                inject_token(method_kwargs, next_token)
                previous_next_token = next_token


    def _handle_first_request(self, response, primary_result_key,
                              starting_truncation):
        #print("_handle_first_request")
        #print(primary_result_key)
        #print(response)

        original = response.get(primary_result_key, [])  # e.g. response["Contents"]
        truncated = original[starting_truncation:]
        response[primary_result_key] = truncated

        # We also need to truncate any secondary result keys
        # because they were not truncated in the previous last
        # response.
        for token in self.result_keys:
            if token == primary_result_key:
                continue
            #print(token)

            sample = response.get(token)
            #print(sample)
            if isinstance(sample, list):
                empty_value = []
            elif isinstance(sample, str):
                empty_value = ""
            elif isinstance(sample, (int, float)):
                empty_value = 0
            else:
                empty_value = None
            response[token] = empty_value
            #set_value_from_jmespath(response, token.expression, empty_value)

        
    def _truncate_response(self, response, primary_result_key, truncate_amount,
                           starting_truncation, next_token):
        original = response.get(primary_result_key, [])  # e.g. response["Contents"]
        amount_to_keep = len(original) - truncate_amount
        truncated = original[:amount_to_keep]
        response[primary_result_key] = truncated

        # The issue here is that even though we know how much we've truncated
        # we need to account for this globally including any starting
        # left truncation. For example:
        # Raw response: [0,1,2,3]
        # Starting index: 1
        # Max items: 1
        # Starting left truncation: [1, 2, 3]
        # End right truncation for max items: [1]
        # However, even though we only kept 1, this is post
        # left truncation so the next starting index should be 2, not 1
        # (left_truncation + amount_to_keep).
        next_token["boto_truncate_amount"] = amount_to_keep + starting_truncation

        token_keys = sorted(self.input_token + ["boto_truncate_amount"])
        dict_keys = sorted(next_token.keys())

        if token_keys == dict_keys:
            self.resume_token = encode_token(next_token)
        else:
            raise ValueError("Bad starting token: {next_token}")


#-------------------------------------------------------------------------------

class Session():
    config_stamp = 0
    credentials_stamp = 0
    config_dict = {}

    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None, aws_session_token=None, region_name=None, profile_name="default"):
        """
        Configure the Session. First check the modify timestamp of
        ~/.aws/config and ~/.aws/credentials and if they differ from our cached
        values then load them into a (static) config_dict that we can
        subsequently use when creating Session instances.
        """
        aws_dir = os.path.expanduser("~/.aws")
        config_path = aws_dir + "/config"
        credentials_path = aws_dir + "/credentials"

        con_stamp = os.stat(config_path).st_mtime
        cred_stamp = os.stat(credentials_path).st_mtime

        # If config/credentials have changed load then to cache.
        if con_stamp != Session.config_stamp or cred_stamp != Session.credentials_stamp:
            Session.config_dict = {}
            try:
                def parse(aws_file):
                    with open(aws_file, "r") as fp:
                        key = ""
                        for line in fp:
                            if line.startswith("["):
                                key = line[1:-2]
                                if key not in Session.config_dict:
                                    Session.config_dict[key] = {}
                            elif key and "=" in line:
                                k, v = line.split("=")
                                Session.config_dict[key][k.strip()] = v.strip()
                parse(config_path)
                parse(credentials_path)
            except IOError as e:
                pass

            Session.config_stamp = con_stamp
            Session.credentials_stamp = cred_stamp

        # Configure instance attributes from supplied args or profile
        profile = Session.config_dict.get(profile_name, {})

        self.AWS_ACCESS_KEY_ID = aws_access_key_id if aws_access_key_id else profile.get("aws_access_key_id")
        self.AWS_SECRET_ACCESS_KEY = aws_secret_access_key if aws_secret_access_key else profile.get("aws_secret_access_key")
        self.AWS_SESSION_TOKEN = aws_session_token if aws_session_token else profile.get("aws_session_token")
        self.REGION_NAME = region_name if region_name else profile.get("region", "eu-west-2")
        self.PROFILE_NAME = profile_name

    @property
    def available_profiles(self):
        return Session.config_dict.keys()

    def client(self, service_name, region_name=None, endpoint_url=None, aws_access_key_id=None, aws_secret_access_key=None, aws_session_token=None, config=None):
        # If arguments are not specified get values from Session
        region_name = region_name if region_name else self.REGION_NAME
        aws_access_key_id = aws_access_key_id if aws_access_key_id else self.AWS_ACCESS_KEY_ID
        aws_secret_access_key = aws_secret_access_key if aws_secret_access_key else self.AWS_SECRET_ACCESS_KEY
        aws_session_token = aws_session_token if aws_session_token else self.AWS_SESSION_TOKEN
        # If endpoint_url not specified the default value is service specific
        # so pass directly through to S3 constructor.
        if endpoint_url.startswith("redis://"):
            return RedisS3(
                region_name=region_name,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
                endpoint_url=endpoint_url,
                config=config
            )
        else:
            return S3(
                region_name=region_name,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
                endpoint_url=endpoint_url,
                config=config
            )

