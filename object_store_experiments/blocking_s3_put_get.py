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
# Run with:
# PYTHONPATH=.. python3 blocking_s3_put_get.py
#
"""
This example illustrates basic boto3 blocking s3.put_object and s3.get_object.

It first times a loop of N x iterations, creating s3://{bucket_name}/{uuid.uuid4()}
URIs and using put_object to store at the specified location. It then iterates
through the list of keys calling get_object (and reading the item from the
StreamingBody response.

Using a single instance Docker minio and the default overlayfs data directory
with 5KB objects the put_object rate seems to be around 20 items/s and
the get_object rate seems to be around 290 items/s with both the application
and minio using very little CPU.

Using tmpfs for the minio /data directory increases the put_object rate to
~260 items/s and the get_object slightly to ~300 items/s
"""

import sys
assert sys.version_info >= (3, 8) # Bomb out if not running Python3.8

import botocore, boto3, os, time, uuid
from botocore.exceptions import ClientError

from utils.logger import init_logging

from s3_utils import (
    create_configured_session,
    create_bucket,
    purge_and_delete_bucket,
    put_object,
    get_object
)

if __name__ == '__main__':
    ITERATIONS = 1000

    # Create bucket to use in this test
    bucket_name = "blocking-s3-put-get"

    # Initialise logger
    logger = init_logging(log_name=bucket_name)

    """
    Creates a boto3.Session() configured from environment variables or users's
    profile or minio profile. The easiest way to use with minio is to add the
    following to ~/.aws/credentials (setting the key_id and key used to set
    MINIO_ROOT_USER and MINIO_ROOT_PASSWORD.

    [minio]
    aws_access_key_id = guest
    aws_secret_access_key = guest@minio
    """
    session = create_configured_session(boto3)

    # Initialise the boto3 client setting the endpoint_url to our local minio
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session.client
    s3 = session.client("s3", endpoint_url="http://localhost:9001")

    create_bucket(s3, bucket_name)

    content = "x" * 5000

    print()
    print(__file__)
    print(f"Testing {ITERATIONS} iterations, with an item size of {len(content)}")
    
    # Test writing objects
    start = time.time()

    object_refs = []
    for i in range(ITERATIONS):
        s3_uri = f"s3://{bucket_name}/{uuid.uuid4()}"
        #print(s3_uri)

        put_object(s3, s3_uri, content)
        object_refs.append(s3_uri)

    end = time.time()
    rate = ITERATIONS/(end - start)
    bandwidth = rate * len(content)/1024
    print(f"put_object: rate {rate} items/s, {bandwidth} KiB/s")

    #print(object_refs)

    # Test reading objects
    start = time.time()

    for s3_uri in object_refs:
        obj = get_object(s3, s3_uri)
        #print(obj)

    end = time.time()
    rate = ITERATIONS/(end - start)
    bandwidth = rate * len(content)/1000
    print(f"get_object: rate {rate} items/s, {bandwidth} KiB/s")
    print()
    
    # Delete the objects we created then the bucket to tidy things up up
    purge_and_delete_bucket(s3, bucket_name)

