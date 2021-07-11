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
# PYTHONPATH=.. python3 asyncio_thread_pool_executor_s3_put_get.py
#
"""
This example illustrates an asyncio application. Because boto3 is a blocking
API it doesn't play nicely by default with asyncio. It is, however, still
possible to use boto3 with asyncio by launching s3.put_object and s3.get_object
calls using a concurrent.futures.ThreadPoolExecutor combined with the asyncio
run_in_executor() method which returns a Future that may be awaited.

The run_in_executor() approach is the standard way to "wrap" blocking libraries
for asyncio, but *NOTE* that unlike native asyncio libraries that
use non-blocking IO and directly integrate with the underlying event loop
wrapped libraries have additional overheads associated with thread dispatch.

This example is loosely based on the example found here:
https://medium.com/tysonworks/concurrency-with-boto3-41cfa300aab4


Using a single instance Docker minio and the default overlayfs data directory
with 5KB objects the put_object rate seems to be around 193 items/s and
the get_object rate seems to be around 220 items/s. Both the application
and minio use much more CPU than the blocking version 53% for minio and ~170%
for the application. N.B. the get_object and put_object rates for this approach
are *less* than the basic non-asyncio thread_pool_executor_s3_put_get.py
example. That shouldn't be too surprising as it is basically performing the same
steps (running put_object and get_object functions as tasks in a 
ThreadPoolExecutor) but with the additional overhead of wrapping in asyncio
Futures.

Using tmpfs for the minio /data directory has minimal effect when using the
ThreadPoolExecutor, which needs further investigation/profiling, but a plausible
hypothesis is that lock contention is likely to be the limiting factor.

Part of the purpose of this example is to illustrate/prove that not all asyncio
approaches are equal. Whilst it can be convenient to wrap existing blocking
libraries the overheads of launching "low work" functions as Threads can be
high and almost certainly won't perform as well as a native asyncio library
that uses non-blocking IO and integrates directly with the event loop.
"""

import sys
assert sys.version_info >= (3, 8) # Bomb out if not running Python3.8

import asyncio, botocore, boto3, os, time, uuid
from botocore.exceptions import ClientError

import concurrent.futures
from functools import partial

from utils.logger import init_logging

from s3_utils import (
    create_configured_session,
    create_bucket,
    purge_and_delete_bucket,
    put_object,
    get_object
)

if __name__ == '__main__':
    ITERATIONS = 10000
    MAX_WORKERS = 300
    MAX_CONNECTIONS = 10

    # Create bucket to use in this test
    bucket_name = "asyncio-thread-pool-executor-s3-put-get"

    # Initialise logger
    logger = init_logging(log_name=bucket_name)

    """
    Creates a boto3.Session() configured from environment variables or users's
    profile or minio profile. The easiest way to use with minio is to add the
    following to ~/.aws/credentials (setting the key_id and key used to set
    MINIO_ROOT_USER and MINIO_ROOT_PASSWORD.

    [minio]
    aws_access_key_id = <id>
    aws_secret_access_key = <key>
    """
    session = create_configured_session(boto3)

    # Initialise the boto3 client setting the endpoint_url to our local minio
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session.client
    s3 = session.client("s3", endpoint_url="http://localhost:9001",
                        config=botocore.config.Config(max_pool_connections=MAX_CONNECTIONS))

    create_bucket(s3, bucket_name)

    content = "x" * 5000

    print()
    print(__file__)
    print(f"Testing {ITERATIONS} iterations and a thread pool of {MAX_WORKERS} workers, with an item size of {len(content)}")
    
    loop = asyncio.get_event_loop()

    object_refs = []

    # Coroutine to launch s3.put_object calls in an executor
    async def non_blocking_put():
        with concurrent.futures.ThreadPoolExecutor(MAX_WORKERS) as executor:
            tasks = []
            for i in range(ITERATIONS):
                s3_uri = f"s3://{bucket_name}/{uuid.uuid4()}"
                #print(s3_uri)
    
                tasks.append(loop.run_in_executor(executor, put_object, s3, s3_uri, content))
                object_refs.append(s3_uri)

            await asyncio.gather(*tasks)

    # Coroutine to launch s3.get_object calls in an executor
    async def non_blocking_get():
        with concurrent.futures.ThreadPoolExecutor(MAX_WORKERS) as executor:
            tasks = []
            for s3_uri in object_refs:
                #print(s3_uri)
    
                tasks.append(loop.run_in_executor(executor, get_object, s3, s3_uri))

            results = await asyncio.gather(*tasks)
        #print(results)
        #return results


    # Test writing objects
    start = time.time()

    loop.run_until_complete(non_blocking_put())

    end = time.time()
    rate = ITERATIONS/(end - start)
    bandwidth = rate * len(content)/1024
    print(f"put_object: rate {rate} items/s, {bandwidth} KiB/s")

    #print(object_refs)

    # Test reading objects
    start = time.time()

    results = loop.run_until_complete(non_blocking_get())

    end = time.time()
    rate = ITERATIONS/(end - start)
    bandwidth = rate * len(content)/1000
    print(f"get_object: rate {rate} items/s, {bandwidth} KiB/s")
    print()

    # Delete the objects we created then the bucket to tidy things up up
    purge_and_delete_bucket(s3, bucket_name)

