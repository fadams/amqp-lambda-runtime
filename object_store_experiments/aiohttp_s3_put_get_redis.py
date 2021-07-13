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
# PYTHONPATH=.. python3 aiohttp_s3_put_get_redis.py
#
"""
This example is exactly the same as aiohttp_s3_put_get.py, but has the Redis
endpoint_url uncommented so it uses the Redis client facade instead of HTTP/S3.

This example illustrates using aiohttp/aiosonic for native asyncio s3.put_object
and s3.get_object. The approach taken is to create a "aioboto3lite" library
which re-implements the main boto3/aioboto3 S3 CRUD methods by directly invoking
the underlying HTTP requests using aiohttp or alternatively aiosonic clients.
This approach avoids much of the many levels of indirection that can slow down
boto3 invocations, though it can get quite complicated due to the header signing
that AWS APIs require hence wrapping all of those gory details in a library.

Another advantage of creating a "fake" aioboto3 is that it is basically possible
to do a "plug in replacement" where instead of doing:
import botocore, aioboto3

we can do:
import aioboto3lite as aioboto3
import aioboto3lite as botocore

and the actual application code can remain the same for both.

N.B. At the moment aioboto3lite is very much a proof of concept and is fairly
limited, only supporting a few of the basic S3 CRUD methods however the
performance difference is *significant*

Using a single instance Docker minio and the default overlayfs data directory
with 5KB objects the put_object rate seems to be around 1429 items/s and
the get_object rate seems to be around 3238 items/s using aiosonic - that's
around 2.65x the write performance of aioboto3 and 4.5x the read performance of
aioboto3.
"""

import sys
assert sys.version_info >= (3, 8) # Bomb out if not running Python3.8

import asyncio, os, time, uuid
import aioboto3lite as aioboto3
import aioboto3lite as aiobotocore
#import aiobotocore, aioboto3  # Uncomment this line to use the real aioboto3/aiobotocore

from utils.logger import init_logging

from s3_utils_asyncio import (
    create_configured_session,
    create_bucket,
    purge_and_delete_bucket,
    put_object,
    get_object
)

async def aiohttp_launch_as_tasks_in_batches():
    """
    Creates aiohttp.Session() configured from environment variables or users's
    profile or minio profile. The easiest way to use with minio is to add the
    following to ~/.aws/credentials (setting the key_id and key used to set
    MINIO_ROOT_USER and MINIO_ROOT_PASSWORD.

    [minio]
    aws_access_key_id = <id>
    aws_secret_access_key = <key>
    """
    session = create_configured_session(aioboto3)

    config=aiobotocore.config.AioConfig(max_pool_connections=MAX_CONNECTIONS)
    #config.http_client = "aiohttp"  # Defaults to "aiosonic"
    #client = session.client("s3", endpoint_url="http://localhost:9001", config=config)
    client = session.client("s3", endpoint_url="redis://localhost:6379", config=config)
    async with client as s3:  # In aioboto3 client and resource are context managers
        await create_bucket(s3, bucket_name)
        
        content = "x" * 5000

        print()
        print(__file__)
        print(f"Testing {ITERATIONS} iterations, with an item size of {len(content)}")

        #------------------------- Test writing objects ------------------------
        start = time.time()
        overall_start = start  # Used to time aggregate put then get time

        object_refs = []
        tasks = []
        for i in range(ITERATIONS):
            s3_uri = f"s3://{bucket_name}/{uuid.uuid4()}"
            #print(s3_uri)

            tasks.append(put_object(s3, s3_uri, content))
            if len(tasks) == MAX_CONNECTIONS:
                await asyncio.gather(*tasks)
                tasks = []

            object_refs.append(s3_uri)

        #print(len(tasks))
        await asyncio.gather(*tasks)  # await any outstanding tasks

        end = time.time()
        rate = ITERATIONS/(end - start)
        bandwidth = rate * len(content)/1024
        print(f"put_object: rate {rate} items/s, {bandwidth} KiB/s")
        
        #------------------------ Test reading objects -------------------------
        start = time.time()

        tasks = []
        for s3_uri in object_refs:
            tasks.append(get_object(s3, s3_uri))
            if len(tasks) == MAX_CONNECTIONS:
                results = await asyncio.gather(*tasks)
                tasks = []

        results = await asyncio.gather(*tasks)  # await any outstanding tasks
        #print(results)

        end = time.time()
        rate = ITERATIONS/(end - start)
        bandwidth = rate * len(content)/1000
        print(f"get_object: rate {rate} items/s, {bandwidth} KiB/s")
        print()
        rate = ITERATIONS/(end - overall_start)
        bandwidth = rate * len(content)/1000
        print(f"Overall put_object then get_object: rate {rate} items/s, {bandwidth} KiB/s")
        print()

        #------------------ Test writing then reading objects ------------------
        async def put_then_get(s3, s3_uri, body):  # put followed by get as a task
            await put_object(s3, s3_uri, body)
            return await get_object(s3, s3_uri)

        start = time.time()

        object_refs = []
        tasks = []
        for i in range(ITERATIONS):
            s3_uri = f"s3://{bucket_name}/{uuid.uuid4()}"
            #print(s3_uri)

            tasks.append(put_then_get(s3, s3_uri, content))
            if len(tasks) == MAX_CONNECTIONS * 2:
                await asyncio.gather(*tasks)
                tasks = []

            object_refs.append(s3_uri)

        #print(len(tasks))
        await asyncio.gather(*tasks)  # await any outstanding tasks

        end = time.time()
        rate = ITERATIONS/(end - start)
        bandwidth = rate * len(content)/1024
        print(f"put_then_get: rate {rate} items/s, {bandwidth} KiB/s")

        # Delete the objects we created then the bucket to tidy things up up
        await purge_and_delete_bucket(s3, bucket_name)


if __name__ == '__main__':
    """
    Attempt to use uvloop libuv based event loop if available
    https://github.com/MagicStack/uvloop
    """
    try:
        import uvloop
        uvloop.install()
    except:  # Fall back to standard library asyncio epoll event loop
        pass

    ITERATIONS = 10000
    MAX_CONNECTIONS = 1000

    # Create bucket to use in this test
    bucket_name = "aiohttp-s3-put-get"

    # Initialise logger
    logger = init_logging(log_name=bucket_name)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(aiohttp_launch_as_tasks_in_batches())

