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
# PYTHONPATH=.. python3 aioboto3_s3_put_get.py
#
"""
This example illustrates using aioboto3 for native asyncio s3.put_object and
s3.get_object.


To get maximum performance it is important to ensure that concurrency is
maximised and aioboto3 s3 write performance in particular seems to require lots
of connections (MAX_CONNECTIONS = 1000 seemed optimal) and launching such that
we have ~1000 concurrent tasks (one per connection it seems) gave maximum
write performance (the launch_as_tasks_in_batches() example)

Using a single instance Docker minio and the default overlayfs data directory
with 5KB objects the put_object rate seems to be around 547 items/s and
the get_object rate seems to be around 715 items/s

It looks like it may be possible to improve things further as the bottleneck
*appears* now to be the performance of the underlying aiohttp library, but
it's still significantly faster than the blocking version, though still kind of
underwhelming compared to the performance of AMQP pass-by-value.
"""

import sys
assert sys.version_info >= (3, 8) # Bomb out if not running Python3.8

import asyncio, aiobotocore, aioboto3, os, time, uuid

from utils.logger import init_logging

from s3_utils_asyncio import (
    create_configured_session,
    create_bucket,
    purge_and_delete_bucket,
    put_object,
    get_object
)

async def naive_version():
    """
    This naive version is using aioboto3 and is, at face value, following
    the patterns illustrated in the aioboto3 examples:
    https://aioboto3.readthedocs.io/en/latest/usage.html#s3-examples
    It is basically mirroring the blocking_s3_put_get.py example, but using
    asyncio. Perhaps surprisingly the performance is almost *exactly the same*
    as the blocking version, but why?

    On closer inspection if one looks at the code iterating put_object and
    get_object calls note that we have to await those calls as the are
    coroutines *however* the next iteration if each loop is *dependent* on the
    await returning so in practice despite using asyncio this introduces a
    form of blocking. To resolve this, rather than awaiting every coroutine
    individually it is more efficient to launch in a batch and gather/wait
    for the results.
    """

    """
    Creates aioboto3.Session() configured from environment variables or users's
    profile or minio profile. The easiest way to use with minio is to add the
    following to ~/.aws/credentials (setting the key_id and key used to set
    MINIO_ROOT_USER and MINIO_ROOT_PASSWORD.

    [minio]
    aws_access_key_id = <id>
    aws_secret_access_key = <key>
    """
    session = create_configured_session(aioboto3)

    # Initialise the aioboto3 client setting the endpoint_url to our local minio
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session.client
    client = session.client("s3", endpoint_url="http://localhost:9001")
    async with client as s3:  # In aioboto3 client and resource are context managers
        await create_bucket(s3, bucket_name)
        
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

            await put_object(s3, s3_uri, content)
            object_refs.append(s3_uri)

        end = time.time()
        rate = ITERATIONS/(end - start)
        bandwidth = rate * len(content)/1024
        print(f"put_object: rate {rate} items/s, {bandwidth} KiB/s")

        #print(object_refs)

        # Test reading objects
        start = time.time()

        for s3_uri in object_refs:
            obj = await get_object(s3, s3_uri)
            #print(obj)

        end = time.time()
        rate = ITERATIONS/(end - start)
        bandwidth = rate * len(content)/1000
        print(f"get_object: rate {rate} items/s, {bandwidth} KiB/s")
        print()

        # Delete the objects we created then the bucket to tidy things up up
        await purge_and_delete_bucket(s3, bucket_name)


async def launch_as_tasks():
    """
    The naive version is basically the equivalent of blocking after each put
    or get. With this version we launch put_object/get_object as asyncio Tasks
    by appending to a list then doing asyncio.gather to run them concurrently:
    https://docs.python.org/3/library/asyncio-task.html#asyncio.gather

    N.B. This example runs *all* the put_object/get_object requests concurrently
    which potentially may result in a large number of tasks.
    """

    """
    Creates aioboto3.Session() configured from environment variables or users's
    profile or minio profile. The easiest way to use with minio is to add the
    following to ~/.aws/credentials (setting the key_id and key used to set
    MINIO_ROOT_USER and MINIO_ROOT_PASSWORD.

    [minio]
    aws_access_key_id = <id>
    aws_secret_access_key = <key>
    """
    session = create_configured_session(aioboto3)

    # Initialise the aioboto3 client setting the endpoint_url to our local minio
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session.client
    config=aiobotocore.config.AioConfig(max_pool_connections=MAX_CONNECTIONS)
    client = session.client("s3", endpoint_url="http://localhost:9001", config=config)
    async with client as s3:  # In aioboto3 client and resource are context managers
        await create_bucket(s3, bucket_name)
        
        content = "x" * 5000

        print()
        print(__file__)
        print(f"Testing {ITERATIONS} iterations, with an item size of {len(content)}")

        # Test writing objects
        start = time.time()

        object_refs = []
        tasks = []
        for i in range(ITERATIONS):
            s3_uri = f"s3://{bucket_name}/{uuid.uuid4()}"
            #print(s3_uri)

            tasks.append(put_object(s3, s3_uri, content))    
            object_refs.append(s3_uri)

        #print(len(tasks))
        await asyncio.gather(*tasks)

        end = time.time()
        rate = ITERATIONS/(end - start)
        bandwidth = rate * len(content)/1024
        print(f"put_object: rate {rate} items/s, {bandwidth} KiB/s")

        # Test reading objects
        start = time.time()

        tasks = []
        for s3_uri in object_refs:
            tasks.append(get_object(s3, s3_uri))

        results = await asyncio.gather(*tasks)
        #print(results)

        end = time.time()
        rate = ITERATIONS/(end - start)
        bandwidth = rate * len(content)/1000
        print(f"get_object: rate {rate} items/s, {bandwidth} KiB/s")
        print()

        # Delete the objects we created then the bucket to tidy things up up
        await purge_and_delete_bucket(s3, bucket_name)


async def launch_as_tasks_in_batches():
    """
    This version is similar to launch_as_tasks(), but instead of awaiting every
    invocation concurrently it instead launches in batches of size MAX_CONNECTIONS
    this approach should be most efficient and less "bursty", as there is no
    real point increasing the concurrency beyond the maximum number of
    connections.

    This approach actually seems faster than the launch_as_tasks() example.
    """

    """
    Creates aioboto3.Session() configured from environment variables or users's
    profile or minio profile. The easiest way to use with minio is to add the
    following to ~/.aws/credentials (setting the key_id and key used to set
    MINIO_ROOT_USER and MINIO_ROOT_PASSWORD.

    [minio]
    aws_access_key_id = <id>
    aws_secret_access_key = <key>
    """
    session = create_configured_session(aioboto3)

    # Initialise the aioboto3 client setting the endpoint_url to our local minio
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session.client
    config=aiobotocore.config.AioConfig(max_pool_connections=MAX_CONNECTIONS)
    client = session.client("s3", endpoint_url="http://localhost:9001", config=config)
    async with client as s3:  # In aioboto3 client and resource are context managers
        await create_bucket(s3, bucket_name)
        
        content = "x" * 5000

        print()
        print(__file__)
        print(f"Testing {ITERATIONS} iterations, with an item size of {len(content)}")

        # Test writing objects
        start = time.time()

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
        
        # Test reading objects
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
    bucket_name = "aioboto3-s3-put-get"

    # Initialise logger
    logger = init_logging(log_name=bucket_name)

    loop = asyncio.get_event_loop()
    #loop.run_until_complete(naive_version())
    #loop.run_until_complete(launch_as_tasks())
    loop.run_until_complete(launch_as_tasks_in_batches())

