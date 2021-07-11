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
# PYTHONPATH=.. python3 aioredis_put_get.py
#
"""
This example illustrates basic blocking Redis put_object/get_object equivalents.

It first times a loop of N x iterations, creating s3://{bucket_name}/{uuid.uuid4()}
URIs and using redis.set to store at the specified location. It then iterates
through the list of keys calling redis.get.

Without hiredis parser
Using a single instance Docker Redis and a bind-mounted Redis append only file
with 5KB objects the put_object rate seems to be around 10383 items/s and
the get_object rate seems to be around 15330 items/s.

With hiredis parser (pip3 install hiredis)
Using a single instance Docker Redis and a bind-mounted Redis append only file
with 5KB objects the put_object rate seems to be around 11500 items/s and
the get_object rate seems to be around 18800 items/s.

TODO: This example is fairly trivial and simply uses Redis set and get commands
to put and get the objects. A more complete example would implement more S3
semantics, for example a "bucket" concept, which in practice would be implemented
as a key prefix.
"""

import sys
assert sys.version_info >= (3, 8) # Bomb out if not running Python3.8

import os, time, uuid
#from redis import Redis
import asyncio

# Important!!!: aioredis 2.0 should be used, which is different from previous
# versions and basically an asyncio reimplementation of redis-py
# pip3 install aioredis==2.0.0a1
# https://github.com/aio-libs/aioredis-py/issues/930
#import aioredis

from utils.logger import init_logging

"""
from s3_utils import (
    create_configured_boto3_session,
    create_bucket,
    delete_bucket,
    put_object,
    get_object
)
"""

async def get_connection(full_url, logger, pool_size=50):
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

    logger.info("Opening Connection to {}".format(url))

    # https://github.com/aio-libs/aioredis-py/issues/930
    # pip3 install aioredis==2.0.0a1
    from aioredis import Redis, BlockingConnectionPool, ConnectionPool, utils

    # Defaults are the same defaults that Pika uses for AMQP connections.
    connection_attempts = int(options.get("connection_attempts", "1"))
    retry_delay = float(options.get("retry_delay", "2.0"))

    if not utils.HIREDIS_AVAILABLE:
        print("Install hiredis for improved parser performance: pip3 install hiredis")

    for i in range(connection_attempts):
        print(pool_size)
        pool = BlockingConnectionPool(max_connections=pool_size).from_url(url)
        #pool = ConnectionPool(max_connections=pool_size).from_url(url)
        connection = Redis(connection_pool=pool)
        #connection = Redis.from_url(url)
        print(connection)
        try:
            await connection.ping()  # Check connection has succeeded
            return connection
        except Exception as e:
            err = e
            logger.warning("RedisStore: {} retrying".format(e))
            #del RedisStore.connection
        await asyncio.sleep(retry_delay)

    logger.error("RedisStore: {} connection_attempts exceeded".format(err))
    sys.exit(1)




async def launch_as_tasks_in_batches():
    redis = await get_connection(
        "redis://localhost:6379?connection_attempts=20&retry_delay=10",
        logger,
        pool_size=MAX_CONNECTIONS
    )

    #redis = await aioredis.from_url("redis://localhost:6379")

    #await create_bucket(s3, bucket_name)

    content = "x" * 5000

    print()
    print(__file__)
    print(f"Testing {ITERATIONS} iterations, with an item size of {len(content)}")

    # Test writing objects
    start = time.time()

    object_refs = []
    tasks = []
    for i in range(ITERATIONS):
        s3_uri = f"{bucket_name}/{uuid.uuid4()}"
        #print(s3_uri)

        #redis.set(s3_uri, content)
        #tasks.append(put_object(s3, s3_uri, content))
        tasks.append(redis.set(s3_uri, content))
        if len(tasks) == MAX_CONNECTIONS:
            await asyncio.gather(*tasks)
            #print("await returned")
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
        #obj = redis.get(s3_uri)
        #tasks.append(get_object(s3, s3_uri))
        tasks.append(redis.get(s3_uri))
        if len(tasks) == MAX_CONNECTIONS:
            results = await asyncio.gather(*tasks)
            #print("await returned")
            tasks = []

    results = await asyncio.gather(*tasks)  # await any outstanding tasks
    #print(results)

    end = time.time()
    rate = ITERATIONS/(end - start)
    bandwidth = rate * len(content)/1000
    print(f"get_object: rate {rate} items/s, {bandwidth} KiB/s")
    print()
    
    # Delete the objects we created then the bucket to tidy things up up
    #await delete_bucket(s3, bucket_name)

    # gracefully closing underlying connection
    await redis.close()

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

    ITERATIONS = 100000
    #MAX_CONNECTIONS = 1000
    MAX_CONNECTIONS = 200

    # Create bucket to use in this test
    # TODO implement more S3-like semantics (like buckets)
    bucket_name = "asyncio-redis-put-get"

    # Initialise logger
    logger = init_logging(log_name=bucket_name)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(launch_as_tasks_in_batches())

