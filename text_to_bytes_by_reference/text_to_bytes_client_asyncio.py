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
# PYTHONPATH=.. python3 text_to_bytes_client_asyncio.py
# PYTHONPATH=.. LOG_LEVEL=DEBUG python3 text_to_bytes_client_asyncio.py
#
# With Yappi profiling enabled
# PYTHONPATH=.. python3 text_to_bytes_client_asyncio.py -p > profile.txt
#
"""
An client intended to test the text_to_bytes AMQP Message RPC Lambda.

The client times a number of iterations of publishing request messages as fast
as it can, then asynchronously receives the results from the processor and
correlates them with the original request.

The timer starts as the first request is published and stops as the last
response is received so the overall calculated RPC invocation rate will take
into account the effects of any queueing that may be a result of the processor
not keeping up with the invocation rate.
"""

import sys
assert sys.version_info >= (3, 6) # Bomb out if not running Python3.6

import asyncio, contextlib, time, uuid
import aioboto3lite as aioboto3
import aioboto3lite as aiobotocore
#import aiobotocore, aioboto3  # Uncomment this line to use the real aioboto3/aiobotocore

from utils.logger import init_logging
from utils.amqp_0_9_1_messaging_asyncio import Connection, Message
from utils.messaging_exceptions import *
from lambda_runtime.lambda_runtime_asyncio import launch  # Boilerplate to start client

from object_store_experiments.s3_utils_asyncio import (
    create_configured_session,
    create_bucket,
    purge_and_delete_bucket,
    put_object,
    get_object
)

"""
Attempt to use ujson if available https://pypi.org/project/ujson/
"""
try:
    import ujson as json
except:  # Fall back to standard library json
    import json

"""
TODO
# Attempt to use libuuid uuid generation if available.
# https://github.com/brandond/python-libuuid/
# https://pypi.org/project/libuuid/
# pip3 install libuuid
# N.B. needs Linux distro uuid-dev package installed
"""

class TextToBinaryClient(object):
    def __init__(self, queue_name, iterations=30000, max_connections=1000):
        self.queue_name = queue_name  # queue_name is the Lambda's RPC queue
        # Initialise logger
        self.logger = init_logging(log_name=queue_name)
        self.iterations = iterations
        self.count = 0  # Counts the number of responses
        self.start_time = 0
        self.connection = None  # start_asyncio() actually opens the Connection
        self.context_stack = contextlib.AsyncExitStack()
        self.s3 = None
        self.max_connections = max_connections

        """
        In order to deal with RPC we need to be able to associate requests
        with their subsequent responses, so this pending_requests dictionary
        maps requests with their callbacks using correlation IDs.
        """
        self.pending_requests = {}

    async def handle_rpcmessage_response(self, message):
        #print(message)
        #print(message.body)

        # Safer but slower alternative to connection.session(auto_ack=True)
        #if self.count % 10 == 0:  # Periodically acknowledge consumed messages
        #    message.acknowledge()

        """
        This is a message listener receiving messages from the reply_to queue.
        TODO cater for the case where requests are sent but responses never
        arrive, this scenario will cause self.pending_requests to "leak" as
        correlation_id keys get added but not removed.
        """
        correlation_id = message.correlation_id
        request = self.pending_requests.get(correlation_id)  # Get request tuple
        if request:
            del self.pending_requests[correlation_id]
            callback = request
            if callable(callback):
                message_body = message.body
                await callback(message_body, correlation_id)
                self.count += 1
                print(f"Response count: {self.count}")

        if self.count == self.iterations:
            print()
            print("Test complete")
            duration = time.time() - self.start_time
            print("Throughput: {} items/s".format(self.iterations/duration))
            print()
            self.connection.close()

    """
    TODO use Futures to allow us to send request then await response rather
    than directly expose the on_response callback, which could ultimately be
    reduced to just resolving the future.
    """
    async def send_rpcmessage(self, body, s3_uri, data, content_type="application/json"):
        async def on_response(result, correlation_id):
            #print(result)
            #print(correlation_id)
            event = json.loads(result)
            if isinstance(event, dict):  # Error response
                print(event)
            else:  # Our text_to_bytes Lambda returns a list of dicts on success
                s3_uri = event[0].get("content")[0].get("text-ref")
                #print(s3_uri)

                response_object = await get_object(self.s3, s3_uri)
                #print(response_object)

        """
        Write data into S3
        """
        await put_object(self.s3, s3_uri, data)

        """
        Publish message to the required Lambda's RPC queue.
        """
        #print("send_rpcmessage")
        #print(body)
        
        # Associate response callback with this request via correlation ID
        # TODO faster UUID generation using Cython and libuuid because at high
        # throughput UUID generation can cause around 10% performance hit.
        correlation_id = str(uuid.uuid4())
        
        message = Message(
            body,
            content_type=content_type,
            reply_to=self.reply_to.name,
            correlation_id=correlation_id,
        )

        """
        The service response message is handled by handle_rpcmessage_response()
        Set request tuple keyed by correlation_id so we can look up the
        required callback
        """
        self.pending_requests[correlation_id] = (
            on_response
        )

        """
        When producer.enable_exceptions(True) is set the send() method is made
        awaitable by returning a Future, which we return to the caller.
        """
        return self.producer.send(message)

    async def start_asyncio(self):
        """
        The code to create and destroy aioboto3 clients is a little bit "fiddly"
        because since aioboto3 v8.0.0+, client and resource are now async
        context managers. If we want to use the same client instance in
        multiple different methods we have to use a context stack.

        The way to deal with this is to create an AsyncExitStack, which
        essentially does async with on the context manager returned by client or
        resource, and saves the exit coroutine so that it can be called later to
        clean up.
        https://aioboto3.readthedocs.io/en/latest/usage.html#aiohttp-server-example
        """
        session = create_configured_session(aioboto3)
        config = aiobotocore.config.AioConfig(max_pool_connections=self.max_connections)
        #config.http_client = "aiohttp"  # Defaults to "aiosonic"
        self.s3 = await self.context_stack.enter_async_context(
            session.client("s3", endpoint_url="http://localhost:9001", config=config)
            #session.client("s3", endpoint_url="redis://localhost:6379", config=config)
        )

        # Create S3 bucket to use in this test
        await create_bucket(self.s3, self.queue_name)

        self.connection = Connection("amqp://localhost:5672?connection_attempts=20&retry_delay=10&heartbeat=0")
        try:
            await self.connection.open()
            #session = await self.connection.session()
            session = await self.connection.session(auto_ack=True)   
            """
            Increase the consumer priority of the reply_to consumer.
            See https://www.rabbitmq.com/consumer-priority.html
            N.B. This syntax uses the JMS-like Address String which gets parsed into
            implementation specific constructs. The link/x-subscribe is an
            abstraction for AMQP link subscriptions, which in AMQP 0.9.1 maps to
            channel.basic_consume and alows us to pass the exclusive and arguments
            parameters. NOTE HOWEVER that setting the consumer priority is RabbitMQ
            specific and it might well not be possible to do this on other providers.
            """
            self.reply_to = await session.consumer(
                '; {"link": {"x-subscribe": {"arguments": {"x-priority": 10}}}}'
                #'sr-preprocessor; {"node": {"auto-delete": true}, "link": {"x-subscribe": {"arguments": {"x-priority": 10}}}}'
            )

            # Enable consumer prefetch
            self.reply_to.capacity = 100;
            await self.reply_to.set_message_listener(self.handle_rpcmessage_response)
            self.producer = await session.producer(self.queue_name)
            self.producer.enable_exceptions(sync=True)

            content = "x" * 5000  # Arbitrary data to put in S3 to pass by reference.

            waiters = []
            self.start_time = time.time()
            for i in range(self.iterations):
                #print(f"Request count: {i}")
                s3_uri = f"s3://{self.queue_name}/{uuid.uuid4()}"  # Use queue name as bucket

                parameters = {
                    "_edh": {"value": "Data Header Cruft"},
                    "content": [{"text-ref": s3_uri}]
                }
                body = json.dumps(parameters)

                try:
                    """
                    When producer.enable_exceptions(True) is set the send()
                    method is made awaitable by returning a Future, resolved
                    when the broker acks the message or exceptioned if the
                    broker nacks. For fully "synchronous" publishing one can
                    simply do: await self.producer.send(message)
                    but that has a serious throughput/latency impact waiting
                    for the publisher-confirms from the broker. The impact is
                    especially bad for the case of durable queues.
                    https://www.rabbitmq.com/confirms.html#publisher-confirms
                    https://www.rabbitmq.com/confirms.html#publisher-confirms-latency
                    To mitigate this the example below publishes in batches
                    (optimally the batch size should be roughly the same as
                    the session capacity). We store the awaitables in a list
                    then periodically do an asyncio.gather of the waiters
                    """
                    waiters.append(self.send_rpcmessage(body, s3_uri, content))
                    if len(waiters) == self.max_connections:
                        await asyncio.gather(*waiters)
                        waiters = []

                except SendError as e:
                    #print(waiters)
                    raise e

            await asyncio.gather(*waiters)  # await any outstanding tasks

            await self.connection.start(); # Wait until connection closes.
    
        except MessagingError as e:  # ConnectionError, SessionError etc.
            self.logger.info(e)

        self.connection.close()

        # Delete the bucket used in this test and all the objects in it.
        await purge_and_delete_bucket(self.s3, self.queue_name)

        await self.context_stack.aclose()  # After this s3 instance should be closed

if __name__ == '__main__':
    launch(TextToBinaryClient(queue_name="text-to-bytes"))

