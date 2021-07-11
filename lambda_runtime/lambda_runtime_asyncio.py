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
# PYTHONPATH=.. python3 lambda_runtime_asyncio.py
# PYTHONPATH=.. LOG_LEVEL=DEBUG python3 lambda_runtime_asyncio.py
#
# With Jaeger tracing enabled
# PYTHONPATH=.. python3 lambda_runtime_asyncio.py -t
#
# With Yappi profiling enabled
# PYTHONPATH=.. python3 lambda_runtime_asyncio.py -p > profile.txt
#
"""
Lambda runtime using asyncio AMQP Message RPC invocations.
Connects to AMQP queue, receives RPC request messages, decodes based on
content_type ("application/octet-stream" = raw binary, "application/json" = JSON),
invokes "lambda_handler" method using decoded event, encodes response from,
handler based on return type, sends RPC response message using correlation_id
and reply_to from request message.

Default lambda_handler simply echoes request event.
"""

import sys
assert sys.version_info >= (3, 6) # Bomb out if not running Python3.6

import argparse, asyncio, importlib, time, opentracing

from utils.logger import init_logging
from utils.open_tracing_factory import create_tracer, span_context, inject_span
from utils.amqp_0_9_1_messaging_asyncio import Connection, Message
from utils.messaging_exceptions import *

"""
Attempt to use ujson if available https://pypi.org/project/ujson/
"""
try:
    import ujson as json
except:  # Fall back to standard library json
    import json

"""
Attempt to use uvloop libuv based event loop if available
https://github.com/MagicStack/uvloop

N.B. We need to do this early *before* we run the Lambda handler module because
if that calls asyncio.get_event_loop() we want it to get the right one.
"""
try:
    import uvloop
    uvloop.install()
except:  # Fall back to standard library asyncio epoll event loop
    pass


# Default lambda_handler, simply echoes input event
def lambda_handler(event, context):
    # In a real Lambda **** DO WORK HERE ****
    return event  # Default to simple echo processor

#-------------------------------------------------------------------------------

class AMQPLambdaRuntimeAsyncio():
    def __init__(self, url, name, queue_name, lambda_handler):
        self.connection_url = url
        self.name = name
        self.queue_name = queue_name
        self.lambda_handler = lambda_handler
        self.lambda_handler_is_coroutine = asyncio.iscoroutinefunction(lambda_handler)
        self.enable_tracing = False
        self.enable_yappi = False
        # Initialise logger
        self.logger = init_logging(log_name=name)
        self.count = 0  # Counts responses to enable clean exit when profiling
        self.connection = None  # start_asyncio() actually opens the Connection

    def decode(self, message):
        """
        Use request message content_type to determine the type of the event.
        For application/octet-stream pass the raw binary, for application/json
        decode the JSON and pass the decoded object as the Lambda event.
        if content_type is unspecified try a JSON decode and if that fails
        pass the raw binary.
        """
        if message.content_type == "application/octet-stream":
            event = message.body
        elif message.content_type == "application/json":
            try:
                event = json.loads(message.body.decode("utf8"))
            except ValueError as e:
                """
                If JSON is specified this is an actual error and we should log.
                TODO decide what to do about the event, for now send empty dict.
                """
                self.logger.error(
                    "Message {} does not contain valid JSON".format(message.body)
                )
                event = {}
        else:
            try:
                event = json.loads(message.body.decode("utf8"))
            except ValueError as e:
                """
                If content_type isn't specified failure to decode JSON isn't
                an actual error as we're just making assumptions.
                Fall back to raw binary. 
                """
                event = message.body
        return event

    def encode(self, reply):
        content_type = ""
        if isinstance(reply, (bytes, bytearray)):
            content_type = "application/octet-stream"
        elif isinstance(reply, (dict, list, str)):
            reply = json.dumps(reply)
            content_type = "application/json"
        return reply, content_type

    def encode_error(self, e):
        code = type(e).__name__
        messsage = f"lambda_handler caused unhandled exception: {code}: {str(e)}"
        self.logger.error(messsage)
        error = f'{{"errorType": "{code}", "errorMessage": "{messsage}"}}'
        return error, "application/json"


    """
    rpc_handler/rpc_handler_traced are the AMQP message_listener callbacks.
    Note that they are defined as coroutines and the underlying message_listener
    in the messaging library detects if the registered callback is a subroutine
    or a coroutine. If it's a subroutine it will be called directly, however if
    it is a coroutine it is instead launched via a call to
    asyncio.get_event_loop().create_task()

    This means that rpc_handler or rpc_handler_traced invocations will run
    *concurrently*.

    The underlying lambda_handler function has some subtleties. In this runtime
    we actually support async lambda_handlers directly, e.g. of the form

    async def async_lambda_handler(event, context):
        <do awaitable asyncio stuff>
        return result

    In that case the invoke method simply awaits the result.

    However the real AWS Lambda runtime does not directly run coroutines. If the
    lambda_handler is a subroutine with no asyncio code then the invoke method
    simply returns the result.

    In order to invoke asyncio code from an AWS Lambda the following pattern
    may be used:
    https://www.trek10.com/blog/aws-lambda-python-asyncio

    async def async_lambda_handler(event, context):
        <do awaitable asyncio stuff>
        return result

    def lambda_handler(event, context):
        loop = asyncio.get_event_loop()
        if loop.is_running:
            return loop.create_task(async_lambda_handler(event, context))
        else:
            return loop.run_until_complete(async_lambda_handler(event, context))

    In this case the lambda_handler launches the async_lambda_handler as a Task
    and returns the Task (which is a Future), so if the result of calling the
    lambda_handler is a Future we can await that, which will essentially give
    the same result as awaiting the async_lambda_handler.

    The if loop.is_running test is because with this Lambda runtime we can't
    simply call loop.run_until_complete(), as the Lambda is being called from
    an already running event loop.
    """
    async def invoke(self, message):
        context = {}  # TODO make this more useful
        if self.lambda_handler_is_coroutine:
            result = await self.lambda_handler(self.decode(message), context)
        else:
            result = self.lambda_handler(self.decode(message), context)
            if isinstance(result, asyncio.Future):
                result = await result

        return self.encode(result)

    async def rpc_handler(self, message):
        try:
            reply, content_type = await self.invoke(message)
        except Exception as e:
            reply, content_type = self.encode_error(e)
        
        """
        Create the response message by reusing the request. Note that this
        approach retains the correlation_id, which is necessary. If a fresh
        Message instance is created we would need to get the correlation_id
        from the request Message and use that value in the response message.
        """
        message.subject = message.reply_to
        message.reply_to = None
        message.body = reply
        message.content_type = content_type
        self.producer.send(message)
        """
        Acknowledges the original request. Note that setting multiple=False is
        important if we want to use the consumer capacity to limit the maximum
        concurrency. Either setting consumer capacity to zero or multiple=True
        will essentially result in potentially unlimited concurrency, which
        doesn't actually improve performance and a value of 1000 seems optimal
        as it ties in with the normal maximum number of available sockets.
        """
        message.acknowledge(multiple=False)

        if self.enable_yappi:
            self.count += 1
            if self.count == 1000000:
                self.connection.close()  # Clean exit needed by yappi profiler

    async def rpc_handler_traced(self, message):
        with opentracing.tracer.start_active_span(
            operation_name="invoke " + self.name,
            child_of=span_context("text_map", message.properties, self.logger),
            tags={
                "component": self.name,
                "message_bus.destination": self.queue_name,
                "span.kind": "consumer",
                "peer.address": "amqp://localhost:5672"
            }
        ) as scope:
            try:
                reply, content_type = await self.invoke(message)
            except Exception as e:
                reply, content_type = self.encode_error(e)

            """
            Start an OpenTracing trace for the rpcmessage response.
            https://opentracing.io/guides/python/tracers/ standard tags are from
            https://opentracing.io/specification/conventions/
            """
            with opentracing.tracer.start_active_span(
                operation_name="invoke " + self.name,
                child_of=opentracing.tracer.active_span,
                tags={
                    "component": self.name,
                    "message_bus.destination": message.reply_to,
                    "span.kind": "producer",
                    "peer.address": "amqp://localhost:5672"
                }
            ) as scope:
                """
                Create the response message by reusing the request. Note that this
                approach retains the correlation_id, which is necessary. If a fresh
                Message instance is created we would need to get the correlation_id
                from the request Message and use that value in the response message.
                """
                message.properties=inject_span("text_map", scope.span, self.logger)
                message.subject = message.reply_to
                message.reply_to = None
                message.body = reply
                message.content_type = content_type
                self.producer.send(message)
                """
                Acknowledges the original request. Note that setting multiple=False is
                important if we want to use the consumer capacity to limit the maximum
                concurrency. Either setting consumer capacity to zero or multiple=True
                will essentially result in potentially unlimited concurrency, which
                doesn't actually improve performance and a value of 1000 seems optimal
                as it ties in with the normal maximum number of available sockets.
                """
                message.acknowledge(multiple=False)

                if self.enable_yappi:
                    self.count += 1
                    if self.count == 1000000:
                        self.connection.close()  # Clean exit needed by yappi profiler

    async def start_asyncio(self):
        self.connection = Connection(self.connection_url)
        try:
            await self.connection.open()
            session = await self.connection.session()
            #session = await self.connection.session(auto_ack=True)
            self.consumer = await session.consumer(
                self.queue_name + '; {"node": {"auto-delete": true}}'
            )
            #self.consumer.capacity = 0 # Unbounded consumer prefetch
            """
            Consumer prefetch provides a way to limit the maximum concurrency.
            Tuning for maximum throughput seems to be "more art than science"
            and with HTTP connections (aioboto3/aiohttp/aiosonic) a value of
            around 1000 seems optimal (possibly concurring with the maximum
            available socket file descriptors around 1024), but with Redis the
            optimum value seems to be around 80
            """
            self.consumer.capacity = 1000
            #self.consumer.capacity = 80

            if self.enable_tracing:
                await self.consumer.set_message_listener(self.rpc_handler_traced)
            else:
                await self.consumer.set_message_listener(self.rpc_handler)
            self.producer = await session.producer()

            await self.connection.start(); # Wait until connection closes.
    
        except MessagingError as e:  # ConnectionError, SessionError etc.
            self.logger.error(e)
        except Exception as e:
            self.logger.error(e)

        self.connection.close();

#-------------------------------------------------------------------------------

def global_exception_handler(loop, context):
    # context["message"] will always be there; but context["exception"] may not
    #msg = context.get("exception", context["message"])
    #print("********** global_exception_handler **********")
    #print(msg)
    pass  # Just swallow "exception was never retrieved" for now.

def launch(alt_task=None):
    """
    Launch the Lambda runtime.
    If alt_task is supplied use that instead of the Lambda runtime (it must
    have a start_asyncio() method).

    If <module>:<lambda_handler> is passed on the command line then load the
    specified module and (optionally) use the specified lambda_handler name
    insted of using the function named lambda_handler.
    """
    caller = sys._getframe(1)  # Get calling frame
    caller_globals = caller.f_globals

    default_name = caller_globals["__file__"].replace(".py", "")
    default_queue_name = caller_globals.get("QUEUE_NAME", default_name)

    description = caller_globals["__doc__"]
    parser = argparse.ArgumentParser(description=description)
    if alt_task == None:
        parser.add_argument("handler", metavar="<module>:<lambda_handler>", nargs="?",
                            help="The name of the module and/or Lambda handler to run")
    parser.add_argument("-u", "--url", help="AMQP broker connection URL of the form: amqp://[<user>:<pass>@]host[:port][/<virtualhost>][?<option>=<value>[&<option>=<value>]*]", default="amqp://localhost:5672?connection_attempts=20&retry_delay=10&heartbeat=0")
    parser.add_argument("-q", "--queue", help="The queue the Lambda runtime should bind to", default=default_queue_name)
    parser.add_argument("-t", "--tracing", help="Enable Jaeger tracing",
                        action="store_true")
    parser.add_argument("-p", "--profiling", help="Enable Yappi profiling",
                        action="store_true")
    args = parser.parse_args()
    if not hasattr(args, "handler"):
        args.handler = ""

    logger = init_logging(log_name=default_name)

    if alt_task:  # Use supplied alternative task instead of Lambda runtime.
        runtime = alt_task
    else:
        if args.handler:  # Import specified module:lambda_handler
            handler = None
            module_and_handler = (args.handler).split(":")
            module_name = module_and_handler[0]
            handler_name = "lambda_handler"  # default
            if len(module_and_handler) > 1 and module_and_handler[1]:
                handler_name = module_and_handler[1]

            try:  # Try to import the specified module
                module = importlib.import_module(module_name)
                default_name = module_name.split(".")[-1]  # Change the default_name
                logger = init_logging(log_name=default_name)
                logger.info(f"Loaded module: {module_name}")
            except ImportError as e:
                logger.error(f"Module {module_name} cannot be loaded. {e}")
                raise e

            if hasattr(module, handler_name):  # Check if imported module has handler
                logger.info(f"Using specified lambda_handler: {handler_name}")
                handler = getattr(module, handler_name)
            else:
                logger.warning(f"Specified lambda_handler: {handler_name} could not be found")

            if hasattr(module, "QUEUE_NAME"):
                args.queue = module.QUEUE_NAME
        else:
            handler = caller_globals.get("lambda_handler")  # Get lambda_handler from caller

        if not handler:
            logger.warning("module's lambda_handler could not be found, using default handler")
            handler = globals().get("lambda_handler")

        runtime = AMQPLambdaRuntimeAsyncio(
            url=args.url,
            name=default_name,
            queue_name=args.queue,
            lambda_handler=handler
        )

    if json.__name__ == "ujson":
        logger.info("Using ujson parser")
    else:
        logger.warning(
            "Using standard library json parser, install ujson for improved performance"
        )

    if globals().get("uvloop"):
        logger.info("Using uvloop event loop")
    else:
        logger.warning(
            "Using standard library epoll event loop, install uvloop for improved performance"
        )

    """
    Initialising OpenTracing here rather than in the LambdaRuntime constructor
    as opentracing.tracer is a per process object not per thread.
    The use_asyncio=True field ensures Tracing uses the main asyncio event loop
    rather than creating a new Tornado ThreadLoop
    """
    if args.tracing:
        create_tracer(runtime.name, {"implementation": "Jaeger"}, use_asyncio=True)
        runtime.enable_tracing = True

    loop = asyncio.get_event_loop()
    loop.set_exception_handler(global_exception_handler)
    task = loop.create_task(runtime.start_asyncio())

    if args.profiling:
        runtime.enable_yappi = True
        import yappi
        yappi.set_clock_type("WALL")
        with yappi.run():
            loop.run_until_complete(task)

        yappi.get_func_stats().print_all(columns={
            0: ("name", 140),
            1: ("ncall", 8),
            2: ("tsub", 8),
            3: ("ttot", 8),
            4: ("tavg", 8)
        })
    else:
        loop.run_until_complete(task)

if __name__ == '__main__':
    QUEUE_NAME = "echo-processor"
    launch()

    """
    Dynamically launch Lambda by specifying module:handler on command line, e.g.

    PYTHONPATH=.. python3 lambda_runtime_asyncio.py text_to_bytes_by_value.text_to_bytes_asyncio
    """
    
