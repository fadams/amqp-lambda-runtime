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
# PYTHONPATH=.. python3 lambda_runtime_blocking.py
# PYTHONPATH=.. LOG_LEVEL=DEBUG python3 lambda_runtime_blocking.py
#
# With Jaeger tracing enabled
# PYTHONPATH=.. python3 lambda_runtime_blocking.py -t
#
# With Yappi profiling enabled
# PYTHONPATH=.. python3 lambda_runtime_blocking.py -p > profile.txt
#
# Yappi is very intrusive for blocking lambda_runtime, but cProfile can be used
# PYTHONPATH=.. python3 -m cProfile -s tottime lambda_runtime_blocking.py > profile.txt
#
# py-spy can be used too
# PYTHONPATH=.. py-spy top -- python3 lambda_runtime_blocking.py
# PYTHONPATH=.. py-spy record -o profile.svg -- python3 lambda_runtime_blocking.py
#
"""
Lambda runtime using blocking connection AMQP Message RPC invocations.
Connects to AMQP queue, receives RPC request messages, decodes based on
content_type ("application/octet-stream" = raw binary, "application/json" = JSON),
invokes "lambda_handler" method using decoded event, encodes response from,
handler based on return type, sends RPC response message using correlation_id
and reply_to from request message.

Default lambda_handler simply echoes request event.
"""

import sys
assert sys.version_info >= (3, 6) # Bomb out if not running Python3.6

import argparse, importlib, time, opentracing

from utils.logger import init_logging
from utils.open_tracing_factory import create_tracer, span_context, inject_span
from utils.amqp_0_9_1_messaging import Connection, Message
from utils.messaging_exceptions import *

"""
Attempt to use ujson if available https://pypi.org/project/ujson/
"""
try:
    import ujson as json
except:  # Fall back to standard library json
    import json


# Default lambda_handler, simply echoes input event
def lambda_handler(event, context):
    # In a real Lambda **** DO WORK HERE ****
    return event  # Default to simple echo processor

#-------------------------------------------------------------------------------

class AMQPLambdaRuntime():
    def __init__(self, url, name, queue_name, lambda_handler):
        self.connection_url = url
        self.name = name
        self.queue_name = queue_name
        self.lambda_handler = lambda_handler
        self.enable_tracing = False
        self.enable_yappi = False
        # Initialise logger
        self.logger = init_logging(log_name=name)
        self.count = 0  # Counts responses to enable clean exit when profiling
        self.connection = None  # start() actually opens the Connection

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

    def invoke(self, message):
        context = {}  # TODO make this more useful
        result = self.lambda_handler(self.decode(message), context)
        return self.encode(result)

    def rpc_handler(self, message):
        try:
            reply, content_type = self.invoke(message)
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
        Acknowledges the original request. The blocking version doesn't need any
        concurrency limiting so we can improve throughput by setting multiple to
        True albeit at the expense of potential message loss.
        """
        message.acknowledge(multiple=True)

        if self.enable_yappi:
            self.count += 1
            if self.count == 10000:
                self.connection.close()  # Clean exit needed by yappi profiler

    def rpc_handler_traced(self, message):
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
                reply, content_type = self.invoke(message)
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
                Acknowledges the original request. The blocking version doesn't need any
                concurrency limiting so we can improve throughput by setting multiple to
                True albeit at the expense of potential message loss.
                """
                message.acknowledge(multiple=True)

                if self.enable_yappi:
                    self.count += 1
                    if self.count == 10000:
                        self.connection.close()  # Clean exit needed by yappi profiler

    def start(self):
        self.connection = Connection(self.connection_url)
        try:
            self.connection.open()
            session = self.connection.session()
            #session = self.connection.session(auto_ack=True)
            self.consumer = session.consumer(
                self.queue_name + '; {"node": {"auto-delete": true}}'
            )
            self.consumer.capacity = 0 # Unbounded consumer prefetch
            #self.consumer.capacity = 1000  # Bounded consumer prefetch

            if self.enable_tracing:
                self.consumer.set_message_listener(self.rpc_handler_traced)
            else:
                self.consumer.set_message_listener(self.rpc_handler)
            self.producer = session.producer()

            self.connection.start(); # Wait until connection closes.
    
        except MessagingError as e:  # ConnectionError, SessionError etc.
            self.logger.error(e)
        except Exception as e:
            self.logger.error(e)

        self.connection.close();

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

        runtime = AMQPLambdaRuntime(
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

    """
    Initialising OpenTracing here rather than in the LambdaRuntime constructor as
    opentracing.tracer is a per process object not per thread.
    """
    if args.tracing:
        create_tracer(runtime.name, {"implementation": "Jaeger"})
        runtime.enable_tracing = True

    if args.profiling:
        runtime.enable_yappi = True
        import yappi
        yappi.set_clock_type("WALL")
        with yappi.run():
            runtime.start()

        yappi.get_func_stats().print_all(columns={
            0: ("name", 140),
            1: ("ncall", 8),
            2: ("tsub", 8),
            3: ("ttot", 8),
            4: ("tavg", 8)
        })
    else:
        runtime.start()

if __name__ == '__main__':
    QUEUE_NAME = "echo-processor"
    launch()

    """
    Dynamically launch Lambda by specifying module:handler on command line, e.g.

    PYTHONPATH=.. python3 lambda_runtime_blocking.py text_to_bytes_by_value.text_to_bytes_blocking
    """
    
