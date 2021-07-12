# lambda_runtime

This directory contains the main AMQP RPC Lambda runtime code.

The blocking and asyncio runtimes include a trivial default lambda_handler that simply echoes the request back to the client and is intended to act as a benchmark to determine expected round-trip RPC throughput. The runtimes provide CLI options to enable Python modules containing more useful lambda_handlers to be dynamically loaded on startup.

The directory contains blocking and asyncio versions of the runtime and a simple echo client that may be used to try them out.

### Quickstart
The runtime expects a running RabbitMQ broker to be available. The [rabbitmq-broker.sh](../rabbitmq-broker.sh) script will run a broker via Docker.

To see the main CLI options:
```
PYTHONPATH=.. python3 lambda_runtime_asyncio.py -h
```
which should return:
```
usage: lambda_runtime_asyncio.py [-h] [-u URL] [-q QUEUE] [-t] [-p]
                                 [<module>:<lambda_handler>]

Lambda runtime using asyncio AMQP Message RPC invocations. Connects to AMQP
queue, receives RPC request messages, decodes based on content_type
("application/octet-stream" = raw binary, "application/json" = JSON), invokes
"lambda_handler" method using decoded event, encodes response from, handler
based on return type, sends RPC response message using correlation_id and
reply_to from request message. Default lambda_handler simply echoes request
event.

positional arguments:
  <module>:<lambda_handler>
                        The name of the module and/or Lambda handler to run

optional arguments:
  -h, --help            show this help message and exit
  -u URL, --url URL     AMQP broker connection URL of the form: amqp://[<user>
                        :<pass>@]host[:port][/<virtualhost>][?<option>=<value>
                        [&<option>=<value>]*]
  -q QUEUE, --queue QUEUE
                        The queue the Lambda runtime should bind to
  -t, --tracing         Enable Jaeger tracing
  -p, --profiling       Enable Yappi profiling

```
The default AMQP connection URL is:
```
amqp://localhost:5672?connection_attempts=20&retry_delay=10&heartbeat=0
```
which may be easily changed via the `-u` option.

The queue that a runtime instance receives RPC requests on is configured by setting the QUEUE_NAME variable in the module hosting the lambda_handler, or alternatively this may be set or overridden via the `-q` CLI option.

To use either of the runtimes to launch another Lambda definition, either import
```
from lambda_runtime.lambda_runtime_asyncio import launch
```
into the module and include:
```
if __name__ == '__main__':
    launch()
```
or, alternatively, dynamically load the module, e.g.:
```
PYTHONPATH=.. python3 lambda_runtime_asyncio.py text_to_bytes_by_value.text_to_bytes_asyncio
```
By default the runtime invokes a handler called lambda_handler with a signature as follows:
```
# Default lambda_handler, simply echoes input event
def lambda_handler(event, context):
    # In a real Lambda **** DO WORK HERE ****
    return event  # Default to simple echo processor
```
however this may be overridden by specifying the required handler name with the module name in the form:
```
<module>:<lambda_handler>
```
So to run the simple echo processor provided by lambda_runtime_asyncio simply run:
```
PYTHONPATH=.. python3 lambda_runtime_asyncio.py
```
or similarly for the blocking version:
```
PYTHONPATH=.. python3 lambda_runtime_blocking.py
```
All being well this should log a successful startup with messages similar to the following:
```
[2021-07-12 11:23:38,861] INFO     - lambda_runtime_asyncio : Using ujson parser
[2021-07-12 11:23:38,861] INFO     - lambda_runtime_asyncio : Using uvloop event loop
[2021-07-12 11:23:38,862] INFO     - amqp_0_9_1_messaging_async : Creating Connection with url: amqp://localhost:5672?connection_attempts=20&retry_delay=10&heartbeat=0
[2021-07-12 11:23:38,862] INFO     - amqp_0_9_1_messaging_async : Opening Connection to localhost:5672
[2021-07-12 11:23:38,869] INFO     - amqp_0_9_1_messaging_async : Creating Session
[2021-07-12 11:23:38,871] INFO     - amqp_0_9_1_messaging_async : Creating Consumer with address: echo-processor; {"node": {"auto-delete": true}}
[2021-07-12 11:23:38,877] INFO     - amqp_0_9_1_messaging_async : Creating Producer with address: 
```
All being well this may now be tested using the echo client:
```
PYTHONPATH=.. python3 echo_client_asyncio.py
```
This should log similar messages to the runtime, but it will otherwise initially *appear* to do very little else as it is simply timing a large number (1000000) of invocations. The easiest way to check it is actually *doing something* is to log in to the RabbitMQ GUI (on port 15672) and check the `echo-processor` queue for activity.
