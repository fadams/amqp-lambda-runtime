# amqp-lambda-runtime
Proof of concept Lambda runtime using AMQP RPC to invoke handler.

### Introduction
Messaging systems like AMQP support a Remote Procedure Call (RPC) pattern.

Doing RPC over most messaging systems is relatively easy, requiring the client to create a callback queue and pass the address of that to the server as the reply_to property of the request message. In addition, in order for clients to associate requests with their responses, a unique ID known as a correlation_id is created by the client for each request and passed as a property of the request message. When the correlation_id is passed back to the client as part of the response message it can use it to match the response with the request that triggered it.

A more detailed explanation of the pattern may be found in the RabbitMQ documentation https://www.rabbitmq.com/tutorials/tutorial-six-python.html though the pattern is a general one and agnostic of the messaging provider.

### Blocking vs asyncio
This repository actually started out as some experiments intended to gauge the relative performance of Python RPC clients and processors implemented using [Pika](https://github.com/pika/pika)'s blocking connection or asyncio connection. The [lambda_runtime](lambda_runtime) directory contains both blocking and asyncio versions, however the asyncio version offers *significantly* better performance and is recommended over the blocking version.

### Repository structure
- The [lambda_runtime](lambda_runtime) directory contains the main AMQP RPC Lambda runtime code. The runtimes include a trivial default lambda_handler that simply echoes the request back to the client and is intended to act as a benchmark to determine expected round-trip RPC throughput. The runtimes contain CLI options to enable python modules containing more useful lambda_handlers to by dynamically loaded on startup.
- The [utils](utils) directory contains library code to wrap Pika's low-level AMQP 0.9.1 calls in a more JMS-like higher level abstraction. The messaging libraries also allow the semantic differences between blocking and asyncio to be minimised by wrapping Pika's callbacks in Futures so, for the most part, the differences between the blocking and asyncio versions of the runtime are a few awaits prefixing what were blocking calls.
- The [text_to_bytes_by_value](text_to_bytes_by_value) directory contains a simple Lambda that processes a request containing some text passed by value and extracts the bytes from the text then base64 encodes and passes the encoded binary by value as part of the response. It is intended to illustrate a fairly common Lambda pattern to gauge round-trip throughput.
- The [object_store_experiments](object_store_experiments) directory illustrates a range of different approaches for efficiently integrating with AWS S3 compatible object stores. Most Python examples online illustrate using boto3, which is a great library, but blocks and can be incredibly inefficient even using mitigations like ThreadPoolExecutors. By using asyncio and talking more directly to the S3 REST API we can achieve orders of magnitude greater throughput.
- The [aioboto3lite](aioboto3lite) directory contains a proof of concept library that uses [aiohttp](https://github.com/aio-libs/aiohttp) or [aiosonic](https://github.com/sonic182/aiosonic) to talk more directly to the S3 REST API, whilst exposing the same API as [aioboto3](https://github.com/terrycain/aioboto3). Note that aioboto3lite is *very much* proof of concept and only supports as limited set of S3 CRUD calls like put_object, get_object, create_bucket, delete_bucket etc. The library also provides client side S3 semantics for Redis, so clients can use Redis as a transient object store using aioboto3 S3 API calls.
- The [text_to_bytes_by_reference](text_to_bytes_by_reference) directory builds on the object_store_experiments and contains a Lambda and client to process  requests containing some text passed by *reference*. In this example the client places the data to be processed into an object store then invokes the lambda passing a reference to the item (in the form of an S3 URI). The processor uses the reference to retrieve the item from the object store then processes it and places the result into the object store before passing the reference to the result back to the client, which in turn uses that reference to retrieve the result data. Pass by reference is a common pattern and is recommended by AWS, particularly in Stepfunctions, instead of passing large payloads https://docs.aws.amazon.com/step-functions/latest/dg/avoid-exec-failures.html.
- The repository also contains some simple shell scripts for launching a rabbitmq-broker, redis and the minio S3 compatible object store using Docker.

### Dependencies
In order to make full use of this repository Docker is required in order to use the scripts to launch rabbitmq-broker, redis and minio, though any instance of those applications could be used.

The code requires Python 3.6 as a minimum, preferably 3.8. A number of Python packages are used:

- pika - pip3 install pika
- ujson (provides improved JSON performance, but code in this repo will fall-back to standard library json) - pip3 install ujson
- pybase64 (provides improved base64 performance, but code in this repo will fall-back to standard library base64) - pip3 install pybase64
- uvloop (provides improved asyncio event loop performance, but code in this repo will fall-back to standard library asyncio event loop) - pip3 install uvloop
- opentracing - pip3 install opentracing
- jaeger - pip3 install jaeger
- aiohttp - pip3 install aiohttp
- aiosonic (alternative asyncio HTTP client) - pip3 install aiosonic
- boto3 - pip3 install aiohttp
- aioboto3 - pip3 install aioboto3
- aioredis - pip3 install aioredis==2.0.0a1 (Important: aioredis 2.0 should be used, which is different from previous versions of aioredis and is basically an asyncio reimplementation of redis-py, see https://github.com/aio-libs/aioredis-py/issues/930)
