# text_to_bytes_by_reference
The code in this directory builds on the [object_store_experiments](../object_store_experiments) and contains a Lambda and client to process  requests containing some text passed by *reference*.

In this example the client places the data to be processed into an object store, then invokes the Lambda by passing a reference to the item (in the form of an S3 URI). The Lambda uses the reference to retrieve the item from the object store, then processes it and places the result into the object store before passing the reference to the result back to the client, which in turn uses that reference to retrieve the result data.

Pass by reference is a common pattern and is recommended by AWS, particularly in Stepfunctions, instead of passing large payloads https://docs.aws.amazon.com/step-functions/latest/dg/avoid-exec-failures.html.

Like the [object_store_experiments](../object_store_experiments) the examples in this directory an AWS S3 compatible object store is required. For simplicity the examples have been set up to use minio, though it is relatively straightforward to modify them to use a different object store.

The script [minio.sh](../minio.sh) runs a single node minio server in a Docker container. Note that this exposes minio on port 9001 not 9000 simply because the author happens to be using 9000 for another application, but the examples in this section are set with an endpoint_url of localhost:9001.

To launch the Lambda run the following (from the lambda_runtime directory):
```
PYTHONPATH=.. python3 lambda_runtime_asyncio.py text_to_bytes_by_reference.text_to_bytes_asyncio
```
or similarly for the blocking version:
```
PYTHONPATH=.. python3 lambda_runtime_blocking.py text_to_bytes_by_reference.text_to_bytes_blocking
```
To run the client run the following from the text_to_bytes_by_reference directory.
```
PYTHONPATH=.. python3 text_to_bytes_client_asyncio.py
```
As with the aiohttp_s3_put_get example in the object_store_experiments it is easy to switch between aioboto3 and aioboto3lite (which is the default) as the code simply aliases the imports.
```
import aioboto3lite as aioboto3
import aioboto3lite as aiobotocore
#import aiobotocore, aioboto3  # Uncomment this line to use the real aioboto3/aiobotocore
```
### Retaining the aioboto3 Session across invocations
The example here illustrate a subtle complication that arises when using aioboto3.

A fairly common pattern when using Lambda is to declare the S3 client outside of the lambda_handler so that (relatively expensive) session and client creation can be done when the module is loaded rather than on every Lambda invocation. For blocking code this is as simple as declaring the following in the main body of the module:
```
session = create_configured_session(boto3)
config = botocore.config.Config(max_pool_connections=MAX_CONNECTIONS)
s3 = session.client("s3", endpoint_url="http://localhost:9001", config=config)
```
With aioboto3 however it is not quite as simple as that because since aioboto3 v8.0.0+, client and resource objects are now async context managers and a normal pattern might do:
```
session = create_configured_session(aioboto3)
client = session.client("s3", endpoint_url="http://localhost:9001")
async with client as s3:
    await s3.create_bucket(bucket_name)
```
If we want to retain the client such that it may be invoked from a different scope (like a lambda_handler) then we must create an AsyncExitStack, which essentially does `async with` on the context manager returned by client or resource, and saves the exit coroutine so that it can be called later to clean up. The approach is described in the aioboto3 documentation: https://aioboto3.readthedocs.io/en/latest/usage.html#aiohttp-server-example.

To achieve this we have the following in the body of the module:
```
async def create_aioboto3_client():
    session = create_configured_session(aioboto3)
    config = aiobotocore.config.AioConfig(max_pool_connections=MAX_CONNECTIONS)
    #config.http_client = "aiohttp"  # Defaults to "aiosonic"
    s3 = await context_stack.enter_async_context(
        session.client("s3", endpoint_url="http://localhost:9001", config=config)
        #session.client("s3", endpoint_url="redis://localhost:6379", config=config)
    )
    return s3

MAX_CONNECTIONS = 1000
context_stack = contextlib.AsyncExitStack()
loop = asyncio.get_event_loop()
s3 = loop.run_until_complete(create_aioboto3_client())
```
So we create an AsyncExitStack and launch the create_aioboto3_client() coroutine when the module loads by using loop.run_until_complete(). That returns the S3 client object by entering the context stack (and thus doing the equivalent of `async with`) and we later implement an exit_handler to tidy up aioboto3 on exit:
```
"""
Tidy up aioboto3 client on exit.
This is made slightly awkward as we need an async
exit handler to await the AsyncExitStack aclose()
"""
async def async_exit_handler():
    await context_stack.aclose()  # After this the s3 instance should be closed

@atexit.register
def exit_handler():
    # Run async_exit_handler
    loop.run_until_complete(async_exit_handler())
```
This example also illustrates another subtlety when using asyncio with AWS Lambda. The AMQP Lambda runtime actually does directly support a coroutine lambda_handler e.g.:
```
async def lambda_handler(event, context):
    # Handler body
```
However, real AWS Lambda does **not** yet (directly) support coroutine Lambda handlers in Python, but the following pattern may be used to launch a handler coroutine as a Task on AWS Lambda:
```
async def async_lambda_handler(event, context):
    print("async_lambda_handler")
    return event

def lambda_handler(event, context):
    if loop.is_running:
        return loop.create_task(async_lambda_handler(event, context))
    else:
        return loop.run_until_complete(async_lambda_handler(event, context))
```
The AMQP Lambda runtime also supports using that pattern, but it's simpler to just define the Lambda handler as a coroutine if that's what we need.