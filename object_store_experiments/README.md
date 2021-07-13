# object_store_experiments
This directory illustrates a range of different approaches for efficiently integrating with AWS S3 compatible object stores.

### Prerequisites
For the examples in this directory an AWS S3 compatible object store is required. For simplicity the examples have been set up to use minio, though it is relatively straightforward to modify them to use a different object store.

The script [minio.sh](../minio.sh) runs a single node minio server in a Docker container. Note that this exposes minio on port 9001 not 9000 simply because the author happens to be using 9000 for another application, but the examples in this section are set with an endpoint_url of localhost:9001.

For simplicity the minio.sh script has sample values set for MINIO_ROOT_USER/MINIO_ROOT_PASSWORD, which should be immediately changed.

The boto3/aioboto3 sessions used to create the S3 client may be configured in the usual way using AWS environment variables like AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY or the ~/.aws/credentials file. The code will automatically look for a profile named after the user's username or alternatively a profile named minio, e.g.:
```
[minio]
aws_access_key_id = <MINIO_ROOT_USER>
aws_secret_access_key = <MINIO_ROOT_PASSWORD>
```
substituting the MINIO_ROOT_USER/MINIO_ROOT_PASSWORD values for those used to configure minio (and omitting the chevrons).

Be aware that the examples in this section attempt to create a bucket named after the example (with underscores replaced by hyphens) and after completion the objects created during the test run are deleted then the bucket is deleted.

### Blocking S3 put/get
Most Python examples online illustrate S3 access using boto3, so that is where we shall begin too:
```
PYTHONPATH=.. python3 blocking_s3_put_get.py
```
Assuming that minio (or another object store) is running and available at `http://localhost:9001` (which is the endpoint_url used by the examples) we should see the example running a number of put_object iterations followed by get_object iterations.

If minio is not correctly set up we are likely to see an error similar to the following:
```
EndpointConnectionError: Could not connect to the endpoint URL: "http://localhost:9001/blocking-s3-put-get
```
It should be immediately clear that the throughput of this example, especially for put_object, is not exactly stellar but we appear to be doing nothing *obviously* "wrong" and the core put_object loop is simply:
```
for i in range(ITERATIONS):
    s3_uri = f"s3://{bucket_name}/{uuid.uuid4()}"
    put_object(s3, s3_uri, content)
```
The issue turns out to be the usual suspects of "blocking" and "network latency", where each iteration results in a network request to minio/S3 that blocks until the server returns a success response. The throughput of put_object is made even worse than get_object because we must also factor in disk IO latency into the round-trip.

### ThreadPoolExecutor S3 put/get
As we have seen, naively using boto3 and doing blocking API calls can become a major system bottleneck. One way to potentially overcome this and the way that seems to be illustrated most often in online tutorials (e.g. https://github.com/boto/boto3/issues/2567) is to use a ThreadPoolExecutor to improve concurrency.

To try this out run:
```
PYTHONPATH=.. python3 thread_pool_executor_s3_put_get.py
```
In this case the core put_object loop is doing:
```
with concurrent.futures.ThreadPoolExecutor(MAX_WORKERS) as executor:
    for i in range(ITERATIONS):
        s3_uri = f"s3://{bucket_name}/{uuid.uuid4()}"
        executor.submit(put_object, s3, s3_uri, content)
```
By improving the concurrency we do see improved throughput for put_object, but the throughput for get_object is actually *worse* than the basic blocking version.

The most likely hypothesis for this is that in order to get good put_object throughput a large number of workers are required and so lock contention is almost certainly a problem, because the actual "work" being done by each executor (put_object) is fairly trivial.

### asyncio ThreadPoolExecutor S3 put/get
Because of its blocking nature it is not advisable to *directly* use boto3 in an asyncio application.

The standard way to wrap blocking calls to make them awaitable for asyncio is to use loop.run_in_executor https://docs.python.org/3/library/asyncio-eventloop.html#executing-code-in-thread-or-process-pools.

To try this out run:
```
PYTHONPATH=.. python3 asyncio_thread_pool_executor_s3_put_get.py
```
In this case the core put_object loop is doing:
```
# Coroutine to launch s3.put_object calls in an executor
async def non_blocking_put():
    with concurrent.futures.ThreadPoolExecutor(MAX_WORKERS) as executor:
        tasks = []
        for i in range(ITERATIONS):
            s3_uri = f"s3://{bucket_name}/{uuid.uuid4()}"    
            tasks.append(loop.run_in_executor(executor, put_object, s3, s3_uri, content))

        await asyncio.gather(*tasks)
```
This is actually very similar to the other ThreadPoolExecutor example, though unfortunately the performance is slightly worse because in addition to the likely lock contention issue there are additional overheads involved in bridging to asyncio.

### aioboto3 S3 put/get
The asyncio ThreadPoolExecutor example primarily serves as a cautionary tale that not all asyncio libraries are created equal.

It is actually quite common to see Python libraries labelled "asyncio" when they are actually using executors to wrap an underlying blocking library. Whilst technically this *is* still legitimately asyncio, it is highly unlikely to give comparable performance to a *native* asyncio library built on top of non-blocking IO.

A far better way for asyncio applications to integrate with AWS services is to use [aioboto3](https://github.com/terrycain/aioboto3) and [aiobotocore](https://github.com/aio-libs/aiobotocore), which are *natively* asyncio, using [aiohttp](https://github.com/aio-libs/aiohttp) as the underlying HTTP client.

To try this out run:
```
PYTHONPATH=.. python3 aioboto3_s3_put_get.py
```
One particular point of note when using aioboto3, or indeed any other asyncio library that closely mirrors its blocking equivalent, is that it can be **very** tempting to write code like:
```
for i in range(ITERATIONS):
    s3_uri = f"s3://{bucket_name}/{uuid.uuid4()}"
    await put_object(s3, s3_uri, content)
```
In other words just adding await in front of put_object. This naive approach will actually run quite happily and is included in the example, but the throughput is basically almost exactly the same as the blocking boto3 version.

When one examines what is going on the reason for this is actually quite obvious, as although we are not *blocking* it **is** still necessary to await the result of each iteration before proceeding to the next, so despite using asyncio this naive example has not actually increased the amount of concurrency.

The *correct* way to use aioboto3 is to refactor into Tasks, where the resulting code is more analogous to the earlier executor examples:
```
for i in range(ITERATIONS):
    s3_uri = f"s3://{bucket_name}/{uuid.uuid4()}"
    tasks.append(put_object(s3, s3_uri, content))
    if len(tasks) == MAX_CONNECTIONS:
        await asyncio.gather(*tasks)
        tasks = []
```
With this approach we periodically run asyncio.gather(), which has the effect of scheduling the awaitables as Tasks and running them concurrently.

By having a concurrency equivalent to MAX_CONNECTIONS (which is 1000) we can achieve *significant* throughput improvements of around 2.85x the best put_object results we achieved with the ThreadPoolExecutor and over 2.5x the get_object results, with significantly reduced CPU utilisation too.

### aiohttp S3 put/get
Using aioboto3 with an appropriate level of concurrency offers significant performance advantages over regular boto3 in asyncio applications. If we profile the application however, we may see that there are many overheads due to the general purpose nature of aioboto3/boto3 and the layers of delegation that entails.

By "working backwards" and talking more directly to the S3 REST API we can achieve orders of magnitude greater throughput than blocking boto3.

The approach taken  in the aiohttp_s3_put_get example is to create a library [aioboto3lite](../aioboto3lite) that allows the application to "look like" an aioboto3 application by "aliasing" the import as follows:
```
import aioboto3lite as aioboto3
import aioboto3lite as aiobotocore
#import aiobotocore, aioboto3  # Uncomment this line to use the real aioboto3/aiobotocore
```
The [aioboto3lite](../aioboto3lite) directory describes the library in more detail, but in essence it uses [aiosonic](https://github.com/sonic182/aiosonic) by default or [aiohttp](https://github.com/aio-libs/aiohttp) optionally as an asyncio HTTP client library to talk directly to the S3 REST API, using optimised header signing and bypassing many of the layers of delegation between the SDK method invocation and the underlying HTTP request.

To try this out run:
```
PYTHONPATH=.. python3 aiohttp_s3_put_get.py
```
Compared with the original blocking boto3 example we can see 75x the original put_object result and more than 12x the original get_object result. These results are also 2.65x the aioboto3 put_object result and  4.7x the aioboto3 get_object result. The actual throughput bottleneck now appears to be our single instance minio server and not the API cost nor network latency.

### blocking Redis put/get
For temporary/transient workloads an alternative approach to object storage is to use Redis, noting that Redis is an in-memory store so we need to be mindful of retention policies and/or object time to live.

One of the biggest advantages of Redis is that it has a very lightweight wire protocol compared with HTTP, so the API overheads are significantly reduced given equivalent sized objects. For small objects using Redis can offer very significant improvements in throughput.

The best way to illustrate the benefits of a lightweight wire protocol is to first consider the blocking example which conceptually behaves in a similar way to the blocking S3 example where a call is made that blocks until the server has signalled success.

To try this example we must have a running Redis server. The script [redis.sh](../redis.sh) runs a single node Redis server in a Docker container exposed on port 6379.
```
PYTHONPATH=.. python3 blocking_redis_put_get.py
```
Compared with the blocking boto3 example we can see 280x the original put_object result and more than 25x the original get_object result.

### asyncio Redis put/get
If we use Redis with asyncio to improve the available concurrency we can achieve even more impressive results:
```
PYTHONPATH=.. python3 aioredis_put_get.py
```
Compared with the blocking boto3 example we can see around 600x the original put_object result and more than 57x the original get_object result.

Note that this example uses Redis directly, however the  [aioboto3lite](../aioboto3lite) library has implemented an S3 client facade around Redis so that it is actually possible to use aioboto3 semantics by specifying a `redis://<host>:<port>` URI as the endpoint_url.

We may see this by running:
```
PYTHONPATH=.. python3 aiohttp_s3_put_get_redis.py
```
This example is *exactly* the same as aiohttp_s3_put_get.py, but has the Redis endpoint_url uncommented so it uses the Redis client facade instead of HTTP/S3.
