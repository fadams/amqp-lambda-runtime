# aioboto3lite
This is a library that uses [aiohttp](https://github.com/aio-libs/aiohttp) or [aiosonic](https://github.com/sonic182/aiosonic) to talk more directly to the S3 REST API, whilst exposing the same API as [aioboto3](https://github.com/terrycain/aioboto3).

Note that aioboto3lite is *very much* **proof of concept** and only supports a limited set of S3 CRUD calls like put_object, get_object, create_bucket, delete_bucket etc. The library also provides a facade giving client side S3 semantics for Redis, so clients can use Redis as a transient object store using aioboto3 S3 API calls simply by using a `redis://<host>:<port>` endpoint_url.

The premise of this library is that whilst botocore, boto3, aiobotocore, aioboto3 etc. are incredibly useful and powerful SDKs, a key design goal of those is to provide a very general SDK that can be dynamically generated from a JSON model, and so can support new AWS services almost as soon as they are available. The down-side of that design goal is that in order to be general there are often many layers of indirection and delegation between the SDK API methods like s3.get_object to the underlying HTTP REST API call.

By contrast, the aim of this library is to illustrate the performance difference that taking a more "direct" route from the SDK method to API invocation might make. It also aims to illustrate how different HTTP client libraries might make a difference. With aiobotocore we only have the option of using aiohttp so with this library we also include support for aiosonic.

In general the performance difference can be profound. Using aiosonic put_object appears to have more than 2.5x the throughput of aioboto3 and get_object appears to have more than 4.5x the throughput.

In addition to increased performance this library has a tiny footprint when
compared with aioboto3 (and all of its dependencies)

The *significant* down-side of this approach, however, is "sustainability". That is to say this proof of concept only supports a fairly limited set of S3 CRUD operations at the moment and although it is *relatively* straightforward to add new methods, and it would be possible to make things more generic and even JSON model driven, there's a danger that that could make things evolve into a slightly "dodgy" boto clone.

Whilst the underlying premise of improving performance by optimising the path from SDK method to API call is sound, ultimately perhaps only a handful of methods *actually* benefit from such an optimisation. Many SDK methods are used for setup/teardown and relatively few (like s3.get_object/s3.put_object/ sfn.start_execution/lambda.invoke etc.) are actually likely to be used on an application's "critical path". So perhaps a better (more sustainable) approach might be to start with aioboto3/aiobotocore and "inject" optimised SDK methods to replace the handful that would really benefit from such optimisation.