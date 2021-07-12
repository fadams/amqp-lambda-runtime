# text_to_bytes_by_value
This directory contains a simple Lambda that processes a request containing some text passed by value and extracts the bytes from the text then base64 encodes and passes the encoded binary by value as part of the response. It is intended to illustrate a fairly common Lambda pattern to gauge round-trip throughput.

The Lambda attempts to import [pybase64](https://github.com/mayeut/pybase64) for improved base64 performance, though it will fall back to standard library base64 if pybase64 is not available.

To launch the Lambda run the following (from the lambda_runtime directory):
```
PYTHONPATH=.. python3 lambda_runtime_asyncio.py text_to_bytes_by_value.text_to_bytes_asyncio
```
or similarly for the blocking version:
```
PYTHONPATH=.. python3 lambda_runtime_blocking.py text_to_bytes_by_value.text_to_bytes_blocking
```
To run the client run the following from the text_to_bytes_by_value directory.
```
PYTHONPATH=.. python3 text_to_bytes_client_asyncio.py
```
As with the basic echo example this client initially *appears* to do very little as it is timing a large number of iterations and as with the echo example the easiest way to check that it is actually doing *something* is to log in to the RabbitMQ GUI.