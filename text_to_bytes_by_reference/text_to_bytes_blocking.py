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
# PYTHONPATH=.. python3 text_to_bytes_blocking.py
# PYTHONPATH=.. LOG_LEVEL=DEBUG python3 text_to_bytes_blocking.py
#
# Or dynamically launch from base processor/Lambda runtime e.g
# PYTHONPATH=.. python3 lambda_runtime_blocking.py text_to_bytes_by_reference.text_to_bytes_blocking
#
# With Jaeger tracing enabled
# PYTHONPATH=.. python3 text_to_bytes_blocking.py -t
#
# With Yappi profiling enabled
# PYTHONPATH=.. python3 text_to_bytes_blocking.py -p > profile.txt
#
# Yappi is very intrusive for blocking processor, but cProfile can be used
# PYTHONPATH=.. python3 -m cProfile -s tottime text_to_bytes_blocking.py > profile.txt
#
# py-spy can be used too
# PYTHONPATH=.. py-spy top -- python3 text_to_bytes_blocking.py
# PYTHONPATH=.. py-spy record -o profile.svg -- python3 text_to_bytes_blocking.py
#
"""
Pass by reference text_to_bytes_blocking based on the original text_to_bytes.
In this example the actual payload is stored in S3 and a reference to it is
passed in the event.
"""

import sys
assert sys.version_info >= (3, 6) # Bomb out if not running Python3.6

import time, uuid
import botocore, boto3

from utils.logger import init_logging
from object_store_experiments.s3_utils_asyncio import (
    create_configured_session,
    parse_s3_uri
)

# Not necessary if dynamically launched from base processor/Lambda runtime
from lambda_runtime.lambda_runtime_blocking import launch

MAX_CONNECTIONS = 1000
"""
Creates boto3.Session() configured from environment variables or users's
profile or minio profile. The easiest way to use with minio is to add the
following to ~/.aws/credentials (setting the key_id and key used to set
MINIO_ROOT_USER and MINIO_ROOT_PASSWORD.

[minio]
aws_access_key_id = <id>
aws_secret_access_key = <key>
"""
# TODO endpoint_url should be configurable, probably via env var
session = create_configured_session(boto3)
config = botocore.config.Config(max_pool_connections=MAX_CONNECTIONS)
s3 = session.client("s3", endpoint_url="http://localhost:9001", config=config)

QUEUE_NAME = "text-to-bytes"

def lambda_handler(event, context):    
    response = []
    edh = event["_edh"]
    if "content" in event:
        for content_block in event["content"]:
            if "text" in content_block:  # Pass by value
                text = content_block["text"]
                #text_bytes = bytearray(text, "utf-8")
                text_bytes = bytes(text, "utf-8")
                id = str(uuid.uuid4())
                item = {
                    "id": id,
                    "content": base64.b64encode(text_bytes).decode("utf-8"),
                    "output": edh
                }
                response.append(item)
            elif "text-ref" in content_block:  # Pass by reference
                uri = content_block["text-ref"]
                """
                The by-reference text-to-bytes is vaguely pointless as the
                item is stored in the object store as bytes, so getting it
                then converting to a string only to get the bytes of the string
                is, well, weird.The point of the exercise is really just to
                illustrate the mechanics of a pass-by-reference Lambda invocation
                where we invoke the Lambda, passing in a reference to data that
                is actually stored in an S3 object store.
                """
                bucket, key = parse_s3_uri(uri)

                # First get get_object response from s3
                obj = s3.get_object(Bucket=bucket, Key=key)

                # Then read actual object into a string from StreamingBody response.
                stream = obj["Body"]
                raw = stream.read()
                text = raw.decode("utf-8")
                text_bytes = bytes(text, "utf-8")
                
                #print(text_bytes)

                # Create new S3 URI for "converted" items to send back to requestor
                id = str(uuid.uuid4())
                uri = f"s3://{bucket}/{id}"
                #print(uri)
                s3.put_object(Body=text_bytes, Bucket=bucket, Key=id)

                item = {
                    "id": id,
                    "content": [{"text-ref": uri}],
                    "output": edh
                }
                response.append(item)

    return response

if __name__ == '__main__':
    launch()

    """
    Or dynamically launch from base processor/Lambda runtime e.g
    PYTHONPATH=.. python3 lambda_runtime_blocking.py text_to_bytes_by_reference.text_to_bytes_blocking
    """

