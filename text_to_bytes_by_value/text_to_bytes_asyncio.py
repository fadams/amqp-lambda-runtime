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
# PYTHONPATH=.. python3 text_to_bytes_asyncio.py
# PYTHONPATH=.. LOG_LEVEL=DEBUG python3 text_to_bytes_asyncio.py
#
# Or dynamically launch from base processor/Lambda runtime e.g
# PYTHONPATH=.. python3 lambda_runtime_asyncio.py text_to_bytes_by_value.text_to_bytes_asyncio
#
# With Jaeger tracing enabled
# PYTHONPATH=.. python3 text_to_bytes_asyncio.py -t
#
# With Yappi profiling enabled
# PYTHONPATH=.. python3 text_to_bytes_asyncio.py -p > profile.txt
#
# py-spy can be used too
# PYTHONPATH=.. py-spy top -- python3 text_to_bytes_asyncio.py
# PYTHONPATH=.. py-spy record -o profile.svg -- python3 text_to_bytes_asyncio.py
#
"""
Pass by value text_to_bytes_asyncio based on the original text_to_bytes below.
Because we're converting the input string to a bytes sequence, in order to do
pass by value we need to base64 encode the text_bytes to attach to our JSON
response.

This is very much an experimental "what if" in order to get more of a feel for
the sort of throughput achievable using a fairly minimalist Message RPC approach
and also to get a feel for differences between Pika asyncio and blocking
connection and the sort of overheads caused by tracing etc.

With standard library json and base64 throughput is around 5475 x 5KB items/s
With ujson and pybase64 throughput is around 5962 x 5KB items/s
With ujson and pybase64 throughput is around 5145 x (2 x 5KB) items/s
With ujson and pybase64 throughput is around 3182 x (5 x 5KB) items/s

try:
    data = json.loads(bin)
    bin_length = len(bin)
    bin = None
except Exception as e:  # Probably ValueError
    pass

text_lengths_bytes = []
if "content" in data:
    for content_block in data["content"]:
        if "text" in content_block:
            text = content_block["text"]
            text_bytes = bytearray(text, "utf-8")
            text_lengths_bytes.append(len(text_bytes))
            del content_block["text"]
            content_block["_edh"] = data["_edh"]
            id = str(uuid.uuid4())

            with data_object.get_child_data_output_stream(id, len(text_bytes), content_block)[0] as f:
                f.write(text_bytes)

    del data["content"]

data_object.set_output_metadata(data)

return data_object
"""

import sys
assert sys.version_info >= (3, 6) # Bomb out if not running Python3.6

import asyncio, time, uuid

from utils.logger import init_logging

# Not necessary if dynamically launched from base processor/Lambda runtime
from lambda_runtime.lambda_runtime_asyncio import launch  # asyncio Lambda runtime

"""
Attempt to use pybase64 libbase64 based codec if available
pip3 install pybase64
https://github.com/mayeut/pybase64
https://github.com/aklomp/base64
"""
try:
    import pybase64 as base64
except:  # Fall back to standard library base64
    import base64


QUEUE_NAME = "text-to-bytes"

def lambda_handler(event, context=None):
    response = []
    edh = event["_edh"]
    if "content" in event:
        for content_block in event["content"]:
            if "text" in content_block:
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

    return response

if __name__ == '__main__':
    launch()

