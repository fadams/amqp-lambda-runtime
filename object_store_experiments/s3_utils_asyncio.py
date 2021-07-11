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
# PYTHONPATH=.. python3 blocking_s3_put_get.py
#
"""
asyncio version of boilerplate for some s3 utility code.

create_configured_boto3_session groks environment variables and profile to
create a Session configured with the required AWS access key ID/password/token

The most interesting is probably delete_bucket() which first deletes all the
objects in the bucket by first listing using a Paginator then iterating over
the pages to retrieve the maximum result set to enable batched object deletes.

The get_object/put_object wrappers parse s3 URI of the form  s3://<bucket>/<key>
but the main use is to make it easy to launch those calls from Executors
"""

import sys
assert sys.version_info >= (3, 8) # Bomb out if not running Python3.8

import asyncio, os
from utils.logger import init_logging

logger = init_logging(log_name=__name__)

def create_configured_session(module):
    """
    boto3.client() doesn't have a (direct) way to set profile_name
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session.client
    so we first create a Session instance and use that to create the client.
    The precedence below is taken from from
    https://docs.aws.amazon.com/cli/latest/topic/config-vars.html#id1
    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY
    AWS_PROFILE
    """
    name = module.__name__
    aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    aws_session_token = os.environ.get("AWS_SESSION_TOKEN")
    aws_profile = os.environ.get("AWS_PROFILE")

    if aws_access_key_id and aws_secret_access_key:
        logger.info(f"Creating {name} Session from AWS_ACCESS_KEY_ID env var")
        session = module.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token
        )
    else:
        available_profiles = module.Session().available_profiles
        profile = aws_profile if aws_profile else os.getlogin()
        profile = profile if profile in available_profiles else "minio"
        if profile in available_profiles:
            logger.info(f"Creating {name} Session from profile: {profile}")
            session = module.Session(profile_name=profile)
        else:
            logger.info(f"Creating default {name} Session")
            session = module.Session()
    return session

def parse_s3_uri(s3_uri):
    """
    Given a URI of the form s3://<bucket>/<key> parse into bucket, key tuple
    For a trivial parse this is faster than using urlparse(s3_uri)
    """
    return s3_uri.replace("s3://", "").split("/", 1)

async def get_object(s3, s3_uri):
    bucket, key = parse_s3_uri(s3_uri)
    try:
        # First get get_object response from s3
        obj = await s3.get_object(Bucket=bucket, Key=key)
        # Then read actual object from StreamingBody response.
        # https://botocore.amazonaws.com/v1/documentation/api/latest/reference/response.html#botocore.response.StreamingBody
        # Note aioboto3 wraps stream in context manager
        # https://aioboto3.readthedocs.io/en/latest/usage.html#streaming-download
        # TODO make exception handling more useful
        async with obj["Body"] as stream:
            value = await stream.read()
            return value
    except s3.exceptions.NoSuchBucket as e:
        logger.info(e)
    except s3.exceptions.NoSuchKey as e:
        logger.info(e)
    except Exception as e:
        code = type(e).__name__
        messsage = f"get_object caused unhandled exception: {code}: {str(e)}"
        logger.error(messsage)
        raise e
        #return b""

async def put_object(s3, s3_uri, body):
    bucket, key = parse_s3_uri(s3_uri)
    # TODO make exception handling more useful
    try:
        await s3.put_object(Body=body, Bucket=bucket, Key=key)
    except s3.exceptions.NoSuchBucket as e:
        logger.info(e)
    except Exception as e:
        code = type(e).__name__
        messsage = f"put_object caused unhandled exception: {code}: {str(e)}"
        logger.error(messsage)

async def create_bucket(s3, bucket_name):
    try:
        logger.info("Creating bucket {} for running test".format(bucket_name))
        response = await s3.create_bucket(
            Bucket=bucket_name
        )
    except s3.exceptions.BucketAlreadyExists as e:
        logger.info(e)
    except s3.exceptions.BucketAlreadyOwnedByYou as e:
        logger.info(e)
    except Exception as e:
        code = type(e).__name__
        messsage = f"create_bucket caused unhandled exception: {code}: {str(e)}"
        logger.error(messsage)

async def purge_and_delete_bucket(s3, bucket_name):
    # Delete the objects we created then the bucket to tidy things up up
    try:
        """
        To deal with lots of response values from list_objects_v2 use a
        paginator to make it easy to iterate through batches of results.
        """
        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket_name)
        tasks = []
        async for page in pages:
            contents = page.get("Contents", [])
            """
            Can't just use "Contents" list as "Objects" value in delete_objects
            request so use list comprehension to create valid "Objects" list.
            """
            delete_list = [{"Key": obj["Key"]} for obj in contents]
            if delete_list:
                """
                Launch delete_objects calls as tasks and gather the responses
                so the deletes run in parallel rather than sequentially.
                """
                tasks.append(s3.delete_objects(
                    Bucket=bucket_name,
                    Delete={"Objects": delete_list, "Quiet": True}
                ))

        await asyncio.gather(*tasks)

        logger.info("Deleting bucket {}".format(bucket_name))
        response = await s3.delete_bucket(Bucket=bucket_name)
    except s3.exceptions.NoSuchBucket as e:
        logger.info(e)
    except Exception as e:
        code = type(e).__name__
        messsage = f"purge_and_delete_bucket caused unhandled exception: {code}: {str(e)}"
        logger.error(messsage)

