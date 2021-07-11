#!/bin/bash
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

#-------------------------------------------------------------------------------
# Runs up a standalone minio server
# Exposes API on port 9001
#
# In ~/.aws/credentials add profile e.g.:
#
# [minio]
# aws_access_key_id = guest
# aws_secret_access_key = guest@minio
#
# Alternatively set environment variables e.g.:
# export AWS_ACCESS_KEY_ID=guest
# export AWS_SECRET_ACCESS_KEY=guest@minio
# https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html
#
#
# Create a bucket using aws cli:
# aws s3 mb s3://bucket-name --endpoint http://localhost:9001 --profile minio
#
# List buckets and objects
# The following example lists all of your S3 buckets.
# aws s3 ls --endpoint http://localhost:9001 --profile minio
#
#-------------------------------------------------------------------------------
# Increase available file descriptors with
# --ulimit nofile=262144:262144

# Use Docker writeable filesystem layer (default)
docker run --rm -it \
    -p 9001:9000 \
    -e MINIO_ROOT_USER=guest \
    -e MINIO_ROOT_PASSWORD=guest@minio \
    minio/minio server /data

# Use tmpfs for minio /data gives fast put_object performance, but in-memory store
#docker run --rm \
#    -p 9001:9000 \
#    -e MINIO_ROOT_USER=guest \
#    -e MINIO_ROOT_PASSWORD=guest@minio \
#    --tmpfs /data \
#    minio/minio server /data

# Use bind-mount
# Create minio-store if it doesn't already exist.
#mkdir -p minio-store
#docker run --rm \
#    -p 9001:9000 \
#    -e MINIO_ROOT_USER=guest \
#    -e MINIO_ROOT_PASSWORD=guest@minio \
#    -v $PWD/minio-store:/data:rw \
#    minio/minio server /data

