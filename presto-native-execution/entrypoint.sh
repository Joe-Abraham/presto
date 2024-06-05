#!/bin/sh
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

<<<<<<<< HEAD:presto-native-execution/entrypoint.sh
echo "node.id=$HOSTNAME" >> /opt/presto-server/etc/node.properties

# Check for the reason of setting split_preload_per_driver:
# https://github.com/prestodb/presto/issues/20020#issuecomment-1785083459
GLOG_logtostderr=1 presto_server \
    --etc-dir=/opt/presto-server/etc \
    2>&1 | tee /var/log/presto-server/console.log
========
FROM quay.io/centos/centos:stream8

ENV PROMPT_ALWAYS_RESPOND=n
ENV CC=/opt/rh/gcc-toolset-9/root/bin/gcc
ENV CXX=/opt/rh/gcc-toolset-9/root/bin/g++

RUN mkdir -p /scripts /velox/scripts
COPY scripts /scripts
COPY velox/scripts /velox/scripts
RUN mkdir build && \
    (cd build && ../scripts/setup-centos.sh && \
                 ../velox/scripts/setup-adapters.sh aws && \
                 ../scripts/setup-adapters.sh ) && \
    rm -rf build
>>>>>>>> master:presto-native-execution/scripts/dockerfiles/centos-8-stream-dependency.dockerfile
