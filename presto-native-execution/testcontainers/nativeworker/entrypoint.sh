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

# Add a new line to /opt/presto-server/etc/node.properties using the environment variable
if [ -n "$NODE_ID" ]; then
    echo "$NODE_ID" >> /opt/presto-server/etc/node.properties
fi

GLOG_logtostderr=1 presto_server \
    --etc-dir=/opt/presto-server/etc
