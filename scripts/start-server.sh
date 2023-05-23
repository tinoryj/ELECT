# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

. /etc/profile

cd /mnt/ssd/Debug/CassandraEC
git pull origin yuanming
kill -9 $(ps aux | grep cassandra| grep -v grep | awk 'NR == 1'  | awk {'print $2'})
rm -rf data logs
mkdir -p data/receivedParityHashes/
mkdir -p data/localParityHashes/
mkdir -p data/ECMetadata/
mkdir -p data/tmp/
mkdir -p logs
ant realclean && ant -Duse.jdk11=true
cp src/native/src/org/apache/cassandra/io/erasurecode/libec.so lib/sigar-bin
nohup bin/cassandra &> logs/debug.log &