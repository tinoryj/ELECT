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

func() {
    coordinator="192.168.10.30"
    sstable_size=$2
    fanout_size=$3
    cd /home/yjren/cassandra
    bin/cqlsh $coordinator -e "create keyspace ycsb WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 3 };
    USE ycsb;
    create table usertable (y_id varchar primary key, field0 varchar);
    ALTER TABLE usertable WITH compaction = { 'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': $sstable_size, 'fanout_size': $fanout_size};
    ALTER TABLE usertable1 WITH compaction = { 'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': $sstable_size, 'fanout_size': $fanout_size};
    ALTER TABLE usertable2 WITH compaction = { 'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': $sstable_size, 'fanout_size': $fanout_size};

    ALTER TABLE ycsb.usertable WITH compression = {'enabled':'false'};
    ALTER TABLE ycsb.usertable1 WITH compression = {'enabled':'false'};
    ALTER TABLE ycsb.usertable2 WITH compression = {'enabled':'false'};

    consistency all;"
}

func "$1" "$2" "$3"
