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

kill -9 $(ps aux | grep cassandra| grep -v grep | awk 'NR == 1'  | awk {'print $2'})
func() {

    ec_data_nodes=$1
    parity_nodes=$2
    max_level_count=$3
    concurrent_ec=$4
    initial_delay=$5
    task_delay=$6
    stripe_update_frequency=$7
    max_send_sstables=$8
    internode_max_message_size=$9
    internode_application_send_queue_capacity="128MiB"
    internode_application_send_queue_reserve_endpoint_capacity="256MiB"
    internode_application_send_queue_reserve_global_capacity="1024MiB"
    internode_application_receive_queue_capacity="128MiB"
    internode_application_receive_queue_reserve_endpoint_capacity="256MiB"
    internode_application_receive_queue_reserve_global_capacity="1024MiB"

    cd /mnt/ssd/Debug/CassandraEC
    # git checkout yuanming
    # git pull origin yuanming
    git checkout 2f23462

    rm -rf data logs
    mkdir -p data/receivedParityHashes/
    mkdir -p data/localParityHashes/
    mkdir -p data/ECMetadata/
    mkdir -p data/tmp/
    mkdir -p logs
    ant realclean && ant -Duse.jdk11=true
    cp src/native/src/org/apache/cassandra/io/erasurecode/libec.so lib/sigar-bin
    
    sed -i "s/ec_data_nodes:.*$/ec_data_nodes: ${ec_data_nodes}/" conf/cassandra.yaml
    sed -i "s/parity_nodes:.*$/parity_nodes: ${parity_nodes}/" conf/cassandra.yaml
    sed -i "s/max_level_count:.*$/max_level_count: ${max_level_count}/" conf/cassandra.yaml
    sed -i "s/concurrent_ec:.*$/concurrent_ec: ${concurrent_ec}/" conf/cassandra.yaml
    sed -i "s/initial_delay:.*$/initial_delay: ${initial_delay}/" conf/cassandra.yaml
    sed -i "s/task_delay:.*$/task_delay: ${task_delay}/" conf/cassandra.yaml
    sed -i "s/stripe_update_frequency:.*$/stripe_update_frequency: ${stripe_update_frequency}/" conf/cassandra.yaml
    sed -i "s/max_send_sstables:.*$/max_send_sstables: ${max_send_sstables}/" conf/cassandra.yaml

    sed -i "s/internode_max_message_size:.*$/internode_max_message_size: ${internode_max_message_size}/" conf/cassandra.yaml
    sed -i "s/internode_application_send_queue_capacity:.*$/internode_application_send_queue_capacity: ${internode_application_send_queue_capacity}/" conf/cassandra.yaml
    sed -i "s/internode_application_send_queue_reserve_endpoint_capacity:.*$/internode_application_send_queue_reserve_endpoint_capacity: ${internode_application_send_queue_reserve_endpoint_capacity}/" conf/cassandra.yaml
    sed -i "s/internode_application_send_queue_reserve_global_capacity:.*$/internode_application_send_queue_reserve_global_capacity: ${internode_application_send_queue_reserve_global_capacity}/" conf/cassandra.yaml
    sed -i "s/internode_application_receive_queue_capacity:.*$/internode_application_receive_queue_capacity: ${internode_application_receive_queue_capacity}/" conf/cassandra.yaml
    sed -i "s/internode_application_receive_queue_reserve_endpoint_capacity:.*$/internode_application_receive_queue_reserve_endpoint_capacity: ${internode_application_receive_queue_reserve_endpoint_capacity}/" conf/cassandra.yaml
    sed -i "s/internode_application_receive_queue_reserve_global_capacity:.*$/internode_application_receive_queue_reserve_global_capacity: ${internode_application_receive_queue_reserve_global_capacity}/" conf/cassandra.yaml

    nohup bin/cassandra &> logs/debug.log &
}

func "$1" "$2" "$3" "$4" "$5" "$6" "$7" "$8" "$9" "$10" "$11" "$12" "$13" "$14" "$15"