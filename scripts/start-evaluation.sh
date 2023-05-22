func() {
    coordinator=$1
    cd /home/yjren/ycsb-0.17.0/
    bin/ycsb load cassandra-cql -p hosts=$coordinator -s -P workloads/workload_template
}

func "$1"
