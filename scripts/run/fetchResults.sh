#!/bin/bash
. /etc/profile
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
source "${SCRIPT_DIR}/../common.sh"

READ_Average=()
READ_P99=()
WRITE_Average=()
WRITE_P99=()
SCAN_Average=()
SCAN_P99=()
UPDATE_Average=()
UPDATE_P99=()
TRHOUGHPUT=()

func() {

    echo "Fetch results..."
    logFilePath=$1

    # 检查文件是否存在
    if [ ! -f "$logFilePath" ]; then
        echo "File not found: $logFilePath"
        exit 1
    fi

    read_avg_latency=$(grep "[READ], AverageLatency(us)" "$logFilePath" | awk -F, '{print $3}' | tr -d ' ')
    read_p99_latency=$(grep "[READ], 99thPercentileLatency(us)" "$logFilePath" | awk -F, '{print $3}' | tr -d ' ')
    READ_Average+=("$read_avg_latency")
    READ_P99+=("$read_p99_latency")

    write_avg_latency=$(grep "[INSERT], AverageLatency(us)" "$logFilePath" | awk -F, '{print $3}' | tr -d ' ')
    write_p99_latency=$(grep "[INSERT], 99thPercentileLatency(us)" "$logFilePath" | awk -F, '{print $3}' | tr -d ' ')
    WRITE_Average+=("$write_avg_latency")
    WRITE_P99+=("$write_p99_latency")

    scan_avg_latency=$(grep "[SCAN], AverageLatency(us)" "$logFilePath" | awk -F, '{print $3}' | tr -d ' ')
    scan_p99_latency=$(grep "[SCAN], 99thPercentileLatency(us)" "$logFilePath" | awk -F, '{print $3}' | tr -d ' ')
    SCAN_Average+=("$scan_avg_latency")
    SCAN_P99+=("$scan_p99_latency")

    update_avg_latency=$(grep "[UPDATE], AverageLatency(us)" "$logFilePath" | awk -F, '{print $3}' | tr -d ' ')
    update_p99_latency=$(grep "[UPDATE], 99thPercentileLatency(us)" "$logFilePath" | awk -F, '{print $3}' | tr -d ' ')
    UPDATE_Average+=("$update_avg_latency")
    UPDATE_P99+=("$update_p99_latency")

    throughput=$(grep "[OVERALL], Throughput(ops/sec)" "$logFilePath" | awk -F, '{print $3}' | tr -d ' ')
    TRHOUGHPUT+=("$throughput")
}

func "$1"

file_name="${expName}-Scheme-${mode}-${workload}-KVNumber-${recordcount}-OPNumber-${operationcount}-ClientNumber-${threads}-Time-$(date +%s)"

