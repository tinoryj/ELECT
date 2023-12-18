#!/bin/bash

types=("cassandra" "elect-trans" "elect-pdm" "elect-rdm")
runnings=("normal" "degraded")
workloads=("workloadReadBreak")
expName="Exp6-breakdown"
roundNumber=5
nodeNumber=10
for type in "${types[@]}"; do
    for running in "${runnings[@]}"; do
        for workload in "${workloads[@]}"; do
            for node in $(seq 1 $nodeNumber); do
                for round in $(seq 1 $roundNumber); do
                    echo "Type: ${type}, Running: ${running}, Workload: ${workload}"
                    echo "Path: "$type/${expName}-${workload}-${round}-Node${node}/Exp6-breakdown-${type}-Run-${running}-Round-${round}_${workload}_After-normal-run_db_stats.txt""
                    input_file="data.txt"
                    if [ $running == "normal" ]; then
                        input_file="$type/${expName}-${workload}-${round}-Node${node}/Exp6-breakdown-${type}-Run-${running}-Round-${round}_${workload}_After-normal-run_db_stats.txt"
                    else
                        input_file="$type/${expName}-${workload}-${round}-Node${node}/Results/Exp6-breakdown-${type}-Run-${running}-Round-${round}_${workload}_After-normal-run_db_stats.txt"
                    fi
                    
                    memtable_time_cost=$(grep "Memtable time cost" $input_file | awk '{print $5}')
                    commitlog_time_cost=$(grep "CommitLog time cost" $input_file | awk '{print $5}')
                    flush_time_cost=$(grep "Flush time cost" $input_file | awk '{print $5}')
                    compaction_time_cost=$(grep "Compaction time cost" $input_file | awk '{print $5}')
                    rewrite_time_cost=$(grep "Rewrite time cost" $input_file | awk '{print $5}')
                    ecsstable_compaction_time_cost=$(grep "ECSSTable compaction time cost" $input_file | awk '{print $7}')
                    encoding_time_cost=$(grep "Encoding time cost" $input_file | awk '{print $5}')
                    migrate_raw_sstable_time_cost=$(grep "Migrate raw SSTable time cost" $input_file | awk '{print $7}')
                    migrate_parity_code_time_cost=$(grep "Migrate parity code time cost" $input_file | awk '{print $7}')

                    read_index_time_cost=$(grep "Read index time cost" $input_file | awk '{print $5}')
                    read_cache_time_cost=$(grep "Read cache time cost" $input_file | awk '{print $5}')
                    read_memtable_time_cost=$(grep "Read memtable time cost" $input_file | awk '{print $5}')
                    read_sstable_time_cost=$(grep "Read SSTable time cost" $input_file | awk '{print $5}')
                    read_migrated_raw_data_time_cost=$(grep "Read migrated raw data time cost" $input_file | awk '{print $8}')
                    retrieve_time_cost=$(grep "Retrieve time cost" $input_file | awk '{print $5}')
                    decoding_time_cost=$(grep "Decoding time cost" $input_file | awk '{print $5}')
                    read_migrated_parity_time_cost=$(grep "Read migrated parity time cost" $input_file | awk '{print $7}')

                    echo $memtable_time_cost
                    echo $commitlog_time_cost
                    echo $flush_time_cost
                    echo $compaction_time_cost
                    echo $rewrite_time_cost
                    echo $ecsstable_compaction_time_cost
                    echo $encoding_time_cost
                    echo $migrate_raw_sstable_time_cost
                    echo $migrate_parity_code_time_cost

                    echo $read_index_time_cost
                    echo $read_cache_time_cost
                    echo $read_memtable_time_cost
                    echo $read_sstable_time_cost
                    echo $read_migrated_raw_data_time_cost
                    echo $retrieve_time_cost
                    echo $decoding_time_cost
                    echo $read_migrated_parity_time_cost
                done
            done
        done
    done
done
