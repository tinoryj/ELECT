#!/bin/bash

function CPU {
    targetName=$1
    # 提取所有大于10的 CPU 负载的百分比值，并保存到一个数组中
    mapfile -t cpu_loads < <(awk -F": " 'NR > 1 {print $(NF)}' ${targetName} | awk -F"%" '$1 > 10 {print $1}')

    # 计算所有值的均值和标准差
    total=0
    count=0
    for i in "${cpu_loads[@]}"; do
        total=$(echo $total+$i | bc)
        ((count++))
    done
    mean=$(echo "scale=2; $total / $count" | bc)

    total_square_diff=0
    for i in "${cpu_loads[@]}"; do
        square_diff=$(echo "($i - $mean)^2" | bc)
        total_square_diff=$(echo "$total_square_diff + $square_diff" | bc)
    done

    std_dev=$(echo "scale=2; sqrt($total_square_diff / $count)" | bc)

    # 输出新的均值
    # echo "Filtered mean CPU Load: $mean%"
    echo "$mean"
}

function Mem {
    targetName=$1
    # 提取所有内存占用数值，并保存到一个数组中
    mapfile -t memory_usages < <(awk -F" " 'NR > 1 {print $(NF-1)}' ${targetName})

    # 计算所有数值的均值和标准差
    total=0
    count=0
    for i in "${memory_usages[@]}"; do
        total=$(echo $total+$i | bc)
        ((count++))
    done
    mean=$(echo "scale=2; $total / $count" | bc)

    total_square_diff=0
    for i in "${memory_usages[@]}"; do
        square_diff=$(echo "($i - $mean)^2" | bc)
        total_square_diff=$(echo "$total_square_diff + $square_diff" | bc)
    done

    std_dev=$(echo "scale=2; sqrt($total_square_diff / $count)" | bc)

    # 输出新的均值
    # echo "Filtered mean Memory Usage: $mean KiB"
    echo "$mean"

}

targetPath=$1
expName=$2
middle=$3
NodeNumber=10
schemes=("cassandra" "elect" "mlsm")

for scheme in "${schemes[@]}"; do
    echo "Scheme: ${scheme}, CPU"
    for ((ID = 1; ID <= NodeNumber; ID++)); do
        # echo "Processing File: $targetPath/${scheme}/${expName}-Load-Node${ID}/${expName}-${scheme}-Load_Loading_cpu_usage.txt"
        CPU "$targetPath/${scheme}/${expName}-${middle}-Node${ID}/${expName}-${scheme}-*_cpu_usage.txt"
    done
done

for scheme in "${schemes[@]}"; do
    echo "Scheme: ${scheme}, Mem"
    for ((ID = 1; ID <= NodeNumber; ID++)); do
        # echo "Processing File: $targetPath/${scheme}/${expName}-Load-Node${ID}/${expName}-${scheme}-Load_Loading_memory_usage.txt"
        Mem "$targetPath/${scheme}/${expName}-${middle}-Node${ID}/${expName}-${scheme}-*_memory_usage.txt"
    done
done
