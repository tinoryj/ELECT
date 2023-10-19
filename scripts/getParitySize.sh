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
#!/bin/bash


# ycsb
# array=(20204 20288 20200 20092 20204 20280 20156 20104 20284 20284)
# zippydb
# array=(1888 1952 1944 1820 1920 2016 2048 2048 1920 1764)
# up2x
# array=(1812 1740 1744 1772 1784 1796 1800 1764 1728 1796)

# value=32
# array=(2620 2696 2736 2712 2700 2676 2692 2632 2636 2668)

# value=128
# array=(2388 2372 2364 2368 2364 2352 2328 2352 2356 2356)
# value=512
# array=(2280 2240 2268 2272 2256 2260 2252 2284 2312 2312)
# value=2048
# array=(1984 1984 1988 1988 1984 1980 1980 1984 1984 1984)
# value=8192
# array=(1984 1980 1984 1988 1984 1976 1980 1984 1980 1984)

# key=8
# array=(2368 2356 2408 2388 2340 2356 2352 2396 2444 2416)
# key=16
# array=(2344 2308 2360 2348 2292 2308 2316 2344 2392 2396)
# key=32
# array=(2280 2240 2268 2272 2256 2260 2252 2284 2312 2312)
# key=64
# array=(2156 2148 2164 2128 2100 2104 2108 2140 2144 2152)
# key=128
# array=(1888 1836 1896 1868 1868 1796 1800 1820 1908 1964 1952)
# 20GB-20nodes elect
# array=(1944 2016 2020 2020 2100 2120 2040 2048 2132 2040 1960 2024 2016 2024 2116 2124 2052 1968 2028 2024)
array=(1924 1928 1928 1924 1936 1928 1932 1932 1924 1932)


total_size=0

# traverse array using a counter
for num in "${array[@]}"
do
  total_size=$((total_size + num))
done

sum=$((total_size / 4 ))
echo "The number of ec sstable count is: $sum"
# divide sum by 8
num_ec=$((sum / 8))

echo "The unique number of ec sstable count is: $num_ec"

echo "Number of parity codes: $(( num_ec / 2))"

# total size of parity codes
code_length=4.2 # MB
total_size=$(echo "scale=10; $num_ec * 4.2 * 2 / 1024" | bc)
echo "Total size of parity codes: $total_size GB"