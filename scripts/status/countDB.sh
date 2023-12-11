#!/bin/bash

ExpName=$1

types=("cassandra" "elect" "mlsm")

for type in "${types[@]}"; do
    du -s /mnt/ssd/backups/$type/$ExpName/data
    if [ "${type}" == "cassandra" ]; then
        find /mnt/ssd/backups/$type/$ExpName/data/data/ycsbraw/usertable* -type f \( -name "*Index.db" -o -name "*Digest.crc32" -o -name "*Filter.db" -o -name "*Statistics.db" -o -name "*Summary.db" -o -name "*TOC.txt" -o -name "*EC.db" -o -name "*Data.db" \) -print0 | xargs -0 du -s | awk '{
        if($2 ~ /Index\.db/) { sum_index += $1 }
        else if($2 ~ /Digest\.crc32/) { sum_digest += $1 }
        else if($2 ~ /Filter\.db/) { sum_filter += $1 }
        else if($2 ~ /Statistics\.db/) { sum_statistics += $1 }
        else if($2 ~ /Summary\.db/) { sum_summary += $1 }
        else if($2 ~ /TOC\.txt/) { sum_toc += $1 }
        else if($2 ~ /Data\.db/) { sum_data += $1 }
        else if($2 ~ /EC\.db/) { sum_ec += $1 }
        } 
        END {
            print "Total for Index.db: " sum_index
            print "Total for Digest.crc32: " sum_digest
            print "Total for Filter.db: " sum_filter
            print "Total for Statistics.db: " sum_statistics
            print "Total for Summary.db: " sum_summary
            print "Total for TOC.txt: " sum_toc
            print "Metadata Total: " sum_index+sum_digest+sum_filter+sum_statistics +sum_summary+sum_toc
            print "Total for Data.db: " sum_data
            print "Total for EC.db: " sum_ec
        }'
    else
        find /mnt/ssd/backups/$type/$ExpName/data/data/ycsb/usertable* -type f \( -name "*Index.db" -o -name "*Digest.crc32" -o -name "*Filter.db" -o -name "*Statistics.db" -o -name "*Summary.db" -o -name "*TOC.txt" -o -name "*EC.db" -o -name "*Data.db" \) -print0 | xargs -0 du -s | awk '{
        if($2 ~ /Index\.db/) { sum_index += $1 }
        else if($2 ~ /Digest\.crc32/) { sum_digest += $1 }
        else if($2 ~ /Filter\.db/) { sum_filter += $1 }
        else if($2 ~ /Statistics\.db/) { sum_statistics += $1 }
        else if($2 ~ /Summary\.db/) { sum_summary += $1 }
        else if($2 ~ /TOC\.txt/) { sum_toc += $1 }
        else if($2 ~ /Data\.db/) { sum_data += $1 }
        else if($2 ~ /EC\.db/) { sum_ec += $1 }
        } 
        END {
            print "Total for Index.db: " sum_index
            print "Total for Digest.crc32: " sum_digest
            print "Total for Filter.db: " sum_filter
            print "Total for Statistics.db: " sum_statistics
            print "Total for Summary.db: " sum_summary
            print "Total for TOC.txt: " sum_toc
            print "Metadata Total: " sum_index+sum_digest+sum_filter+sum_statistics        +sum_summary+sum_toc
            print "Total for Data.db: " sum_data
            print "Total for EC.db: " sum_ec
        }'
    fi
done
