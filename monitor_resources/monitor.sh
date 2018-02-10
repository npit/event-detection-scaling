#!/usr/bin/env bash
output_file="$1"
cmd="dstat"
# stats to show
cmd+=" -tcgydsn"
# cpus
cmd+=" -C $(seq -s , 0 7),total"
# output
cmd+=" --output ${output_file}"

#echo "Dstat command:"
#echo "${cmd}"
#run
$cmd > /dev/null &
