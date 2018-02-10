#!/usr/bin/env bash

cd ../exec

optsFile="./clustering.bioasq.properties"
monitorScript="/home/pittarasnikif/monitor_resources/monitor.sh"

#articleSizes="32 64 128 256 512 1024 2048"
#articleSizes="16384"
articleSizes="1024 2048"
partitionNumSizes="16"
#numExperiments="1 2 3 4 5"
numExperiments="1"

#runModes="distributed parallel"
#runModes="parallel distributed"
runModes="distributed"



for rmode in $runModes; do

	echo "Running in mode $rmode , opts: $optsFile"
        sed -i "s/operation_mode=.*/operation_mode=$rmode/g" $optsFile
	grep operation_mode $optsFile


	if [ "$rmode" == "parallel" ]; then
		partitionNumSizes="x";
	        echo "Setting partitions to -1 because parallel."
	fi

	for partitionNum in $partitionNumSizes ; do
	
        	sed -i "s/num_partitions=.*/num_partitions=$partitionNum/g" $optsFile
	        echo "NumpartitionNums : $partitionNum"
        	grep "num_partitions" $optsFile
	        echo "NumpartitionNums : $partitionNum" >> log
	
	        for experiment  in $numExperiments ; do
	                for articlesize  in $articleSizes ; do
				echo "Starting experiment at $(date)"
        	                echo "article size: $articlesize"
	
        	                echo >> log && echo >> log && echo "start: $(date)" >> log
                	        echo "Num articles :$articlesize" >> log
                        	echo >> log
	                        sed -i "s/max_articles=.*/max_articles=$articlesize/g" $optsFile

				# start monitoring
				monitorFile="stats_${rmode}_${partitionNum}_${experiment}_${articlesize}.txt"
				$monitorScript "$monitorFile"
				
				echo $optsFile
				
        	                ./cluster $optsFile  >> log 2>&1
				# kill monitor, copy back
				kill `pgrep dstat`
				mv "$monitorFile" ../sensitive/

				echo "end: $(date)" >> log
	                        grep "\[clustering\]" log
        	                mv log "../sensitive/log_p${partitionNum}_a${articlesize}_e${experiment}_${rmode}"
				cp clustering.properties "../sensitive/log_p${partitionNum}_a${articlesize}_e${experiment}_${rmode}.properties"
                	done
	        done
	done

done


cd ../sensitive


