#!/bin/bash
LOG_DIR=('./logs/RR' './logs/RANDOM' './logs/LL')
ALGO=('RR' 'RANDOM' 'LL');
NUM_ALGO=3;

#creates a trap - what to do upon receiving ctrl-C
#kills all python processes and exits script
trap "killall python3; exit" SIGINT

#recreate logs directories
rm -rf ./logs
sleep .5
for (( i=0; i<3; i++ )); do 
    mkdir -p ${LOG_DIR[$i]};
done
sleep .5

#storing command line arguments
i=$1;
base_port=$(($2-1));
num_workers=$(($3+1));

for (( i=0; i<$NUM_ALGO; i++ )); do   #for each algorithm

    #START WORKER AND MASTER PROCESSES
    printf "Starting with %s\n\n" ${ALGO[$i]}
	for (( j = 1;  j<$num_workers; j++ )); do
    	port=$(($base_port+$j));    #assign port number
   		python3 -u worker.py $port $j &> ./logs/w$((j))_out.txt &    #-u flag is to prevent stdout buffering
    	echo Started worker $j with PID $! and is listening to port $port   #log statement 
	done    
    sleep 2;    #allow for worker processes to begin
    python3 -u master.py config.json ${ALGO[$i]} &> ./logs/master_out.txt &
    printf "Started master with PID %s and is listening to port 5000 and 5001\n\n" $! #log statement 
    
    #SEND JOB REQUESTS
    printf "Starting requests\n\n"
    sleep 2;    #allow for master process to begin
    python3 -u ./tests/requests.py 5

    #WAIT FOR JOBS TO COMPLETE
    printf "\n\nWaiting for jobs to complete\n";
    while : ; do
        #read last n lines master_out file
        recent_logs=$(tail -$num_workers ./logs/master_out.txt)

        ctr=0       #counts the number of finished workers
        while read line; do
            temp="${line: -1}"      #read the last character of each line (number of active slots)
            #echo "$temp"
            if [[ $temp = 0 ]]; then    #if all slots have completed their tasks
                let "ctr += 1"          #'let' allows arithmetic operations
                #printf "\t%d\n" $ctr
            fi
        done <<< "$recent_logs"     #redirects variable input
        echo Number of workers that have completed processing: $ctr

        if [[ "$ctr" -eq "$num_workers-1" ]]; then #if all jobs completed
            printf "All jobs have been completed\n. Running anlaysis.\n"
            sleep .5
            killall python3;    #terminate processes
            break
        else
            sleep 1
        fi
    done

    #RUN ANALYSIS
    python3 analysis.py

    #MOVE LOGS TO RESPECTIVE ALGORITHM'S DIRECTORIES
    mv ./logs/master.log ${LOG_DIR[$i]}             #master log
    mv ./logs/master_out.txt ${LOG_DIR[$i]}         #master output
    for (( j = 1;  j<$num_workers; j++ )); do
        mv ./logs/w$((j)).log ${LOG_DIR[$i]}        #worker log
        mv ./logs/w$((j))_out.txt ${LOG_DIR[$i]}    #worker output
    done
    printf "All log files moved\n"
    
    printf "Done with %s\n\n" ${ALGO[$i]}
    sleep 2
done

#RUN FINAL ANALYSIS
#python3 analysis.py all
