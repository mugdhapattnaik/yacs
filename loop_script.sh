#!/bin/bash
LOG_DIR=('./logs/rr' './logs/random' './logs/ll')
ALGO=('RR' 'RANDOM' 'LL');

#creates a trap - what to do upon receiving ctrl-C
#kills all python processes and exits script
trap "killall python3; exit" SIGINT

#create logs directories if not present
rm -rf ./logs
sleep .5
for (( i=0; i<3; i++ )); do 
    mkdir -p ${LOG_DIR[$i]};
done
sleep .5

i=$1;
base_port=$(($2-1));
num_workers=$(($3+1));

#start 3 worker threads
for (( j = 1;  j<$num_workers; j++ )); do
    port=$(($base_port+$j));    #assign port number
    python3 -u worker.py $port $j &> ./logs/$port.txt &    #-u flag is to prevent stdout buffering
    #python3 -u worker.py $port $j &    #-u flag is to prevent stdout buffering
    echo Started worker $j with PID $! and is listening to port $port   #log statement 
done

for (( i=0; i<3; i++ )); do   #for each algorithm

    printf "Starting with %s\n\n" ${ALGO[$i]}
    

    sleep 2;    #allow for worker processes to begin
    python3 -u master.py config.json ${ALGO[$i]} &> ./logs/master_out.txt &
    printf "Started master with PID %s and is listening to port 5000 and 5001\n\n" $! #log statement 
    lsof -i
    sleep 2;    #allow for master process to begin
    python3 -u ./tests/requests.py 5

    #pause processing until 'n' has been input
    printf "\n\nEnter n after %s gets over\n" ${ALGO[$i]};
    while : ; do
        read -n 1 key <&1
        if [[ $key = n ]] ; then
            printf "\nRun analysis.py\n"
            killall -2 python3;
            break
        fi
    done

    : '
    #run analysis.py
    #python3 analysis.py
    '

    #movement of log files to their directories
    mv ./logs/master_log.txt ${LOG_DIR[$i]} #master log
    mv ./logs/w1_log.txt ${LOG_DIR[$i]}
    mv ./logs/w2_log.txt ${LOG_DIR[$i]}
    mv ./logs/w3_log.txt ${LOG_DIR[$i]}     #worker logs

    #movement of processing logs of workers and master
    for (( j = 1;  j<$num_workers; j++ )); do
        port=$(($base_port+$j));    #port number
        mv ./logs/$port.txt ${LOG_DIR[$i]}     #worker logs
    done
    mv ./logs/master_out.txt ${LOG_DIR[$i]}    #master log
    printf "All log files moved\n"

    printf "Done with %s\n\n" ${ALGO[$i]}
    sleep 10

done
