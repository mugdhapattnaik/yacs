# YACS
 
A simple centralised scheduler for Map-Reduce jobs.

## Setting up the environment

Create a Python3 virtual environment.  
~~~ 
pip3 install python3-venv  OR  pip3 install virtualenv 
python3 -m venv name-of-env  
~~~  
Activate the virtual environment  
~~~
source name-of-env/bin/activate  
~~~  

Clone this repository. 
~~~ 
git clone https://github.com/mugdhapattnaik/yacs.git  
cd yacs  
~~~

Install the dependencies.  
~~~
pip3 install -r requirements.txt
~~~  

To configure the number of Workers and their attributes (workerID, number of slots, port numbers, host IPs), edit the `config.json` using any text editor
  
## Run all algorithms using loop_script.sh

This script runs all the algorithms and performs analysis on each of them, including drawing comparisons. This script rewrites the logs and graphs directories. It ensures the addresses used by the processes are released and the processes are terminated.

~~~
./loop_script.sh <starting worker_id> <starting port> <number of workers>
~~~
For the default config.json the above command would translate to:
~~~
./loop_script.sh 1 4000 3
~~~

It's important to note that the shell script does not read from the config.json file, but assumes a series of worker_id and port numbers being used. The number of workers to be specified must be the same as the number of entries in the config.json file. 

### Run the processes individually on different terminals

***1. Running the processes***

Run the following on a terminal tab for the **Master** process. The path to the config file, and the scheduling algorithm (RANDOM, RR, LL) are given to the Master as command line arguments.    
~~~ 
python3 master.py /path/to/config.json <scheduling_algo> 
~~~  

Run the following on *n* terminal tabs for *n* **Worker** processes. The port and worker_id are supplied as command line arguments to each Worker.   
~~~
python3 worker.py <port> <worker_id>  
~~~  
In a separate terminal tab, initiate job requests by running:
~~~
python3 requests.py <number_of_requests>
~~~
  
***2. Analytics***

Run the following after completing task executions.  
~~~
python3 analysis.py <args>
~~~ 
~~~
args        meaning
========================     
<!--- RR          to generate graphs for Round Robin algorithm  --->
<!--- LL          to generate graphs for Least Loaded algorithm  --->
<!--- RANDOM      to generate graphs for Random algorithm --->
current     to generate graphs (tasks vs time; jobs vs time; running tasks in each worker vs time) for the most recently generated logs
ALL         to generate comparison graphs for mean and median times for the last 3 scheduling algorithms. To be used after calling `python3 analysis.py current` for the 3 scheduling algorithms (RANDOM, RR, LL)  
~~~
To view the generated graphs:
~~~
cd graphs
~~~   
  
***3. To stop all running Master and Workers***
Run this command in a separate terminal tab.  
**WARNING** - Doing so will stop other currently running python3 scripts   
~~~
pkill python3
~~~
Simple check to see all python processes that are currently running:  
~~~
ps au | grep python3
~~~
The processes can be individually stopped manually by pressing the keys `Ctrl+C` two times.  
  
## Deactivate the environment 

To deactivate the environment, run:
~~~
deactivate
~~~
