# YACS
 
A simple centralised scheduler for Map-Reduce tasks.

The path to the config file, and the scheduling algorithm (RANDOM, RR, LL) are given to the Master as command line arguments.   
E.g. ` python Master.py /path/to/config.json RR `

The port and worker_id are supplied as command line arguments to each Worker.  
E.g. ` python Worker.py 4000 1 `
