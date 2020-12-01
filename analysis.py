#!/usr/bin/python3

import json
import numpy as np
import matplotlib.pyplot as plt
from statistics import mean, median

master = open("logs/master_log.txt", "r")
config_file = open("config.json", 'r')
config = json.load(config_file)
config_file.close()
worker_ids = []
for w in config["workers"]:
    worker_ids.append(w["worker_id"])   

jobs = {}
tasks = {}
job_ids = []
time_intervals = []
num_tasks = {}
job_start_times = {}
task_start_times = {}
count_m = 0
count_w = 0

for line in master.readlines():
    if count_m != (3 + len(worker_ids)):
        count_m += 1
    else:  
        l = line.strip().split()
        if len(l) == 2:
            job_id, job_start = l
            job_start = float(job_start)
            job_ids.append(job_id)
            job_start_times[job_id] = job_start
        else:
            time_intervals.append(float(l[-1]))
            l.pop(-1)
            for i in l:
                id, n = i.split(":")
                num_tasks[id] = n

for ji in job_ids:
    jobs[ji] = []

for i in worker_ids:
    w = "logs/w" + str(i) + "_log.txt"
    worker_file = open(w,"r")
    task_start_times[i] = []
    count_w = 0
    
    for line in worker_file.readlines():
        if count_w != 3:
            count_w += 1
        else: 
            j, t, st, end, id = line.strip().split() 
            st = float(st)
            end = float(end)       
            jobs[j].append(end)
            tasks[j+"_"+t] = (end - st)
            task_start_times[i].append(st)

for j in jobs:
    jobs[j] = round((max(jobs[j]) - job_start_times[j]), 8)
      
#Not sure where these should be shown
job_mean = mean(jobs[k] for k in jobs)
job_median = median(jobs[k] for k in jobs)
task_mean = mean(tasks[k] for k in tasks)
task_median = median(tasks[k] for k in tasks)


#Graph showing all the completion times for all the tasks
plt.bar(list(tasks.keys()),list(tasks.values()))
plt.xticks(fontsize=6.5)
plt.xticks(rotation=45)
plt.ylabel('Completion time(s)')
plt.xlabel('JobID_TaskID')
plt.savefig("graphs/task_completion.png", bbox_inches="tight")


#Graph showing all the completion times for all the jobs
job_ids = ["Job "+i for i in job_ids]
part1_jobs_fig = plt.figure()
part1_jobs_ax = part1_jobs_fig.add_axes([0.1, 0.1, 0.85, 0.85])
part1_jobs_ax.bar(job_ids,list(jobs.values()))
part1_jobs_ax.set_ylabel('Completion time(s)')
part1_jobs_ax.set_xlabel('JobID')
part1_jobs_fig.savefig("graphs/job_completion.png")


#Graph showing number of tasks on each machine against time
part2_fig = plt.figure()
part2_ax = part2_fig.add_axes([0,0,1,1])

print("Mean of job completion times = ", job_mean)
print("Mean of task completion times = ", task_mean)
print("Median of job completion times = ", job_median)
print("Median of task completion times = ", task_median)
