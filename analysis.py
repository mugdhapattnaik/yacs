#!/usr/bin/python3

import json
#import numpy as np
#import matplotlib.pyplot as plt
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
            print(job_id,job_start)
            job_ids.append(job_id)
            end_times = []
            for i in worker_ids:
                w = "logs/w" + str(i) + "_log.txt"
                worker_file = open(w,"r")
                task_start_times[i] = []
                time_interval = 1
                for line in worker_file.readlines():
                    if count_m != (3 + len(worker_ids)):
                        count_m += 1
                    else: 
                        j, t, st, end = line.strip().split()
                        st = float(st)
                        end = float(end)
                        if j == job_id:
                            end_times.append(end)
                        tasks[j+t] = (end - st)
                        task_start_times[i].append(st)
            jobs[job_id] = round((max(end_times) - job_start), 8)
            

#Not sure where these should be shown
job_mean = mean(jobs[k] for k in jobs)
job_median = median(jobs[k] for k in jobs)
task_mean = mean(tasks[k] for k in tasks)
task_median = median(tasks[k] for k in tasks)

'''
#Graph showing all the completion times for all the tasks
part1_tasks_fig = plt.figure()
part1_tasks_ax = part1_tasks_fig.add_axes([0,0,1,1])
#to be completed 

#Graph showing all the completion times for all the jobs
job_ids = ["Job "+i for i in job_ids]
part1_jobs_fig = plt.figure()
part1_jobs_ax = part1_jobs_fig.add_axes([0,0,1,1])
part1_jobs_ax.bar(job_ids,list(jobs.values()))
part1_jobs_fig.savefig("logs/job_completion.png")


#Graph showing number of tasks on each machine against time
part2_fig = plt.figure()
part2_ax = part2_fig.add_axes([0,0,1,1])
'''

#print(task_start_times)  
#print(job_mean)
#print(task_mean)
#print(job_median)
#print(task_median)