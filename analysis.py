#!/usr/bin/python3

import json
import numpy as np
import matplotlib.pyplot as plt
#import seaborn as sns #maybe ?
from statistics import mean, median

master = open("logs/master_log.txt", "r")
config_file = open("config.json", 'r')
config = json.load(config_file)
config_file.close()

worker_ids = []
jobs = {}
tasks = {}
job_ids = []
time_intervals = []
num_tasks = {}
job_start_times = {}
task_start_times = {}
task_end_times = {}
count_m = 0
count_w = 0
scheduling_algo = ["Round Robin", "Random", "Least Loaded"]
sch = 0
for w in config["workers"]:
    worker_ids.append(w["worker_id"])   



for line in master.readlines():
    if count_m == 0:
        if("random_algo" in line):
            sch = 1
        elif("least_loaded_algo" in line):
            sch = 2
        else:
            sch = 0
        count_m+=1
    elif count_m != (3 + len(worker_ids)):
        count_m += 1
    else:  
        l = line.strip().split()
        if len(l) == 2:
            job_id, job_start = l
            job_start = float(job_start)
            job_ids.append(job_id)
            job_start_times[job_id] = job_start
        else:
            t = l[-1]
            time_intervals.append(float(t))
            l.pop(-1)
            for i in l:
                id, n = i.split(":")
                num_tasks[id+"_"+t] = n

for ji in job_ids:
    jobs[ji] = []

for i in worker_ids:
    w = "logs/w" + str(i) + "_log.txt"
    worker_file = open(w,"r")
    task_start_times[i] = []
    task_end_times[i] = []    
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
            task_end_times[i].append(end)

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
plt.xticks(rotation=90)
plt.ylabel('Completion time(s)')
plt.xlabel('JobID_TaskID')
plt.savefig("graphs/task_completion.png", bbox_inches="tight")
plt.close()
'''
#Graph showing all the completion times for all the jobs
job_ids = ["Job "+i for i in job_ids]
part1_jobs_fig = plt.figure()
part1_jobs_ax = part1_jobs_fig.add_axes([0.1, 0.1, 0.85, 0.85])
part1_jobs_ax.bar(job_ids,list(jobs.values()))
part1_jobs_ax.set_ylabel('Completion time(s)')
part1_jobs_ax.set_xlabel('JobID')
part1_jobs_fig.savefig("graphs/job_completion.png")
#plt.close()
'''
#Graph showing number of tasks on each machine against time

s = 'Worker '
for i in task_start_times:
	task = 0
	number_tasks = []
	number_tasks.append(0)
	time = []
	time.append(0)
	j=0
	k=0
	task_start_times[i].sort()
	task_end_times[i].sort()
	#print(task_start_times[i])
	#print(task_end_times[i])
	while (j<len(task_start_times[i]) and k<len(task_end_times[i])):
		if(task_start_times[i][j] < task_end_times[i][k]):
			task+=1
			number_tasks.append(task)
			time.append(task_start_times[i][j]-job_start_times['0'])
			j+=1
		else:
			task-=1
			number_tasks.append(task)
			time.append(task_end_times[i][k]-job_start_times['0'])
			k+=1
	if(j == len(task_start_times[i])):
		while(k<len(task_end_times[i])):
			task-=1
			number_tasks.append(task)
			time.append(task_end_times[i][k]-job_start_times['0'])
			k+=1
	else:
		while(j<len(task_start_times[i])):	
			task+=1
			number_tasks.append(task)
			time.append(task_start_times[i][j]-job_start_times['0'])
			j+=1
	#print(time, number_tasks)		
	plt.plot(time, number_tasks, label=s+str(i))
plt.title(scheduling_algo[sch]+" Scheduling")
plt.legend(loc="upper right")
plt.xlabel("Time (s)")
plt.ylabel("No. of tasks")
plt.savefig("graphs/time_vs_tasks_"+scheduling_algo[sch]+".png", bbox_inches="tight")
plt.close()
#print(number_tasks)
#print(time)
	

#print(num_tasks) # format is key = workerId_time, value = number of tasks at the time
print("Mean of job completion times = ", job_mean)
print("Mean of task completion times = ", task_mean)
print("Median of job completion times = ", job_median)
print("Median of task completion times = ", task_median)
