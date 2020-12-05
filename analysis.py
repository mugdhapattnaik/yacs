#!/usr/bin/python3

import json
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mp
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
job_start_times = {}    #dict of job start times referenced using job_id
task_start_times = {}   #dict of all task start times referenced using worker_id
task_end_times = {}     #dict of task end times referenced using worker_id
count_m = 0             #tracks number of master log lines read (to discard irrelevant data) 
count_w = 0             #tracks number of worker log lines read (to discard irrelevant data)
scheduling_algo = ["Round Robin", "Random", "Least Loaded"]
sch = 0
for w in config["workers"]:
    worker_ids.append(w["worker_id"])   


#read master logs
for line in master.readlines():
    #read first line and extract scheduling algorithm information
    if count_m == 0:
        if("random_algo" in line):
            sch = 1
        elif("least_loaded_algo" in line):
            sch = 2
        else:
            sch = 0
        count_m+=1
    
    #discard worker information lines - not relevant to analysis
    elif count_m != (3 + len(worker_ids)):
        count_m += 1
    
    #read all lines that specify jobs starting times (epoch time)
    else:  
        l = line.strip().split()
        if len(l) == 2:
            job_id, job_start = l
            job_start = float(job_start)

            #check to ensure redundant information is not added
            if job_id not in job_ids:
                job_ids.append(job_id)
            job_start_times[job_id] = job_start #append job data


for ji in job_ids:
    jobs[ji] = []

#for each worker
for i in worker_ids:
    #read log file
    w = "logs/w" + str(i) + "_log.txt"
    worker_file = open(w,"r")

    task_start_times[i] = []    #dict of task start times referenced by worker_id
    task_end_times[i] = []      #dict of task start times referenced by worker_id
    count_w = 0                 #tracks number of log lines
    
    for line in worker_file.readlines():
        #first 3 lines are meta data and not relevant to analysis
        if count_w != 3:
            count_w += 1
        else: 
            j, t, st, end, id = line.strip().split() 
            st = float(st)      #start time of task
            end = float(end)    #end time of task
            jobs[j].append(end) #append end time to respective job (indexed using job id)
            tasks[j+"_"+t] = (end - st) #task completion time
            task_start_times[i].append(st)  #record task start time
            task_end_times[i].append(end)   #record task end time

#for each job, calculate the time required to complete last reduce task
#starting from the reception of request in master 
for j in jobs:
    jobs[j] = round((max(jobs[j]) - job_start_times[j]), 8)

#calculate mean and median statistics
job_mean = mean(jobs[k] for k in jobs)
job_median = median(jobs[k] for k in jobs)
task_mean = mean(tasks[k] for k in tasks)
task_median = median(tasks[k] for k in tasks)

#labels for the mean and median lines
mean_patch = mp.Patch(color='crimson', label="Mean")
median_patch = mp.Patch(color='midnightblue', label="Median")

#Graph showing all the completion times for all the tasks
plt.bar(list(tasks.keys()),list(tasks.values()), color ='thistle')
plt.xticks(fontsize=6.5)
plt.xticks(rotation=90)
plt.ylabel('Completion time(s)')
plt.xlabel('JobID_TaskID')
plt.axhline(task_mean, color ='crimson', linestyle = "--")
plt.axhline(task_median, color ='midnightblue', linestyle = "--")
plt.legend(handles = [mean_patch,median_patch])
plt.savefig("graphs/task_completion_"+scheduling_algo[sch]+".png", bbox_inches="tight")
plt.close()

#Graph showing all the completion times for all the jobs
job_ids = ["Job "+i for i in job_ids]
part1_jobs_fig = plt.figure()
part1_jobs_ax = part1_jobs_fig.add_axes([0.1, 0.1, 0.85, 0.85])
part1_jobs_ax.bar(job_ids,list(jobs.values()), color ='thistle')
part1_jobs_ax.tick_params(axis = 'x',labelrotation=45)
part1_jobs_ax.set_ylabel('Completion time(s)')
part1_jobs_ax.set_xlabel('JobID')
plt.axhline(job_mean, color ='crimson', linestyle = "--")
plt.axhline(job_median, color ='midnightblue', linestyle = "--")
plt.legend(handles = [mean_patch,median_patch])
part1_jobs_fig.savefig("graphs/job_completion_"+scheduling_algo[sch]+".png", bbox_inches="tight")
plt.close()

#Graph showing number of tasks on each machine against time
s = 'Worker '
shift = 0
for i in task_start_times:      #for each worker
	task = 0
    #initialize number of active tasks
	number_tasks = []
	number_tasks.append(0)
    #initialize time of observation
	time = []
	time.append(0)  
                
	j=0
	k=0
	task_start_times[i].sort()
	task_end_times[i].sort()
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
#	plt.plot(time, number_tasks, label=s+str(i))
	number_tasks = [i-shift for i in number_tasks]
	time = [i+shift for i in time]
	plt.step(time, number_tasks, label=s+str(i), where = "pre", alpha = 0.9)
	shift += 0.02
plt.title(scheduling_algo[sch]+" Scheduling")
plt.legend(loc="upper right")
plt.xlabel("Time (s)")
plt.ylabel("No. of tasks")
plt.savefig("graphs/time_vs_tasks_"+scheduling_algo[sch]+".png", bbox_inches="tight")
plt.close()

#print job statistics
print("Mean of job completion times = ", job_mean)
print("Mean of task completion times = ", task_mean)
print("Median of job completion times = ", job_median)
print("Median of task completion times = ", task_median)

#L52
