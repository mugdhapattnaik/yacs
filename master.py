#!/usr/bin/python3

import json
import socket
import time
import sys
import random
import threading
import datetime

from queue import Queue

lock = threading.Lock()

#logging in master
class masterLogger():
	#master log file name
	fname = 'logs/master.log'
	
	def __init__(self):
		self.logmsg = ''
		try:
			os.makedirs("logs")
		except:
			pass
		
	#master's header log
	def initLog(self, sch_alg, wid, wrk):
		#time and chosen scheduler algorithm
		self.logmsg = '$$$ \tdate: ' + str(datetime.datetime.now()) + f" {sch_alg.__name__} $$$\n\n"
		
		#worker information
		for ele in wid:
			self.logmsg += 'Worker_id: ' + str(ele) + '\tTotal Slots: ' + str(wrk[ele].total_slots) + '\tPort number: ' + str(wrk[ele].port) + '\n'
		self.logmsg+='\n'
		
		with open(masterLogger.fname, 'w') as f:	#write log into file
			f.write(self.logmsg)
		self.logmsg = ''	#reset log string

	def logtime(self, job_id):
		#logs current time
		self.logmsg = str(job_id) + " " + str(time.time()) + "\n"

		with open(masterLogger.fname, 'a') as f:	#write log into file
			f.write(self.logmsg)
		self.logmsg = ''	#reset log string
		
	def prLog(self, worker_id, workers, now):
		#for each worker, print worker id and number of occupied slots
		for i in range(len(worker_id)):
			#initial condition			
			idx = worker_id[i]
			worker = workers[idx]
			self.logmsg += str(worker.id) + ":" + str(worker.active_slots)
			self.logmsg += ' '
		self.logmsg+= f'\t\t{now}\n'	#append time to log string
		
		with open(masterLogger.fname, 'a') as f:	#write log into file
			f.write(self.logmsg)
		self.logmsg = ''	#reset log string


class Master:

	class Worker:
		
		#initializes worker object from config file entry
		def __init__(self, config):
			self.id = config["worker_id"]
			self.total_slots = int(config["slots"])
			self.active_slots = 0
			self.port = config["port"]
		
		#returns True if worker has available slots
		#requires lock to be acquired
		def available(self):
			return self.active_slots < self.total_slots
		
	class Job:
		#NOTE: Each job may have multiple map tasks and multiple reduce tasks	

		class Task:

			#initializes each task (map or reduce) with task information
			def __init__(self, task_id, job_id, duration, task_type):
				self.id = task_id
				self.job_id = job_id
				self.duration = duration
				self.type = task_type
		
		#initializes Job and its associated tasks from json request
		def __init__(self, master, request):
			self.id = request["job_id"]

			#NOTE: different map task and reduce task queues
			self.map_tasks = Queue()	#queue of incomplete map tasks
			self.reduce_tasks = Queue()	#queue of incomplete reduce tasks

			#number of map tasks tracked to ensure
			#all map tasks are completed before reduce tasks
			self.num_map_tasks = len(request["map_tasks"])
			
			for t in request["map_tasks"]:
				task = self.Task(t["task_id"], self.id, t["duration"], "map")	# create map task object 
				self.map_tasks.put(task)	# map task is enqueued as an incomplete map_task
				master.tasks[task.id] = task	# add task to dictionary of all tasks

			for t in request["reduce_tasks"]:
				task = self.Task(t["task_id"], self.id, t["duration"], "reduce")	# create reduce task object
				self.reduce_tasks.put(task)	# reduce task is enqueued as an incomplete reduce_task
				master.tasks[task.id] = task	# add task to dictionary of all tasks

	def __init__(self, config, sch_algo='RR'):
		
		self.worker_ids = []	#list of worker ids
		self.workers = {} #dict of worker objects, referenced by worker_id
		self.jobs = {} #dict of job objects, referenced by job_id
		self.tasks = {} #dict of tasks referenced by task_id
		self.independent_tasks_q = Queue() #list of tasks not having any dependencies
		
		for worker_config in config["workers"]:
			self.worker_ids.append(worker_config["worker_id"])
			
			# create worker object using config, then populate dict
			self.workers[worker_config["worker_id"]] = self.Worker(worker_config)
		
		#When no entries are present in worker config file, quits with error
		if(not self.worker_ids):
			print("ERROR: No workers to schedule tasks to")
			exit(1)
			
		#sorted for round_robin_algo
		self.worker_ids.sort()
		
		#sch_algo holds 
		if sch_algo == 'RR':
			self.rr_worker_id_index = 0	# to keep track of next worker to be alloted
			self.sch_algo = self.round_robin_algo
		elif sch_algo == 'RANDOM':
			self.sch_algo = self.random_algo
		elif sch_algo == 'LL':
			self.sch_algo = self.least_loaded_algo
		
		#initializing logger
		self.ml = masterLogger()	
		self.ml.initLog(self.sch_algo, self.worker_ids, self.workers)
	
	#prints each workers total slot capacity and number of occupied slots
	def pr_workers(self):
		for i in self.worker_ids:
			worker = self.workers[i]
			print("Worker ", worker.id, ": ", worker.total_slots, worker.active_slots)

	#thread which listens for job requests
	def listen_requests(self):

		#TCP/IP socket
		requests_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		#Setting address to be reusable to bypass timeout  
		requests_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

		#listen on localhost and port 5000
		requests_port = 5000
		requests_socket.bind(('localhost', requests_port))	
		requests_socket.listen() #listen to incoming requests
		
		while True:
			print("Listening for job requests...")

			#accept client request and data being sent 
			req_conn, addr = requests_socket.accept()	
			r = req_conn.recv(2048).decode()

			#extract data from json request
			request = json.loads(r)
			
			print("Received request of job ID : ", request["job_id"])
			
			#create a Job object
			job = self.Job(self, request)
		
			#index an entry in dictionary with job_id
			self.jobs[request["job_id"]] = job

			#Log the time when master receives the request
			self.ml.logtime(job.id)
			
			#queue all map tasks as independent tasks
			#map tasks do not have any dependencies
			while not job.map_tasks.empty():
				map_task = job.map_tasks.get()
				self.independent_tasks_q.put(map_task)

			#close socket connection
			req_conn.close()
	
	#thread which listens for updates from the worker threads
	def listen_updates(self):

		#TCP/IP socket
		worker_updates_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		#Setting address to be reusable to bypass timeout 
		worker_updates_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

		#listen on localhost and port 5001
		worker_updates_port = 5001
		worker_updates_socket.bind(('localhost', worker_updates_port))
		worker_updates_socket.listen() #backlog default, reasonable number of connections
		
		while True:
			#accept worker's connection request and update message
			conn, addr = worker_updates_socket.accept()
			m = conn.recv(2048).decode()
			
			#extract data from update
			message = json.loads(m)
			print("Received update from worker")
			
			worker_id = message["worker_id"]
			task_id = message["task_id"]

			worker = self.workers[worker_id]
			task = self.tasks[task_id]
			job = self.jobs[task.job_id]
			
			#print log information
			print("========== WORKER", worker.id, "COMPLETED TASK", task.id, "==========")
			print("Updating task dependencies")	
			
			lock.acquire()
			worker.active_slots -= 1
			self.pr_workers()
			lock.release()
			
			#if its a map task
			if(task.type == "map"):
				job.num_map_tasks -= 1
				
				# if all the map tasks of the job are completed
				if(job.num_map_tasks == 0):
					# then add all the reduce tasks to independent tasks queue
					while not job.reduce_tasks.empty():
						reduce_task = job.reduce_tasks.get()
						self.independent_tasks_q.put(reduce_task)
			
			conn.close()

	#Scheduler thread which schedules tasks among workers depending on
	#scheduling algorithm
	def schedule(self):
		
		while True:
			# wait for independent tasks queue to have a task for scheduling
			if self.independent_tasks_q.empty():
				continue
			
			#fetch task to be scheduled
			task = self.independent_tasks_q.get()
			
			#fetch worker to be assigned new task
			worker = self.sch_algo()
			
			#print log information
			if(task.type == "map"):
				print("========== SENT MAP TASK", task.id, "TO WORKER", worker.id, "==========")
			elif(task.type == "reduce"):
				print("========== SENT REDUCE TASK", task.id, "TO WORKER", worker.id, "==========")
			
			#log information
			self.pr_workers()
			self.ml.prLog(self.worker_ids, self.workers, time.time())
			
			#releases lock acquired in the scheduling algorithm function
			lock.release()
			
			#send task information to worker
			self.send_task(task, worker)
				
	def send_task(self, task, worker):
		port = int(worker.port)

		#TCP/IP socket
		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as c:
			#connect to worker socket
			c.connect(('localhost', port))

			#encode task information as json object
			message = {"task_id": task.id, "job_id": task.job_id, "duration": task.duration}
			message = json.dumps(message).encode()

			c.send(message)	#send message

	def random_algo(self):
		print("Scheduling task using random_algo")

		while True:
			#chooses a random worker
			worker_id = random.choice(self.worker_ids)
			worker = self.workers[worker_id]
			
			#acquire lock for shared variable processing
			lock.acquire()
			if(worker.available()):
				worker.active_slots += 1 #increments number of slots occupied with tasks
				return worker

			#release lock
			lock.release()

	def round_robin_algo(self):
		print("Scheduling task using round_robin_algo")
	
		while True:
			#get worker id of worker to send task to
			#extract worker object using worker id
			rr_worker = self.workers[self.worker_ids[self.rr_worker_id_index]]

			#update rr_worker_id_index to point to next worker  
			self.rr_worker_id_index = (self.rr_worker_id_index + 1) % len(self.worker_ids)
			
			#acquire lock for shared variable processing
			lock.acquire()
			if(rr_worker.available()):
				rr_worker.active_slots += 1 #increments number of slots occupied with tasks
				return rr_worker
			
			#release lock
			lock.release()

	def least_loaded_algo(self):
		print("Scheduling task using least_loaded_algo")

		while True:
			#initialize variable with first worker in list
			least_loaded = self.workers[self.worker_ids[0]]
			
			#acquire lock for shared variable processing
			lock.acquire()

			#initalize max_slots with available slots of first worker
			max_slots = least_loaded.total_slots - least_loaded.active_slots

			#iterate through the other workers' status
			for worker_id in self.worker_ids[1::]:
				worker = self.workers[worker_id]
				curr_slots = worker.total_slots - worker.active_slots

				#if any worker has greater number of available slots
				if(curr_slots > max_slots):
					least_loaded = worker
					max_slots = curr_slots	#update least loaded worker
			
			worker = least_loaded
			if(worker.available()):
				worker.active_slots += 1 #increments number of slots occupied with tasks
				return worker

			#release lock							
			lock.release()


if __name__ == '__main__':	 

	config_file = open(sys.argv[1], 'r')	#config file name
	config = json.load(config_file)			#load config file
	scheduling_algo = str(sys.argv[2])		#chosen scheduling algorithm
	
	master = Master(config, scheduling_algo)	#create master object from config

	#create three different threads	
	listen_requests_thread = threading.Thread(target = master.listen_requests)
	listen_updates_thread = threading.Thread(target = master.listen_updates)
	schedule_thread = threading.Thread(target = master.schedule)
	
	#start all 3 threads
	listen_requests_thread.start()
	listen_updates_thread.start()
	schedule_thread.start()
	
	#wait for all 3 of them to terminate
	listen_requests_thread.join()
	listen_updates_thread.join()
	schedule_thread.join()
