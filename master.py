#!/usr/bin/python3

import json
import socket
import time
import sys
import random
import threading

from queue import Queue
from logger import masterLogger

lock = threading.Lock()

class Master:

	class Worker:
		
		def __init__(self, config):
			self.id = config["worker_id"]
			self.total_slots = int(config["slots"])
			self.active_slots = 0
			self.port = config["port"]
		
		def available(self):
			return self.active_slots < self.total_slots
		
	class Job:
		
		class Task:
			
			def __init__(self, task_id, job_id, duration, task_type):
				self.id = task_id
				self.job_id = job_id
				self.duration = duration
				self.type = task_type
		
		def __init__(self, master, request):
			self.id = request["job_id"]
			self.map_tasks = Queue()
			self.reduce_tasks = Queue()
			self.num_map_tasks = len(request["map_tasks"])
			
			for t in request["map_tasks"]:
				task = self.Task(t["task_id"], self.id, t["duration"], "map")
				self.map_tasks.put(task)
				master.tasks[task.id] = task

			for t in request["reduce_tasks"]:
				task = self.Task(t["task_id"], self.id, t["duration"], "reduce")
				self.reduce_tasks.put(task)
				master.tasks[task.id] = task

	def __init__(self, config, sch_algo='RR'):
		
		self.worker_ids = []
		self.workers = {}
		self.jobs = {}
		self.tasks = {}
		self.independent_tasks_q = Queue()
		
		for worker_config in config["workers"]:
			self.worker_ids.append(worker_config["worker_id"])
			self.workers[worker_config["worker_id"]] = self.Worker(worker_config)
		
		if(not self.worker_ids):
			print("No workers to schedule tasks to")
			exit(1)
			
		self.worker_ids.sort()
		
		if sch_algo == 'RR':
			self.rr_worker_id_index = 0
			self.sch_algo = self.round_robin_algo
		elif sch_algo == 'RANDOM':
			self.sch_algo = self.random_algo
		elif sch_algo == 'LL':
			self.sch_algo = self.least_loaded_algo
		
		#initializing loggers
		self.ml = masterLogger()	
		self.ml.initLog(self.sch_algo, self.worker_ids, self.workers)
	
	def pr_workers(self):

		for i in self.worker_ids:
			worker = self.workers[i]
			print("Worker ", worker.id, ": ", worker.total_slots, worker.active_slots)

	def pr_jobs(self):
		print("Jobs:")
		for k, v in self.jobs.items():
			print(k, ": ")
			print("map_tasks:")
			while(not v.map_tasks.empty()):
				t = v.map_tasks.get()
				print(t.id, t.duration, t.type)

			print("reduce_tasks:")
			while(not v.reduce_tasks.empty()):
				t = v.reduce_tasks.get()
				print(t.id, t.duration, t.type)				
				
	def listen_requests(self):
		requests_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		requests_port = 5000
		requests_socket.bind(('localhost', requests_port))
		requests_socket.listen()
		
		while True:
			print("Listening for job requests...")
			req_conn, addr = requests_socket.accept()	
			r = req_conn.recv(2048).decode()
			request = json.loads(r)
			
			print("\nReceived request with job ID : ", request["job_id"])
			
			job = self.Job(self, request)
			self.jobs[request["job_id"]] = job

			self.ml.logtime(job.id)
			
			while not job.map_tasks.empty():
				map_task = job.map_tasks.get()
				self.independent_tasks_q.put(map_task)

			req_conn.close()
	
	def listen_updates(self):
		worker_updates_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		worker_updates_port = 5001
		
		worker_updates_socket.bind(('localhost', worker_updates_port))
		worker_updates_socket.listen(10)
		
		while True:
			conn, addr = worker_updates_socket.accept()
			m = conn.recv(2048).decode()
			
			message = json.loads(m)
			print("Received update from worker")
			
			worker_id = message["worker_id"]
			task_id = message["task_id"]

			worker = self.workers[worker_id]
			task = self.tasks[task_id]
			job = self.jobs[task.job_id]
			
			print("========== WORKER", worker.id, "COMPLETED TASK", task.id, "==========")
			print("Updating task dependencies")	
			
			lock.acquire()
			worker.active_slots -= 1
			self.pr_workers()
			lock.release()
			
			if(task.type == "map"):
				job.num_map_tasks -= 1
				
				if(job.num_map_tasks == 0):
					while not job.reduce_tasks.empty():
						reduce_task = job.reduce_tasks.get()
						self.independent_tasks_q.put(reduce_task)
			
			conn.close()

	def schedule(self):
		
		while True:
			if self.independent_tasks_q.empty():
				continue
			
			task = self.independent_tasks_q.get()
			
			worker = self.sch_algo()
			
			if(task.type == "map"):
				print("========== SENT MAP TASK", task.id, "TO WORKER", worker.id, "==========")
			elif(task.type == "reduce"):
				print("========== SENT REDUCE TASK", task.id, "TO WORKER", worker.id, "==========")
			self.pr_workers()
			self.ml.prLog(self.worker_ids, self.workers, time.time())
			lock.release()
			
			self.send_task(task, worker)
				
	def send_task(self, task, worker):
		port = int(worker.port)

		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as c:
			c.connect(('localhost', port))
			message = {"task_id": task.id, "job_id": task.job_id, "duration": task.duration}
			message = json.dumps(message).encode()
			c.send(message)

	def random_algo(self):
		print("Task scheduled using random_algo")

		while True:
			worker_id = random.choice(self.worker_ids)
			worker = self.workers[worker_id]
			
			lock.acquire()
			if(worker.available()):
				worker.active_slots += 1
				return worker
			lock.release()

	def round_robin_algo(self):
		print("Task scheduled using round_robin_algo")
	
		while True:
			rr_worker = self.workers[self.worker_ids[self.rr_worker_id_index]]
			self.rr_worker_id_index = (self.rr_worker_id_index + 1) % len(self.worker_ids)
			
			lock.acquire()
			if(rr_worker.available()):
				rr_worker.active_slots += 1
				return rr_worker
			lock.release()

	def least_loaded_algo(self):
		print("Task scheduled using least_loaded_algo")

		while True:
			least_loaded = self.workers[self.worker_ids[0]]
			
			lock.acquire()
			max_slots = least_loaded.total_slots - least_loaded.active_slots
			for worker_id in self.worker_ids[1::]:
				worker = self.workers[worker_id]
				curr_slots = worker.total_slots - worker.active_slots
				if(curr_slots > max_slots):
					least_loaded = worker
					max_slots = curr_slots
			
			worker = least_loaded
			if(worker.available()):
				worker.active_slots += 1
				return worker							
			lock.release()


if __name__ == '__main__':	 

	config_file = open(sys.argv[1], 'r')
	config = json.load(config_file)
	scheduling_algo = str(sys.argv[2])
	
	master = Master(config, scheduling_algo)
	
	listen_requests_thread = threading.Thread(target = master.listen_requests)
	listen_updates_thread = threading.Thread(target = master.listen_updates)
	schedule_thread = threading.Thread(target = master.schedule)
	
	listen_requests_thread.start()
	listen_updates_thread.start()
	schedule_thread.start()
	
	listen_requests_thread.join()
	listen_updates_thread.join()
	schedule_thread.join()
